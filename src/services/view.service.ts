import { ADDRESS } from "@/config";
import ioClient from "@/io/client/index.client";
import { broadcast } from "@/io/index.io";
import ioServer from "@/io/server/index.server";
import viewModel from "@/models/view.model";
import { logger } from "@/utils/logger";
import { Mutex } from "async-mutex";
import axios from "axios";
import kvsService from "@/services/kvs.service";
import { KeyDistribution, Shard, ShardIdKeyPairing } from "@/interfaces/shard.interface";
import { makeEventuallyConsistent } from "./replication.service";
import { KvStore } from "@/interfaces/kv.interface";

class ViewService {
  public viewObject = viewModel;
  public mutex = new Mutex();
  public num_shards = 1;
  public oldShardKeysRemoved: boolean;

  constructor() {
    this.viewObject.view = [];
    this.viewObject.shard_index = 0;
    this.oldShardKeysRemoved = true;
  }

  public async getView(): Promise<{ view: Shard[] }> {
    let ret: { view: Shard[] };
    await this.mutex.runExclusive(async () => {
      ret = this.viewObject;
    });
    return ret;
  }

  public async setView(incomingBody: { num_shards: number; nodes: string[] }, sender = "client"): Promise<void> {
    let oldList = [];
    let oldView: Shard[] = [];
    const prev_num_shards = this.num_shards;
    this.num_shards = incomingBody.num_shards;
    await this.mutex.runExclusive(async () => {
      oldList = []; // Fetch entire list of old addresses

      this.viewObject.view.forEach(shard => {
        shard.nodes.forEach(addr => {
          oldList.push(addr);
        });
      });
      oldView = this.viewObject.view;
    });

    // resharding - get all keys from all shards
    let allKeys:ShardIdKeyPairing = {};
    if (this.num_shards !== prev_num_shards) {
      allKeys = await this.getAllKeysFromShards(oldView);
      this.oldShardKeysRemoved = false;
    }

    const incoming = incomingBody.nodes;
    await this.updateView(incoming); // Update View

    const view = (await this.getView()).view;

    if (oldList.length === 0) {
      // Uninitialized
      // was the view change sent by another replica?
      if (sender === "client") {
        // forward view change to all other replicas via HTTP
        await this.sendViewChange(
          incoming.filter(replica => replica !== ADDRESS),
          view,
          {}
        );
        // get IO Server to start listening for connections
        ioServer.listen();
        makeEventuallyConsistent();
      }
    } else {
      // Already Initialized
      if (sender === "client") {
        // check for shard changes too
        const containsAll = (arr1, arr2) => arr2.every(arr2Item => arr1.includes(arr2Item));
        const sameMembers = (arr1, arr2) => containsAll(arr1, arr2) && containsAll(arr2, arr1);
        const missing = oldList.filter(n => !incoming.includes(n));
        const extra = incoming.filter(n => !oldList.includes(n));

        // view has changed
        if (!sameMembers(oldList, incoming) || prev_num_shards !== this.num_shards) {
          // resharding - update shard KVS with new keys
          const distribution = this.getKeyDistribution(allKeys);
          logger.info(`key distribution = ${JSON.stringify(distribution)}`);
          await this.updateShardKvs(distribution, oldView);

          let ioServerKilled = false;
          const ioServerIP = ioClient.getIP();
          // figure out if replica running IO server has been killed or not
          if (!ioServer.isListening() && ioClient.isConnected()) {
            // this replica is not running IO Server
            if (missing.includes(ioServerIP)) {
              ioServerKilled = true;
            }
          }
          if (missing.length > 0) {
            // some replicas been removed from view
            broadcast("viewchange:kill", missing);
          }
          if (ioServerKilled) {
            // IO Server has been killed
            // 1. disconnect from current IO Server
            if (ioClient.isConnected()) {
              ioClient.disconnect();
            }
            // 2. this replica becomes IO server
            ioServer.listen();
            // 3. forward view change to all other replicas via HTTP
            await this.sendViewChange(
              incoming.filter(replica => replica !== ADDRESS),
              view,
              distribution,
            );
          } else {
            if (extra.length > 0) {
              // new replicas added to view
              await this.sendViewChange(extra, view, distribution, ioServerIP);
            }
            // update existing replica views
            broadcast("viewchange:update", { sender: ADDRESS, view: Array.from(view), keyDistribution: distribution });
          }
        }
      }
    }
  }

  public async updateView(incoming: string[]): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.viewObject.view = [];
      for (let i = 0; i < this.num_shards; i++) {
        this.viewObject.view.push({ shard_id: i, nodes: [] });
      }

      for (let i = 0; i < incoming.length; i++) {
        const k = i % this.num_shards;
        if (incoming[i] === ADDRESS) {
          this.viewObject.shard_index = k;
        }
        this.viewObject.view[k].nodes.push(incoming[i]);
      }
    });
  }

  public async replaceView(view: Shard[], sender: string, distribution: KeyDistribution): Promise<void> {
    logger.info(`replaceView: ${JSON.stringify(Array.from(view))}`);
    const oldView = (await this.getView()).view;
    this.num_shards = view.length;
    await this.mutex.runExclusive(async () => {
      this.viewObject.view = view;
      for (let i = 0; i < view.length; i++) {
        if (view[i].nodes.includes(ADDRESS)) {
          this.viewObject.shard_index = i;
          break;
        }
      }
    });

    // resharding
    if (Object.keys(distribution).length > 0) {
      this.oldShardKeysRemoved = false;
      await this.updateShardKvs(distribution, oldView);
    }

    if (sender !== "broadcast") {
      // If replica was hosting IO Server in prev view, kill it
      if (ioServer.isListening()) {
        ioServer.shutdown();
      }
      // Disconnect from previous IO Server
      if (ioClient.isConnected()) {
        ioClient.disconnect();
      }
      setTimeout(() => {
        // Connect to sender's IO Server
        ioClient.connect(`http://${sender}`);
        makeEventuallyConsistent();
      }, 500);
    }
  }

  public async deleteView(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      await kvsService.clearKvs();
      this.viewObject.view = [];
      this.viewObject.shard_index = 0;
    });
    if (ioServer.isListening()) {
      ioServer.shutdown();
    } else if (ioClient.isConnected()) {
      ioClient.disconnect();
    }
  }

  public async sendViewChange(replicas: string[], view: Shard[], keyDistribution: KeyDistribution, sender = ADDRESS): Promise<void> {
    const addresses = replicas.map(replicaAddress => `http://${replicaAddress}/kvs/admin/view/shards`);
    try {
      const reqBody = {
        view,
        keyDistribution,
        sender,
      };
      const reqHeaders = { headers: { "Content-Type": "application/json" } };
      const reqPromises = addresses.map(address => axios.put(address, reqBody, { ...reqHeaders, timeout: 10000 }));

      await Promise.all(reqPromises);
    } catch (error) {
      logger.error("viewService:sendViewChange - " + error);
    }
  }

  public async getAllKeysFromShards(view: Shard[]): Promise<ShardIdKeyPairing> {
    const shardIndex = this.viewObject.shard_index;
    const keys:ShardIdKeyPairing = {
      [shardIndex]: (await kvsService.getAllKeys()).keys,
    };
    for (const shard of view) {
      if (shard.shard_id !== shardIndex) {
        for (const node of shard.nodes) {
          const address = `http://${node}/kvs/data/`;
          try {
            const response = await axios.get(address, { timeout: 2000 });
            if (response.status === 200) {
              keys[shard.shard_id] = response.data.keys;
              break;
            }
          } catch (error) {
            logger.error("viewService:getAllKeysFromShards - " + error);
          }
        }
      }
    }
    return keys;
  }

  public getKeyDistribution(allKeys:ShardIdKeyPairing):KeyDistribution {
    const distribution:KeyDistribution = {};
    for (const shardId in allKeys) {
      const oldShardID = parseInt(shardId, 10);
      for (const key of allKeys[shardId]) {
        const newShardID = kvsService.lookUp(this.num_shards, key);
        if (distribution[newShardID] === undefined) {
          distribution[newShardID] = {};
        }
        if (distribution[newShardID][oldShardID] === undefined) {
          distribution[newShardID][oldShardID] = [key];
        } else {
          distribution[newShardID][oldShardID].push(key);
        }
      }
    }
    return distribution;
  }

  public async updateShardKvs(distribution:KeyDistribution, oldView: Shard[]):Promise<void> {
    const newShardIndex = await this.getShardIndex();
    const oldShardIndex = oldView.findIndex(shard => shard.nodes.includes(ADDRESS));
    const keysInShard = distribution[newShardIndex];

    for (const oldShardID in keysInShard) {
      if (parseInt(oldShardID, 10) !== oldShardIndex) {
        const keys = keysInShard[oldShardID];
        const oldShard = oldView[oldShardID];
        for (const node of oldShard.nodes) {
          const address = `http://${node}/kvs/resharding/data/`;
          try {
            const reqBody = {
              keys
            };
            const reqHeaders = { headers: { "Content-Type": "application/json" } };
            const response = await axios.put(address, reqBody, { ...reqHeaders, timeout: 2000 });
            if (response.status === 200) {
              const { kvs }:{ kvs: KvStore} = response.data;
              for (const key in kvs) {
                const v = kvs[key];
                await kvsService.createOrUpdateKv({ key, val: v.val }, v.causalContext.causalMetadata, v);
              }
              break;
            }
          } catch (error) {
            logger.error("viewService:getUpdateShardKvs - " + error);
          }
        }
      }
    }
  }

  public async checkReplicas(err, responses) {
    logger.info(`checkReplicas - ${JSON.stringify(responses)}`);
    if (err) {
      logger.info("viewService:checkReplicas - " + err);
      const view = await this.getView();
      const viewReplicas = view.view;
      const missingReplicas = viewReplicas.filter(replica => !responses.includes(replica));
      if (missingReplicas.length > 0) {
        logger.error("viewService:checkReplicas - " + missingReplicas + " missing");
      } else {
        logger.info("viewService:checkReplicas - all replicas are acked write");
      }
    }
  }

  public async changeIOServer(): Promise<void> {
    const view = (await this.getView()).view;
    const viewReplicas = view.map(shard => shard.nodes).flat();
    // IO Server is down but still in view, so connect to this replica's IO server
    // 1. disconnect from current IO Server
    if (ioClient.isConnected()) {
      ioClient.disconnect();
    }
    // 2. this replica becomes IO server
    ioServer.listen();
    // 3. forward view change to all other replicas via HTTP
    await this.sendViewChange(
      viewReplicas.filter(replica => replica !== ADDRESS),
      view,
      {}
    );
  }

  public async getShardReplicas(shard_id: number): Promise<string[]> {
    let ret = [];
    await this.mutex.runExclusive(async () => {
      ret = this.viewObject.view[shard_id].nodes;
    });
    return ret;
  }

  public async getShardIndex(): Promise<number> {
    let ret = 0;
    await this.mutex.runExclusive(async () => {
      ret = this.viewObject.shard_index;
    });
    return ret;
  }
}

const myService = new ViewService();
export default myService;
