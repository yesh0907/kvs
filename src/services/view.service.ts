import { ADDRESS } from "@/config";
import ioClient from "@/io/client/index.client";
import { broadcast } from "@/io/index.io";
import ioServer from "@/io/server/index.server";
import viewModel from "@/models/view.model";
import { logger } from "@/utils/logger";
import { Mutex } from "async-mutex";
import axios from "axios";
import clockService from "@/services/clock.service";
import kvsService from "@/services/kvs.service";
// FIX LATER
// import ReplicationService from "@/services/replication.service";
import { Shard } from "@/interfaces/shard.interface";

class ViewService {
  public viewObject = viewModel;
  public mutex = new Mutex();
  public num_shards = 1;

  constructor() {
    this.viewObject.view = [];
    this.viewObject.shard_index = -1;
  }

  public async getView(): Promise<{ view: Shard[] }> {
    let ret: { view: Shard[] };
    await this.mutex.runExclusive(async () => {
      ret = this.viewObject;
    });
    return ret;
  }

  public kvsService = kvsService;

  public async setView(incomingBody: { num_shards: number; nodes: string[] }, sender = "client"): Promise<void> {
    let oldList = [];
    const prev_num_shards = this.num_shards;
    this.num_shards = incomingBody.num_shards;
    await this.mutex.runExclusive(async () => {
      oldList = []; // Fetch entire list of old addresses

      this.viewObject.view.forEach(shard => {
        shard.nodes.forEach(addr => {
          oldList.push(addr);
        });
      });
    });

    const incoming = incomingBody.nodes;
    await this.updateView(incoming); // Update View

    const view = (await this.getView()).view;
    const vc = clockService.getVectorClock();
    view[this.viewObject.shard_index].nodes.forEach(replica => vc.addClock(replica));

    if (oldList.length === 0) {
      // Uninitialized
      // was the view change sent by another replica?
      if (sender === "client") {
        // forward view change to all other replicas via HTTP
        await this.sendViewChange(
          incoming.filter(replica => replica !== ADDRESS),
          view,
        );
        // get IO Server to start listening for connections
        ioServer.listen();
      }
      // FIX LATER
      // const replication = new ReplicationService();
      // replication.begin();
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
            );
          } else {
            if (extra.length > 0) {
              // new replicas added to view
              await this.sendViewChange(extra, view, ioServerIP);
            }

            // rehashing
            const { count, keys } = await kvsService.getAllKeys();
            for (const key of keys) {
              let shard_id = this.kvsService.lookUp(this.num_shards, key);
              let replicas: Promise<string[]> = this.getShardReplicas(shard_id);
              let val = await this.kvsService.getKv(key);
              // Broadcast createUpdateKv with key, val to all replicas
              const broadcastData = { key: key, val: val, sender: ADDRESS, view: replicas };
              broadcast("kvs:write", broadcastData);
              if(val !== undefined) {
                await this.kvsService.deleteKv(key);
              }
            }
            // update existing replica views
            broadcast("viewchange:update", { sender: ADDRESS, view: Array.from(view) });
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
        this.viewObject.shard_index = k;
        this.viewObject.view[k].nodes.push(incoming[i]);
      }
    });
  }

  public async replaceView(view: Shard[], sender: string): Promise<void> {
    logger.info(`replaceView: ${JSON.stringify(Array.from(view))}`);
    const prev = (await this.getView()).view;
    await this.mutex.runExclusive(async () => {
      this.viewObject.view = view;
      for (let i = 0; i < view.length; i++) {
        if (view[i].nodes.includes(ADDRESS)) {
          this.viewObject.shard_index = i;
          break;
        }
      }
    });
    const vc = clockService.getVectorClock();
    view[this.viewObject.shard_index].nodes.forEach(replica => vc.addClock(replica));

    if (sender !== "broadcast") {
      if (prev.length === 0) {
        // connect to sender's IO Server
        ioClient.connect(`http://${sender}`);
      } else {
        // Disconnect from previous IO Server
        if (ioClient.isConnected()) {
          ioClient.disconnect();
        }
        // Connect to sender's IO Server
        ioClient.connect(`http://${sender}`);
      }
    }
  }

  public async deleteView(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      await kvsService.clearKvs();
      this.viewObject.view = [];
      this.viewObject.shard_index = -1;
    });
  }

  public async sendViewChange(replicas: string[], view: Shard[], sender = ADDRESS): Promise<void> {
    const addresses = replicas.map(replicaAddress => `http://${replicaAddress}/kvs/admin/view/shards`);
    try {
      const reqBody = {
        view,
        sender,
      };
      const reqHeaders = { headers: { "Content-Type": "application/json" } };
      const reqPromises = addresses.map(address => axios.put(address, reqBody, { ...reqHeaders, timeout: 10000 }));

      await Promise.all(reqPromises);
    } catch (error) {
      logger.error("viewService:sendViewChange - " + error);
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
    );
  }

  public async getShardReplicas(shard_id: number): Promise<string[]> {
    let ret = [];
    await this.mutex.runExclusive(async () => {
      ret = this.viewObject.view[shard_id].nodes;
    });
    return ret;
  }
}

const myService = new ViewService();
export default myService;
