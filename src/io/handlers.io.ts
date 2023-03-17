import { ADDRESS } from "@/config";
import { logger } from "@/utils/logger";
import viewService from "@/services/view.service";
import kvsService from "@/services/kvs.service";
import ioClient from "@/io/client/index.client";
import ioServer from "@/io/server/index.server";
import { broadcast, IOEventEmitter, sendTo } from "@/io/index.io";
import { maxIP } from "@/utils/util";
import { Shard } from "@/interfaces/shard.interface";
import { KvOperation } from "@/interfaces/kvOperation.interface";
import { KV, KvStore, ValWithCausalContext } from "@/interfaces/kv.interface";
import { ProxyResponse } from "@/interfaces/proxyRespnse.interface";
import { CausalContext, CausalMetadata } from "@/interfaces/causalContext.interface";

export const onViewChangeKill = async data => {
  if (data.includes(ADDRESS)) {
    logger.info("replica has been killed");
    await viewService.deleteView();
    if (ioServer.isListening()) {
      ioServer.shutdown();
    } else {
      ioClient.disconnect();
    }
  }
};

export const onViewChangeUpdate = async data => {
  const { view, sender }: { view: Shard[]; sender: string } = data;
  if (sender !== ADDRESS) {
    // apply view change from another replica
    const replicas = view.map(shard => shard.nodes).flat();
    if (replicas.includes(ADDRESS)) {
      logger.info(`received view change: ${JSON.stringify(view)} from ${sender}`);
      await viewService.replaceView(view, "broadcast");
    }
  }
};

export const onKvsAll = async data => {
  const { kvs }: { kvs: KvStore } = data;
  await kvsService.updateKvs(kvs);
};

export const onKvsWrite = async data => {
  const { key, val, sender }: { key: string; val: ValWithCausalContext; sender: string } = data;
  const receivedMetadata = val.causalContext.causalMetadata;
  const kvData = { key, val: val.val };
  logger.info(`received write for ${key}=${val.val} from ${sender}`);

  const localVal = await kvsService.getKv(key);
  if (localVal !== undefined && localVal.causalContext.timestamp === val.causalContext.timestamp) {
    // concurrent b/c they have the same timestamp
    logger.info(`concurrent and conflicting operations from ${sender}`);
    if (maxIP(sender, ADDRESS) === sender) {
      await kvsService.createOrUpdateKv(kvData, receivedMetadata, val);
    } else {
      logger.info(`telling sender to use ${key}=${localVal.val}`);
      // add shard id
      broadcast("kvs:write", { key, val: localVal, sender: ADDRESS });
    }
  } else {
    await kvsService.createOrUpdateKv(kvData, receivedMetadata, val);
    logger.info(`write ${key}=${val.val} to kvs`);
  }
};

export const onKvsDelete = async data => {
  const { key, causalContext, sender }: { key: string; causalContext: CausalContext; sender: string } = data;
  const receivedMetadata: CausalMetadata = causalContext.causalMetadata;
  logger.info(`received delete for ${key} from ${sender}`);

  const localVal = await kvsService.getKv(key);
  if (localVal !== undefined && localVal.causalContext.timestamp === causalContext.timestamp) {
    // concurrent b/c they have the same timestamp
    logger.info(`concurrent and conflicting operations from ${sender}`);
    if (maxIP(sender, ADDRESS) === sender) {
      logger.info(`deleted ${key} from kvs`);
      await kvsService.deleteKv(key, receivedMetadata);
    } else {
      // use write b/c delete won't remove the key, will just make it undefined
      logger.info(`not deleting ${key} - telling sender to use ${key}=${localVal.val}`);
      // add shard id
      broadcast("kvs:write", { key, val: localVal, sender: ADDRESS });
    }
  } else {
    await kvsService.deleteKv(key, receivedMetadata);
    logger.info(`deleted ${key} from kvs`);
  }
};

export const onCausalGetKey = async data => {
  const { metadata, key, sender }: { metadata: CausalMetadata; key: string; sender: string } = data;
  if (Object.keys(metadata).length === 0) {
    const broadcastData = { exists: false, key, val: undefined, requester: sender };
    broadcast("causal:update-key", broadcastData);
  } else {
    const kv = await kvsService.getKv(key);
    if (kv !== undefined) {
      const { causalContext } = kv;
      if (causalContext.timestamp >= metadata[key]) {
        const broadcastData = { exists: kv.val !== undefined, key, val: kv, requester: sender };
        broadcast("causal:update-key", broadcastData);
      }
    }
  }
};

export const onCausalUpdateKey = async data => {
  const { key, val, exists, requester }: { key: string; val: ValWithCausalContext; exists: boolean; requester: string } = data;
  if (val !== undefined) {
    const kv = await kvsService.getKv(key);
    if (kv === undefined || kv.causalContext.timestamp < val.causalContext.timestamp) {
      const { causalMetadata } = val.causalContext;
      if (exists) {
        await kvsService.createOrUpdateKv({ key, val: val.val }, causalMetadata, val);
      } else {
        await kvsService.deleteKv(key, causalMetadata, val);
      }
    }
  }
  if (requester === ADDRESS) {
    IOEventEmitter.emit(`causal:${key}-consistent`, { key, value: val, exists });
  }
};

export const onCausalGetKvs = async data => {
  const { metadata, keys, sender }: { metadata: CausalMetadata; keys: string[]; sender: string } = data;
  if (Object.keys(metadata).length === 0) {
    const kvs = await kvsService.getCurrentKvs();
    const broadcastData = { kvs, requester: sender };
    broadcast("causal:update-kvs", broadcastData);
  } else {
    const consistentVals: KvStore = {};
    for (const key of keys) {
      if (metadata[key] !== undefined) {
        const kv = await kvsService.getKv(key);
        if (kv !== undefined && kv.causalContext.timestamp >= metadata[key]) {
          consistentVals[key] = kv;
        }
      }
    }
    if (Object.keys(consistentVals).length > 0) {
      const broadcastData = { kvs: consistentVals, requester: sender };
      broadcast("causal:update-kvs", broadcastData);
    }
  }
};

export const onCausalUpdateKvs = async data => {
  const { kvs, requester }: { kvs: KvStore; requester: string } = data;
  if (Object.keys(kvs).length > 0) {
    for (const key of Object.keys(kvs)) {
      const kv = await kvsService.getKv(key);
      if (kv === undefined || kv.causalContext.timestamp < kvs[key].causalContext.timestamp) {
        const { causalMetadata } = kvs[key].causalContext;
        if (kvs[key].val !== undefined) {
          await kvsService.createOrUpdateKv({ key, val: kvs[key].val }, causalMetadata, kvs[key]);
        } else {
          await kvsService.deleteKv(key, causalMetadata, kvs[key]);
        }
      }
    }
  }
  if (requester === ADDRESS) {
    IOEventEmitter.emit(`causal:kvs-consistent`, { keys: Object.keys(kvs) });
  }
};

export const onReplicationConverge = async data => {
  const { kvs }: { kvs: KvStore } = data;
  if (Object.keys(kvs).length > 0) {
    for (const key of Object.keys(kvs)) {
      const kv = await kvsService.getKv(key);
      if (kv === undefined || kv.causalContext.timestamp < kvs[key].causalContext.timestamp) {
        const { causalMetadata } = kvs[key].causalContext;
        if (kvs[key].val !== undefined) {
          await kvsService.createOrUpdateKv({ key, val: kvs[key].val }, causalMetadata, kvs[key]);
        } else {
          await kvsService.deleteKv(key, causalMetadata, kvs[key]);
        }
      }
    }
  }
};

export const onShardProxyRequest = async data => {
  const { shard_id, req, sender }: { shard_id: number; req: { id: number; op: KvOperation }; sender: string } = data;
  const shardIndex = await viewService.getShardIndex();
  if (shard_id === shardIndex) {
    const response: ProxyResponse = {
      id: req.id,
      status: 200,
      metadata: undefined,
    };
    const { op } = req;
    if (op.type === "write") {
      const kvData: KV = { key: op.key, val: op.val };
      const receivedMetadata = op.metadata;
      const { prev, metadata } = await kvsService.writeKv(shardIndex, kvData, receivedMetadata);
      response.metadata = metadata;
      if (prev !== undefined) {
        response.status = 200;
      } else {
        response.status = 201;
      }
    } else if (op.type === "delete") {
      const receivedMetadata = op.metadata;
      const { prev, metadata } = await kvsService.removeKv(shardIndex, op.key, receivedMetadata);
      response.metadata = metadata;
      if (prev === undefined) {
        response.status = 404;
        response.exists = false;
      } else {
        response.status = 200;
        response.exists = true;
      }
    } else {
      const receivedMetadata = op.metadata;
      const { success, val, metadata } = await kvsService.readKv(op.key, receivedMetadata);
      if (success) {
        response.metadata = metadata;
        if (val !== undefined) {
          response.status = 200;
          response.val = val;
          response.exists = true;
        } else {
          response.status = 404;
          response.exists = false;
        }
      } else {
        response.status = 500;
      }
    }
    sendTo(sender, "shard:proxy-response", response);
  }
};

export const onShardProxyResponse = async data => {
  const { id }: ProxyResponse = data;
  IOEventEmitter.emit(`shard:proxy-response:${id}`, data);
};
