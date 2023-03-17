import { ADDRESS } from "@/config";
import { CausalContext, CausalMetadata } from "@/interfaces/causalContext.interface";
import { KV, KvRequest, KvStore, ValWithCausalContext } from "@/interfaces/kv.interface";
import { ProxyResponse } from "@/interfaces/proxyRespnse.interface";
import { broadcast, IOEventEmitter, IORunning } from "@/io/index.io";
import kvsModel from "@/models/kvs.model";
import updates from "@/models/update.model";
import { logger } from "@/utils/logger";
import { Mutex } from "async-mutex";
import { getConsistentKeys, getConsistentVal } from "./replication.service";

class KvsService {
  public kvs = kvsModel;
  public mutex = new Mutex();
  public proxyReqCount = 0;

  public async writeKv(
    shard_id: number,
    kvData: KV,
    receivedMetadata: CausalMetadata,
  ): Promise<{ prev: string | undefined; metadata: CausalMetadata }> {
    const { prev, val } = await this.createOrUpdateKv(kvData, receivedMetadata);
    const { causalContext } = val;
    const updatedReceivedCausalMetadata = this.updateReceivedCausalMetadata(receivedMetadata);
    const metadata = { ...updatedReceivedCausalMetadata, [kvData.key]: causalContext.timestamp };
    // broadcast write
    if (IORunning()) {
      const broadcastData = { key: kvData.key, val, sender: ADDRESS, shard_id };
      broadcast("kvs:write", broadcastData);
    }
    return { prev, metadata };
  }

  public async readKv(key: string, receivedMetadata: CausalMetadata): Promise<{ success: boolean; val: string; metadata: CausalMetadata }> {
    const kv = await this.getKv(key);
    if (kv !== undefined) {
      const { causalContext } = kv;
      if (receivedMetadata[key] === undefined || causalContext.timestamp >= receivedMetadata[key]) {
        const metadata = this.updateCausalMetadata(key, causalContext, receivedMetadata);
        return { success: true, val: kv.val, metadata };
      }
    }
    // ran when replica does not hav kv, or when the kv is not consistent
    if (IORunning()) {
      const { success, value, exists }: { success: boolean; value: any; exists: boolean } = await getConsistentVal(key, receivedMetadata);
      if (success) {
        if (exists) {
          const { val, causalContext } = value as ValWithCausalContext;
          const metadata = this.updateCausalMetadata(key, causalContext, receivedMetadata);
          return { success: true, metadata, val };
        }
      } else {
        return { success: false, metadata: receivedMetadata, val: undefined };
      }
    }
    if (kv === undefined || kv.val === undefined) {
      return { success: true, metadata: receivedMetadata, val: undefined };
    } else {
      // causal dependency not satisfied and can't be satisfied (partioned)
      return { success: false, metadata: receivedMetadata, val: undefined };
    }
  }

  public async removeKv(
    shard_id: number,
    key: string,
    receivedMetadata: CausalMetadata,
  ): Promise<{ prev: string | undefined; metadata: CausalMetadata }> {
    const { prev, val } = await this.deleteKv(key, receivedMetadata);
    const { causalContext } = val;
    const updatedReceivedCausalMetadata = this.updateReceivedCausalMetadata(receivedMetadata);
    let metadata = updatedReceivedCausalMetadata;
    if (prev !== undefined) {
      metadata = { ...metadata, [key]: causalContext.timestamp };
    }
    // broadcast delete
    if (IORunning()) {
      const broadcastData = { key, causalContext, sender: ADDRESS, shard_id };
      broadcast("kvs:delete", broadcastData);
    }
    return { prev, metadata };
  }

  public async retreiveAllKeys(
    receivedMetadata: CausalMetadata,
  ): Promise<{ success: boolean; count?: number; keys?: string[]; metadata?: CausalMetadata }> {
    const inconsistentKeys:string[] = [];
    for (const key in receivedMetadata) {
      if (this.kvs[key] === undefined || this.kvs[key].causalContext.timestamp < receivedMetadata[key]) {
        logger.info(`inconsistent for key ${key} - local: ${this.kvs[key]?.causalContext.timestamp} - received: ${receivedMetadata[key]}`);
        inconsistentKeys.push(key);
      }
    }
    if (inconsistentKeys.length === 0) {
      const keys = await this.getAllKeys();
      const metadata = this.updateReceivedCausalMetadata(receivedMetadata);
      return { ...keys, metadata, success: true };
    } else {
      if (IORunning()) {
        const { success, value }: { success: boolean; value: any } = await getConsistentKeys(inconsistentKeys, receivedMetadata);
        if (success) {
          const metadata = this.updateReceivedCausalMetadata(receivedMetadata);
          return { ...value, metadata, success: true };
        } else {
          return { success: false };
        }
      } else {
        return { success: false };
      }
    }
  }

  public async createOrUpdateKv(kvData: KV, causalMetadata: CausalMetadata, valWithContext?: ValWithCausalContext): Promise<{ prev: string | undefined; val: ValWithCausalContext }> {
    let prevVal: string | undefined;
    const timestamp = Date.now();
    let val: ValWithCausalContext = {
      val: kvData.val,
      causalContext: {
        timestamp,
        causalMetadata,
      },
    }
    if (valWithContext !== undefined) {
      val = valWithContext;
    }
    await this.mutex.runExclusive(async () => {
      const prev = this.kvs[kvData.key];
      if (prev !== undefined) {
        prevVal = prev.val;
      }
      this.kvs[kvData.key] = val;
    });
    updates.last = timestamp;
    return { prev: prevVal, val };
  }

  public async getAllKeys(): Promise<{ count: number; keys: string[] }> {
    // search for only non undefined values in kvs
    const keys = Object.keys(this.kvs).filter(key => this.kvs[key].val !== undefined);
    return {
      count: keys.length,
      keys,
    };
  }

  public async getKv(key: string): Promise<ValWithCausalContext | undefined> {
    let value: ValWithCausalContext;
    await this.mutex.runExclusive(async () => {
      value = this.kvs[key];
    });
    return value;
  }

  public async deleteKv(key: string, causalMetadata: CausalMetadata, valWithContext?: ValWithCausalContext): Promise<{ prev: string | undefined; val: ValWithCausalContext }> {
    let prevVal: string | undefined;
    const timestamp = Date.now();
    let val: ValWithCausalContext = {
      val: undefined,
      causalContext: {
        timestamp,
        causalMetadata,
      },
    }
    if (valWithContext !== undefined) {
      val = valWithContext;
    }
    await this.mutex.runExclusive(async () => {
      const prev = this.kvs[key];
      if (prev !== undefined) {
        prevVal = prev.val;
        this.kvs[key] = val;
      }
    });
    updates.last = timestamp;
    return { prev: prevVal, val };
  }

  public async updateKvs(newKvs: KvStore): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.kvs = { ...this.kvs, ...newKvs };
    });
    updates.last = Date.now();
  }

  public async clearKvs(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.kvs = {};
    });
    updates.last = Date.now();
  }

  public getCurrentKvs() {
    return this.kvs;
  }

  public updateCausalMetadata(key: string, causalContext: CausalContext, receivedMetadata: CausalMetadata): CausalMetadata {
    const metadata: CausalMetadata = { ...receivedMetadata, [key]: causalContext.timestamp };
    for (const [k, timestamp] of Object.entries(causalContext.causalMetadata)) {
      if (k !== key) {
        const receivedCMTimestamp = receivedMetadata[k] || 0;
        // Update Causal Context with the most up to date timestamp of keys
        metadata[k] = Math.max(timestamp, receivedCMTimestamp);
      }
    }
    return metadata;
  }

  public updateReceivedCausalMetadata(receivedMetadata: CausalMetadata) {
    const metadata: CausalMetadata = { ...receivedMetadata };
    for (const [k, timestamp] of Object.entries(receivedMetadata)) {
      const localTimestamp = this.kvs[k]?.causalContext.timestamp || 0;
      // Update Causal Context with the most up to date timestamp of keys
      metadata[k] = Math.max(timestamp, localTimestamp);
    }
    return metadata;
  }

  public lookUp(num_shards: number, str: string) {
    return this.hashFunction(str) % num_shards;
  }

  public hashFunction(str: string) {
    const p = 31;
    const m = 1e9 + 9;
    let hash_value = 0;
    let p_pow = 1;
    for (let i = 0; i < str.length; i++) {
      const c = str.charCodeAt(i);
      hash_value = (hash_value + (c - 98) * p_pow) % m;
      p_pow = (p_pow * p) % m;
    }

    return Math.abs(hash_value);
  }

  public async proxyRequest(shard_id: number, op: KvRequest): Promise<ProxyResponse> {
    this.proxyReqCount++;
    const reqId = this.proxyReqCount;
    broadcast("shard:proxy-request", { shard_id, req: { id: reqId, op }, sender: ADDRESS });
    const res = await new Promise<ProxyResponse>(resolve => {
      const timeout = setTimeout(() => {
        logger.error(`timeout waiting for proxy request to shard: ${shard_id} for req ${reqId}`);
        resolve({ id: reqId, status: 503 });
      }, 20000);
      IOEventEmitter.once(`shard:proxy-response:${reqId}`, (data: ProxyResponse) => {
        clearTimeout(timeout);
        logger.info(`received proxy response for request ${reqId}`);
        resolve(data);
      });
    });

    return res;
  }
}

const kvsService = new KvsService();
export default kvsService;
