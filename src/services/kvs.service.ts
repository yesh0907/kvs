import { ADDRESS } from "@/config";
import { KV } from "@/interfaces/kv.interface";
import { KvOperation } from "@/interfaces/kvOperation.interface";
import { ProxyResponse } from "@/interfaces/proxyRespnse.interface";
import { broadcast, IOEventEmitter, IORunning } from "@/io/index.io";
import VectorClock from "@/models/clock";
import kvsModel from "@/models/kvs.model";
import { logger } from "@/utils/logger";
import { Mutex } from "async-mutex";
import clockService from "./clock.service";

class KvsService {
  public kvs = kvsModel;
  public mutex = new Mutex();
  public proxyReqCount = 0;

  public async writeKv(shard_id: number, kvData: KV, receivedClock: VectorClock): Promise<{ prev: string | undefined; metadata: [string, number][] }> {
    const localClock = clockService.getVectorClock();
    if (localClock.validateClock(receivedClock)) {
      // already causally consistent
      localClock.incrementClock(ADDRESS);
    } else {
      // not causally consistent - but we can still update
      localClock.updateClock(receivedClock);
      localClock.incrementClock(ADDRESS);
    }
    const metadata = Array.from(localClock.getClock());
    const prev = await this.createOrUpdateKv(kvData);
    // broadcast write
    if (IORunning()) {
      const broadcastData = { key: kvData.key, val: kvData.val, "causal-metadata": metadata, sender: ADDRESS, shard_id };
      broadcast("kvs:write", broadcastData);
    }
    return { prev, metadata };
  }

  public async readKv(key: string, receivedClock: VectorClock): Promise<{ success: boolean; val?: string; metadata?: [string, number][] }> {
    const localClock = clockService.getVectorClock();

    if (localClock.validateClock(receivedClock)) {
      let val = await kvsService.getKv(key);
      if (IORunning() && val === undefined) {
        // check other replicas for the key
        const { success, value, exists }: { success: boolean; value: string; exists: boolean } = await clockService.getCausalConsistency(
          key,
          receivedClock,
        );
        if (success) {
          if (exists) {
            val = value;
          }
        } else {
          return { success: false };
        }
      }
      const metadata = Array.from(localClock.getClock());
      return { success: true, metadata, val };
    } else {
      if (IORunning()) {
        const { success, value, exists }: { success: boolean; value: string; exists: boolean } = await clockService.getCausalConsistency(
          key,
          receivedClock,
        );
        if (success) {
          const metadata = Array.from(localClock.getClock());
          return { success: true, metadata, val: exists ? value : undefined };
        } else {
          return { success: false };
        }
      }
    }
  }

  public async removeKv(shard_id: number, key: string, receivedClock: VectorClock): Promise<{ prev: string | undefined; metadata: [string, number][] }> {
    const localClock = clockService.getVectorClock();
    if (!localClock.validateClock(receivedClock)) {
      // not causally consistent - but we can still update
      localClock.updateClock(receivedClock);
    }

    const prev = await this.deleteKv(key);
    if (prev !== undefined) {
      localClock.incrementClock(ADDRESS);
    }
    const metadata = Array.from(localClock.getClock());
    // broadcast delete
    if (IORunning()) {
      const broadcastData = { key, "causal-metadata": metadata, sender: ADDRESS, shard_id };
      broadcast("kvs:delete", broadcastData);
    }
    return { prev, metadata };
  }

  public async retreiveAllKeys(
    receivedClock: VectorClock,
  ): Promise<{ success: boolean; count?: number; keys?: string[]; metadata?: [string, number][] }> {
    const localClock = clockService.getVectorClock();

    if (localClock.validateClock(receivedClock)) {
      const metadata = Array.from(localClock.getClock());
      const keys = await kvsService.getAllKeys();
      return { ...keys, metadata, success: true };
    } else {
      const { success, value }: { success: boolean; value: any } = await clockService.getCausalConsistency(undefined, receivedClock);
      if (success) {
        const metadata = Array.from(localClock.getClock());
        return { ...value, metadata, success: true };
      } else {
        return { success: false };
      }
    }
  }

  public async createOrUpdateKv(kvData: KV): Promise<string | undefined> {
    let prevVal: string | undefined;
    await this.mutex.runExclusive(async () => {
      prevVal = this.kvs.get(kvData.key);
      this.kvs.set(kvData.key, kvData.val);
    });
    return prevVal;
  }

  public async getAllKeys(): Promise<{ count: number; keys: string[] }> {
    const keys = Array.from(this.kvs.keys());
    return {
      count: keys.length,
      keys,
    };
  }

  public async getKv(key: string): Promise<string | undefined> {
    let value: string | undefined;
    await this.mutex.runExclusive(async () => {
      value = this.kvs.get(key);
    });
    return value;
  }

  public async deleteKv(key: string): Promise<string | undefined> {
    let prevValue: string | undefined;
    await this.mutex.runExclusive(async () => {
      prevValue = this.kvs.get(key);
      this.kvs.delete(key);
    });
    return prevValue;
  }

  public async updateKvs(newKvs: Map<string, string>): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.kvs = new Map<string, string>([...this.kvs, ...newKvs]);
    });
  }

  public async clearKvs(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.kvs.clear();
    });
  }

  public parseReceivedKvs(metadata: [string, string][]): Map<string, string> {
    const kvs = new Map<string, string>();
    if (metadata !== undefined) {
      metadata.reduce((kvs, [key, val]) => {
        kvs.set(key, val);
        return kvs;
      }, kvs);
    }
    return kvs;
  }

  public getCurrentKvs() {
    return this.kvs;
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

  public async proxyRequest(shard_id: number, op: KvOperation): Promise<ProxyResponse> {
    this.proxyReqCount++;
    const reqId = this.proxyReqCount;
    broadcast("shard:proxy-request", { shard_id, req: { id: reqId, op }, sender: ADDRESS });
    const res = await new Promise<ProxyResponse>(resolve => {
      const timeout = setTimeout(() => {
        logger.error(`timeout waiting for proxy request to shard: ${shard_id}`);
        resolve({ id: reqId, status: 503, metadata: op.metadata });
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
