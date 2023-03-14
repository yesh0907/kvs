import { KV } from "@/interfaces/kv.interface";
import kvsModel from "@/models/kvs.model";
import { Mutex } from "async-mutex";

class KvsService {
  public kvs = kvsModel;
  public mutex = new Mutex();

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
    // logger.info(`received KVS: ${JSON.stringify(Object.entries(kvs))}`);
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

    return hash_value;
  }
}

const kvsService = new KvsService();
export default kvsService;
