import { ADDRESS } from "@/config";
import { broadcast, IOEventEmitter, IORunning } from "@/io/index.io";
import { CausalMetadata } from "@/interfaces/causalContext.interface";
import { logger } from "@/utils/logger";
import { ValWithCausalContext } from "@/interfaces/kv.interface";
import kvsService from "./kvs.service";
import updates from "@/models/update.model";

export const getConsistentVal = async (key: string, receivedMetadata: CausalMetadata): Promise<{ success: boolean; value: any; exists: boolean }> => {
  logger.info(`getting causal consistency for key: ${key} with received metadata: ${JSON.stringify(receivedMetadata)}`);
  const message = { metadata: receivedMetadata, sender: ADDRESS, key };
  broadcast("causal:get-key", message);
  const res = await new Promise<{ success: boolean; value: ValWithCausalContext; exists: boolean }>(resolve => {
    const timeout = setTimeout(() => {
      logger.error(`timeout waiting for causal consistency on key: ${key}`);
      resolve({ success: false, value: undefined, exists: false });
    }, 20000);
    IOEventEmitter.once(`causal:${key}-consistent`, data => {
      clearTimeout(timeout);
      const { key: k, value, exists }: { key: string; value: ValWithCausalContext; exists: boolean } = data;
      logger.info(`received causal consistency for key: ${k}`);
      resolve({ success: true, value, exists });
    });
  });

  return res;
};

export const getConsistentKeys = async (inconsistentKeys: string[], receivedMetadata: CausalMetadata): Promise<{ success: boolean; value: any }> => {
  logger.info(`getting causal consistency for ${inconsistentKeys} with received metadata: ${JSON.stringify(receivedMetadata)}`);
  const message = { metadata: receivedMetadata, keys: inconsistentKeys, sender: ADDRESS };
  broadcast("causal:get-kvs", message);
  const receivedKeys = new Set<string>();
  const res = await new Promise<{ success: boolean; value: any }>(resolve => {
    const timeout = setTimeout(() => {
      logger.error(`timeout waiting for causal consistency on kvs`);
      resolve({ success: false, value: undefined });
    }, 20000);
    IOEventEmitter.on(`causal:kvs-consistent`, async data => {
      for (const key of data.keys) {
        receivedKeys.add(key);
      }
      if (receivedKeys.size === inconsistentKeys.length) {
        clearTimeout(timeout);
        logger.info(`received causal consistency for kvs`);
        const value = await kvsService.getAllKeys();
        resolve({ success: true, value });
      }
    });
  });
  return res;
};

export const makeEventuallyConsistent = () => {
  setInterval(async () => {
    if (updates.prevUpdateTime !== updates.last && Date.now() - updates.last >= 10000 && IORunning()) {
      logger.info(`making updates eventually consistent`);
      const kvs = await kvsService.getCurrentKvs();
      broadcast("replication:converge", { kvs });
      updates.prevUpdateTime = updates.last;
    }
  }, 1000);
};
