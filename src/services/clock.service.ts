import VectorClock from "@/models/clock";
import { ADDRESS } from "@/config";
import { logger } from "@/utils/logger";
import { isEmpty } from "@/utils/util";
import { broadcast, IOEventEmitter } from "@/io/index.io";

// should use mutex to make thread safe
class ClockService {
  private vectorClock: VectorClock;

  constructor(processIds: string[]) {
    this.vectorClock = new VectorClock(processIds);
  }

  getVectorClock(): VectorClock {
    return this.vectorClock;
  }

  parseReceivedClock(metadata: [string, number][]): VectorClock {
    const vc = new VectorClock([]);
    if (metadata !== undefined && !isEmpty(metadata)) {
      metadata.reduce((clock, [key, val]) => {
        clock.setClock(key, val);
        return clock;
      }, vc);
    }
    // logger.info("received VC: " + this.vcToString(vc));
    return vc;
  }

  vcToString(vc: VectorClock): string {
    return JSON.stringify(Array.from(vc.getClock()));
  }

  async getCausalConsistency(key: string, receivedClock: VectorClock): Promise<{ success: boolean, value: any, exists: boolean }> {
    logger.info(`getting causal consistency for key: ${key} with received clock: ${this.vcToString(receivedClock)}`);
    const localClock = this.getVectorClock();
    let res = {
      success: false,
      value: undefined,
      exists: false
    }

    // If local clock is less than received clock, perform broadcast and update local clock
    if(!localClock.validateClock(receivedClock)) {
      // Step 1: Broadcast to all nodes asking for a vector clock >= received clock and key or kvs
      const message = { clock: Array.from(receivedClock.getClock()), sender: ADDRESS };
      if (key === undefined) {
        broadcast('causal:get-kvs', message);
        // Step 4: Wait for data and proceed to process request
        res = await new Promise<{ success: boolean, value: any, exists: boolean }>(resolve => {
          const timeout = setTimeout(() => {
            logger.error(`timeout waiting for causal consistency on key: ${key}`);
            resolve({ success: false, value: undefined, exists: false });
          }, 20000);
          IOEventEmitter.once(`causal:kvs-consistent`, (data) => {
            clearTimeout(timeout);
            logger.info(`received causal consistency for kvs`);
            resolve({ success: true, value: data, exists: true });
          });
        });
      } else {
        broadcast('causal:get-key', {key, ...message});
        // Step 4: Wait for data and proceed to process request
        res = await new Promise<{ success: boolean, value: string, exists: boolean }>(resolve => {
          const timeout = setTimeout(() => {
            logger.error(`timeout waiting for causal consistency on key: ${key}`);
            resolve({ success: false, value: undefined, exists: false });
          }, 20000);
          IOEventEmitter.once(`causal:${key}-consistent`, (data) => {
            clearTimeout(timeout);
            const { key: k, value: v, exists }:{key: string; value: string; exists: boolean} = data;
            if (k === key) {
              logger.info(`received causal consistency for key: ${k}`);
              resolve({ success: true, value: v, exists });
            }
          });
        });
      }
    }

    return res;
  }
}

const VC = new ClockService([ADDRESS]);
export default VC;
