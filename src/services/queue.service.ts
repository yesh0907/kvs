import { Queue } from "@/interfaces/queue.interface";
import queue from "@/models/queue";
import clockService from "@/services/clock.service";
import { Mutex } from "async-mutex";
import kvsService from "@/services/kvs.service";

class QueueService {
  public queue = queue;
  public mutex = new Mutex();

  constructor() {
    setInterval(async () => {
      await this.pop();
    }, 1000);
  }

  public async getQueue(): Promise<Queue[]> {
    let ret: Queue[];
    await this.mutex.runExclusive(async () => {
      ret = this.queue;
    });
    return ret;
  }

  public async setQueue(incoming: Queue[]): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.queue = incoming;
    });
  }

  public async push(incoming: Queue): Promise<void> {
    await this.mutex.runExclusive(async () => {
      this.queue.push(incoming);
    });
  }

  public async pop(): Promise<void> {
    await this.mutex.runExclusive(async () => {
      for (let i = 0; i < this.queue.length; i++) {
        const q = this.queue[i];
        const { sender } = q;
        const vc = clockService.parseReceivedClock(q.vc);
        const localVC = clockService.getVectorClock();
        if (localVC.compareClocks(sender, vc)) {
          await this.resolve(q);
          this.queue.splice(i, 1);
        } else if (localVC.validateClock(vc)) {
          // remove operation from queue if local VC is ahead of received VC
          this.queue.splice(i, 1);
        }
      }
    });
  }

  public async resolve(q: Queue): Promise<void> {
    const localVC = clockService.getVectorClock();
    const { action, vc } = q;
    if (action === 'write') {
      const { key, val } = q;
      await kvsService.createOrUpdateKv({key, val});
    } else if (action === 'delete') {
      const { key } = q;
      await kvsService.deleteKv(key);
    } else if (action === 'kvs') {
      const kvs = kvsService.parseReceivedKvs(q.kvs);
      await kvsService.updateKvs(kvs);
    }
    localVC.updateClock(clockService.parseReceivedClock(vc));
  }
}

const queueService = new QueueService();
export default queueService;
