import { ADDRESS } from "@/config";
import { logger } from "@/utils/logger";
import viewService from "@/services/view.service";
import clockService from "@/services/clock.service";
import kvsService from "@/services/kvs.service";
import ioClient from "@/io/client/index.client";
import ioServer from "@/io/server/index.server";
import { broadcast, IOEventEmitter } from "@/io/index.io";
import { maxIP } from "@/utils/util";
import queueService from "@/services/queue.service";
import { Queue } from "@/interfaces/queue.interface";

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
  if (data.includes(ADDRESS)) {
    logger.info("replica is making a view change");
    await viewService.updateView(data);
  }
};

export const onKvsAll = async data => {
  const { clock, kvs }: { clock: [string, number][]; kvs: [string, string][] } = data;
  const vc = clockService.parseReceivedClock(clock);
  const receivedKvs = kvsService.parseReceivedKvs(kvs);
  clockService.getVectorClock().updateClock(vc);
  await kvsService.updateKvs(receivedKvs);
};

export const onKvsWrite = async data => {
  const clock: [string, number][] = data["causal-metadata"];
  const { key, val, sender }: { key: string; val: string; sender: string } = data;
  const kvData = { key, val };
  logger.info(`received write for ${key}=${val} from ${sender}`);

  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();

  const equal = localVC.equals(receivedVC);
  const localVal = await kvsService.getKv(key);
  if (equal || localVC.compareClocks(sender, receivedVC)) {
    if (equal) {
      // concurrent b/c they have the same VC
      logger.info(`concurrent and conflicting operations from ${sender}`);
      if (localVal === undefined || (localVal !== undefined && maxIP(sender, ADDRESS) === sender)) {
        // write to kvs if sender has higher IP
        localVC.updateClock(receivedVC);
        await kvsService.createOrUpdateKv(kvData);
        logger.info(`write ${key}=${val} to kvs`);
      } else {
        logger.info(`telling sender to use ${key}=${localVal}`);
        broadcast("kvs:write", { key, val: localVal, sender: ADDRESS, "causal-metadata": Array.from(localVC.getClock()) });
      }
    } else {
      localVC.updateClock(receivedVC);
      await kvsService.createOrUpdateKv(kvData);
      logger.info(`write ${key}=${val} to kvs`);
    }
  } else {
    // add to queue
    const q: Queue = {
      action: "write",
      key,
      val,
      sender,
      vc: clock,
    };
    logger.info(`pushed write ${key}=${val} from ${sender} to queue`);
    queueService.push(q);
  }
};

export const onKvsDelete = async data => {
  const clock: [string, number][] = data["causal-metadata"];
  const { key, sender }: { key: string; sender: string } = data;
  logger.info(`received delete for ${key} from ${sender}`);

  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();

  const equal = localVC.equals(receivedVC);
  const localVal = await kvsService.getKv(key);

  if (equal || localVC.compareClocks(sender, receivedVC)) {
    if (equal) {
      // concurrent b/c they have the same VC
      logger.info(`concurrent and conflicting operation from ${sender}}`);
      if (localVal !== undefined && maxIP(sender, ADDRESS) === sender) {
        // delete from kvs if sender has higher IP
        const prev = await kvsService.deleteKv(key);
        logger.info(`deleted ${key} from kvs`);
        if (prev !== undefined) {
          localVC.updateClock(receivedVC);
        }
      } else {
        logger.info(`not deleting ${key} from kvs`);
      }
    } else {
      const prev = await kvsService.deleteKv(key);
      logger.info(`deleted ${key} from kvs`);
      if (prev !== undefined) {
        localVC.updateClock(receivedVC);
      }
    }
  } else {
    // add to queue
    const q: Queue = {
      action: "delete",
      key,
      sender,
      vc: clock,
    };
    logger.info(`pushed delete ${key} from ${sender} to queue`);
    queueService.push(q);
  }
};

export const onCausalGetKey = async data => {
  const { clock, key, sender }: { clock: [string, number][]; key: string; sender: string } = data;
  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();
  // local clock should be ahead of received clock to help with causal consistency - local clock > received clock
  if (localVC.validateClock(receivedVC)) {
    logger.info(`local clock ahead of received clock for ${key} from ${sender}`);
    const value = await kvsService.getKv(key);
    const broadcastData = { exists: value !== undefined, key, value, sender, clock: Array.from(localVC.getClock()) };
    broadcast("causal:update-key", broadcastData);
  }
};

export const onCausalUpdateKey = async data => {
  const { clock, key, value, exists }: { clock: [string, number][]; key: string; value: string; exists: boolean } = data;
  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();
  if (!localVC.validateClock(receivedVC)) {
    localVC.updateClock(receivedVC);
    if (exists) {
      await kvsService.createOrUpdateKv({ key, val: value });
    } else {
      await kvsService.deleteKv(key);
    }
    IOEventEmitter.emit(`causal:${key}-consistent`, { key, value, exists });
  }
};

export const onCausalGetKvs = async data => {
  const { clock, sender }: { clock: [string, number][]; sender: string } = data;
  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();
  logger.info(`received causal:get-kvs from ${sender}`);
  logger.info(`local clock: ${localVC.getClock()}`);
  logger.info(`received clock: ${receivedVC.getClock()}`);
  // local clock should be ahead of received clock to help with causal consistency - local clock > received clock
  if (localVC.validateClock(receivedVC)) {
    logger.info(`local clock ahead of received clock from ${sender}`);
    const kvs = await kvsService.getCurrentKvs();
    const broadcastData = { kvs: Array.from(kvs), sender, clock: Array.from(localVC.getClock()) };
    broadcast("causal:update-kvs", broadcastData);
  }
};

export const onCausalUpdateKvs = async data => {
  const { clock, kvs, sender }: { clock: [string, number][]; kvs: [string, string][]; sender: string } = data;
  const receivedVC = clockService.parseReceivedClock(clock);
  const localVC = clockService.getVectorClock();
  if (!localVC.validateClock(receivedVC)) {
    logger.info(`causal:kvs-update updating local kvs from ${sender}`)
    localVC.updateClock(receivedVC);
    const receivedKvs = await kvsService.parseReceivedKvs(kvs);
    await kvsService.updateKvs(receivedKvs);
    const data = await kvsService.getAllKeys();
    IOEventEmitter.emit(`causal:kvs-consistent`, data);
  }
};

export const onReplicationConverge = async data => {
  const { clock, sender, kvs }: { clock: [string, number][]; sender: string; kvs: [string, string][] } = data;
  const localVc = clockService.getVectorClock();
  const receivedVc = clockService.parseReceivedClock(clock);
  const receivedKvs = kvsService.parseReceivedKvs(kvs);
  // update replica if local clock is behind received clock
  if (!localVc.validateClock(receivedVc)) {
    logger.info(`converging replica from ${sender}`);
    localVc.updateClock(receivedVc);
    await kvsService.updateKvs(receivedKvs);
  }
};
