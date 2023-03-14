import { NextFunction, Request, Response } from "express";
import kvsService from "@/services/kvs.service";
import clockService from "@/services/clock.service";
import { UpdateKVDto } from "@/dtos/kv.dto";
import { HttpException } from "@/exceptions/HttpException";
import { KV } from "@/interfaces/kv.interface";
import { isEmpty } from "@/utils/util";
import { ADDRESS } from "@/config";
import { broadcast, IORunning } from "@/io/index.io";
import viewService from "@/services/view.service";

const CAUSAL_METADATA_KEY = "causal-metadata";

class KvsController {
  public kvsService = kvsService;

  public createOrUpdate = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      if (isEmpty(req.params) || isEmpty(req.params.key)) {
        throw new HttpException(400, "bad request");
      }
      const view = await viewService.getView();
      if (view.view.length === 0) {
        throw new HttpException(418, "uninitialized");
      }
      const valDto: UpdateKVDto = req.body;
      const kvData: KV = { key: req.params.key, val: valDto.val };

      // Get replicas
      let shard_id = this.kvsService.lookUp(viewService.num_shards, kvData.key);
      let replicas: Promise<string[]> = viewService.getShardReplicas(shard_id);

      const receivedClock = clockService.parseReceivedClock(req.body[CAUSAL_METADATA_KEY]);
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
      const prev = await this.kvsService.createOrUpdateKv(kvData);
      if (prev === undefined) {
        res.status(201).json({ "causal-metadata": metadata, "shard-id": shard_id });
      } else {
        res.status(200).json({ "causal-metadata": metadata, "shard-id": shard_id });
      }
      // broadcast write
      if (IORunning()) {
        const broadcastData = { key: kvData.key, val: kvData.val, "causal-metadata": metadata, sender: ADDRESS, "shard-id": shard_id };
        broadcast("kvs:write", broadcastData);
      }
    } catch (error) {
      next(error);
    }
  };

  public getAllKvKeys = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const view = await viewService.getView();
      if (view.view.length === 0) {
        throw new HttpException(418, "uninitialized");
      }
      const receivedClock = clockService.parseReceivedClock(req.body[CAUSAL_METADATA_KEY]);
      const localClock = clockService.getVectorClock();

      // getting replicas which contain all shards altogether
      for (let shard_id = 0; shard_id < viewService.num_shards; shard_id++) {
        let replicas: Promise<string[]> = viewService.getShardReplicas(shard_id);
      }

      if (localClock.validateClock(receivedClock)) {
        const metadata = Array.from(localClock.getClock());
        const keys = await this.kvsService.getAllKeys();
        res.status(200).json({ "causal-metadata": metadata, ...keys });
      } else {
        const { success, value }: { success: boolean; value: any } = await clockService.getCausalConsistency(undefined, receivedClock);
        if (success) {
          const metadata = Array.from(localClock.getClock());
          res.status(200).json({ "causal-metadata": metadata, ...value });
        } else {
          throw new HttpException(500, "timed out while waiting for depended updates");
        }
      }
    } catch (error) {
      next(error);
    }
  };

  public getKvByKey = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      if (isEmpty(req.params) || isEmpty(req.params.key)) {
        throw new HttpException(400, "bad request");
      }
      const view = await viewService.getView();
      if (view.view.length === 0) {
        throw new HttpException(418, "uninitialized");
      }

      const receivedClock = clockService.parseReceivedClock(req.body[CAUSAL_METADATA_KEY]);
      const localClock = clockService.getVectorClock();
      const key = req.params.key as string;
      // Get replicas
      let shard_id = this.kvsService.lookUp(viewService.num_shards, key);
      let replicas: Promise<string[]> = viewService.getShardReplicas(shard_id);
      if (localClock.validateClock(receivedClock)) {
        let val = await this.kvsService.getKv(key);
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
            throw new HttpException(500, "timed out while waiting for depended updates");
          }
        }
        const metadata = Array.from(localClock.getClock());
        if (val === undefined) {
          res.status(404).json({ "causal-metadata": metadata, "shard-id": shard_id });
        } else {
          res.status(200).json({ val, "causal-metadata": metadata, "shard-id": shard_id });
        }
      } else {
        if (IORunning()) {
          const { success, value, exists }: { success: boolean; value: string; exists: boolean } = await clockService.getCausalConsistency(
            key,
            receivedClock,
          );
          if (success) {
            const metadata = Array.from(localClock.getClock());
            if (exists) {
              res.status(200).json({ val: value, "causal-metadata": metadata, "shard-id": shard_id });
            } else {
              res.status(404).json({ "causal-metadata": metadata, "shard-id": shard_id });
            }
          } else {
            throw new HttpException(500, "timed out while waiting for depended updates");
          }
        }
      }
    } catch (error) {
      next(error);
    }
  };

  public deleteKvByKey = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      if (isEmpty(req.params) || isEmpty(req.params.key)) {
        throw new HttpException(400, "bad request");
      }
      const view = await viewService.getView();
      if (view.view.length === 0) {
        throw new HttpException(418, "uninitialized");
      }

      const receivedClock = clockService.parseReceivedClock(req.body[CAUSAL_METADATA_KEY]);
      const localClock = clockService.getVectorClock();
      if (localClock.validateClock(receivedClock)) {
        const key = req.params.key as string;
        const prev = await this.kvsService.deleteKv(key);
        let metadata = Array.from(localClock.getClock());
        // Get replicas
        let shard_id = this.kvsService.lookUp(viewService.num_shards, key);
        let replicas: Promise<string[]> = viewService.getShardReplicas(shard_id);
        if (prev === undefined) {
          res.status(404).json({ "causal-metadata": metadata, "shard-id": shard_id });
        } else {
          localClock.incrementClock(ADDRESS);
          metadata = Array.from(localClock.getClock());
          res.status(200).json({ "causal-metadata": metadata, "shard-id": shard_id });
          // broadcast delete
          if (IORunning()) {
            const broadcastData = { key, "causal-metadata": metadata, sender: ADDRESS, "shard-id": shard_id };
            broadcast("kvs:delete", broadcastData);
          }
        }
      }
    } catch (error) {
      next(error);
    }
  };
}

export default KvsController;
