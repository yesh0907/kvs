import { NextFunction, Request, Response } from "express";
import kvsService from "@/services/kvs.service";
import clockService from "@/services/clock.service";
import { UpdateKVDto } from "@/dtos/kv.dto";
import { HttpException } from "@/exceptions/HttpException";
import { KV } from "@/interfaces/kv.interface";
import { isEmpty } from "@/utils/util";
import viewService from "@/services/view.service";
import { logger } from "@/utils/logger";
import { KvOperation } from "@/interfaces/kvOperation.interface";

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
      const receivedClock = clockService.parseReceivedClock(req.body[CAUSAL_METADATA_KEY]);

      const shard_id = this.kvsService.lookUp(viewService.num_shards, kvData.key);
      const shardIndex = await viewService.getShardIndex();
      if (shard_id !== shardIndex) {
        // proxy request
        logger.info(`Proxying request to shard ${shard_id}`);
        const op: KvOperation = {
          key: kvData.key,
          val: kvData.val,
          metadata: Array.from(receivedClock.getClock()),
          type: "write",
        };
        const proxyRes = await this.kvsService.proxyRequest(shard_id, op);
        if (proxyRes.status !== 503) {
          res.status(proxyRes.status).json({ "causal-metadata": proxyRes.metadata });
        } else {
          res.status(503).json({ error: "upstream down", upstream: { shard_id, nodes: viewService.getShardReplicas(shard_id) } });
        }
      } else {
        const { prev, metadata } = await this.kvsService.writeKv(shardIndex, kvData, receivedClock);
        if (prev === undefined) {
          res.status(201).json({ "causal-metadata": metadata });
        } else {
          res.status(200).json({ "causal-metadata": metadata });
        }
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
      const { success, keys, count, metadata } = await this.kvsService.retreiveAllKeys(receivedClock);
      if (success) {
        res.status(200).json({ keys, count, "causal-metadata": metadata });
      } else {
        throw new HttpException(500, "timed out while waiting for depended updates");
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
      const key = req.params.key as string;

      const shard_id = this.kvsService.lookUp(viewService.num_shards, key);
      const shardIndex = await viewService.getShardIndex();
      if (shard_id !== shardIndex) {
        // proxy request
        logger.info(`Proxying request to shard ${shard_id}`);
        const op: KvOperation = {
          key,
          metadata: Array.from(receivedClock.getClock()),
          type: "read",
        };
        const proxyRes = await this.kvsService.proxyRequest(shard_id, op);
        if (proxyRes.status === 503) {
          res.status(503).json({ error: "upstream down", upstream: { shard_id, nodes: viewService.getShardReplicas(shard_id) } });
        } else if (proxyRes.status === 500) {
          throw new HttpException(500, "timed out while waiting for depended updates");
        } else {
          const resData = {
            "causal-metadata": proxyRes.metadata,
          };
          if (proxyRes.exists) {
            resData["val"] = proxyRes.val;
          }
          res.status(proxyRes.status).json(resData);
        }
      } else {
        const { success, val, metadata } = await this.kvsService.readKv(key, receivedClock);
        if (success) {
          if (val === undefined) {
            res.status(404).json({ "causal-metadata": metadata });
          } else {
            res.status(200).json({ "causal-metadata": metadata, val });
          }
        } else {
          throw new HttpException(500, "timed out while waiting for depended updates");
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
      const key = req.params.key as string;

      const shard_id = this.kvsService.lookUp(viewService.num_shards, key);
      const shardIndex = await viewService.getShardIndex();

      if (shard_id !== shardIndex) {
        // proxy request
        logger.info(`Proxying request to shard ${shard_id}`);
        const op: KvOperation = {
          key,
          metadata: Array.from(receivedClock.getClock()),
          type: "delete",
        };
        const proxyRes = await this.kvsService.proxyRequest(shard_id, op);
        if (proxyRes.status !== 503) {
          res.status(proxyRes.status).json({ "causal-metadata": proxyRes.metadata });
        } else {
          res.status(503).json({ error: "upstream down", upstream: { shard_id, nodes: viewService.getShardReplicas(shard_id) } });
        }
      } else {
        const { prev, metadata } = await this.kvsService.removeKv(shardIndex, key, receivedClock);
        if (prev === undefined) {
          res.status(404).json({ "causal-metadata": metadata });
        } else {
          res.status(200).json({ "causal-metadata": metadata });
        }
      }
    } catch (error) {
      next(error);
    }
  };
}

export default KvsController;
