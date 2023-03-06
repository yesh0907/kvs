import { Router } from "express";
import KvsController from "@controllers/kvs.controller";
import { UpdateKVDto } from "@/dtos/kv.dto";
import { Routes } from "@/interfaces/routes.interface";
import validationMiddleware from "@/middlewares/validation.middleware";

class KvsRoute implements Routes {
  public path = "/kvs/data";
  public router = Router();
  public kvsController = new KvsController();

  constructor() {
    this.initializeRoutes();
  }

  private initializeRoutes() {
    this.router.get(`${this.path}`, this.kvsController.getAllKvKeys);
    this.router.get(`${this.path}/:key?`, this.kvsController.getKvByKey);
    this.router.delete(`${this.path}/:key?`, this.kvsController.deleteKvByKey);
    this.router.put(`${this.path}/:key?`, validationMiddleware(UpdateKVDto, "body"), this.kvsController.createOrUpdate);
  }
}

export default KvsRoute;
