import { Router } from "express";
import ViewController from "@controllers/view.controller";
import { Routes } from "@/interfaces/routes.interface";

class ViewRoute implements Routes {
  public path = "/kvs/admin/view";
  public router = Router();
  public viewController = new ViewController();

  constructor() {
    this.initializeRoutes();
  }

  private initializeRoutes() {
    this.router.get(`${this.path}`, this.viewController.getView);
    this.router.delete(`${this.path}`, this.viewController.deleteView);
    this.router.put(`${this.path}`, this.viewController.setView);
    this.router.put(`${this.path}/shards`, this.viewController.setShards)
  }
}

export default ViewRoute;
