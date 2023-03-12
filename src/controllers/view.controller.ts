import { NextFunction, Request, Response } from "express";
import viewService from "@/services/view.service";
import { isEmpty } from "@/utils/util";

class ViewController {
  public viewService = viewService;

  public getView = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const currView = await this.viewService.getView();
      res.status(200).json({ view: currView.view });
    } catch (error) {
      next(error);
    }
  };

  public setView = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      let sender = req.body.sender;
      if (isEmpty(sender)) {
        sender = "client";
      }
      await this.viewService.setView(req.body, sender);
      res.status(200).json();
    } catch (error) {
      next(error);
    }
  };

  public deleteView = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const old = await this.viewService.getView();
      if (old.view.length === 0) {
        res.status(418).json({ error: "uninitialized" });
      } else {
        await this.viewService.deleteView();
        res.status(200).json(old.view.length);
      }
    } catch (error) {
      next(error);
    }
  };

  // receives the setViewChange broadcast from a replica
  public setShards = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { sender, view } = req.body;
      await this.viewService.replaceView(view, sender);
      res.status(200).json();
    } catch (error) {
      next(error);
    }
  };
}

export default ViewController;
