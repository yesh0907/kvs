import compression from "compression";
import cookieParser from "cookie-parser";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import hpp from "hpp";
import morgan from "morgan";
import { createServer, type Server } from "http";
import { Server as IOServer } from "socket.io";
import { NODE_ENV, IP, PORT, LOG_FORMAT, ORIGIN, CREDENTIALS } from "@config";
import { Routes } from "@interfaces/routes.interface";
import errorMiddleware from "@middlewares/error.middleware";
import { logger, stream } from "@utils/logger";
import ioServer from "@/io/server/index.server";

class App {
  public app: express.Application;
  public env: string;
  public ip: string;
  public port: number;
  public io: IOServer;

  private httpServer:Server;

  constructor(routes: Routes[]) {
    this.app = express();
    this.httpServer = createServer(this.app);
    this.io = new IOServer(this.httpServer, {
      cors: {
        origin: ORIGIN,
      },
      transports: ["websocket"],
    });
    // give IOServer instance access to the socket.io server module
    ioServer.setServer(this.io);

    this.ip = IP;
    this.port = PORT;
    this.env = NODE_ENV || "development";

    this.initializeMiddlewares();
    this.initializeRoutes(routes);
    this.initializeErrorHandling();
  }

  public listen() {
    try {
      this.httpServer.listen(this.port, this.ip, () => {
        logger.info(`=================================`);
        logger.info(`======= ENV: ${this.env} =======`);
        logger.info(`🚀 App listening on ${this.ip}:${this.port}`);
        logger.info(`=================================`);
      });
    } catch (error) {
      logger.error(error);
      process.exit(1);
    }
  }

  public getServer() {
    return this.app;
  }

  private initializeMiddlewares() {
    this.app.use(morgan(LOG_FORMAT, { stream }));
    this.app.use(cors({ origin: ORIGIN, credentials: CREDENTIALS }));
    this.app.use(hpp());
    this.app.use(helmet());
    this.app.use(compression());
    this.app.use(express.json( { limit: "10mb" }));
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cookieParser());
  }

  private initializeRoutes(routes: Routes[]) {
    routes.forEach(route => {
      this.app.use("/", route.router);
    });
  }

  private initializeErrorHandling() {
    this.app.use(errorMiddleware);
  }
}

export default App;
