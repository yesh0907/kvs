import { io as ioc, type Socket } from "socket.io-client";
import { logger } from "@utils/logger";
import { onViewChangeKill, onViewChangeUpdate, onKvsAll, onKvsWrite, onKvsDelete, onCausalGetKey, onCausalUpdateKey, onReplicationConverge, onCausalGetKvs, onCausalUpdateKvs, onShardProxyRequest, onShardProxyResponse } from "@io/handlers.io";
import viewService from "@/services/view.service";
import { broadcast, broadcastAck } from "@/io/index.io";
import { NODE_ENV } from "@/config";

// Handles socket.io client-side logic
class IOClient {
  private socket: Socket;
  private address: string;
  private connected: boolean;

  constructor() {
    this.socket = null;
    this.address = "";
    this.connected = false;
  }

  public isConnected() {
    return this.connected;
  }

  public getIP() {
    // remove http:// and / at the end from address
    const start = this.address.indexOf("//") + 2;
    const end = this.address.indexOf("/", start) === -1 ? this.address.length : this.address.indexOf("/", start);
    return this.address.substring(start, end);
  }

  // connects to a socket.io server
  public connect(address: string) {
    if (NODE_ENV === "test") {
      return;
    }
    this.address = address;
    this.socket = ioc(this.address, {
      transports: ["websocket"],
      upgrade: false,
    });

    // handle connection
    this.socket.on("connect", this.onConnect.bind(this));
    this.socket.on("disconnect", this.onDisconnect.bind(this));

    // handle events from server
    this.handleEvents();
  }

  public disconnect() {
    this.socket.disconnect();
  }

  private handleEvents() {
    this.socket.on("kvs:all", onKvsAll);
    this.socket.on("kvs:write", onKvsWrite);
    this.socket.on("kvs:delete", onKvsDelete)
    this.socket.on("viewchange:kill", onViewChangeKill);
    this.socket.on("viewchange:update", onViewChangeUpdate);
    this.socket.on("causal:get-key", onCausalGetKey);
    this.socket.on("causal:update-key", onCausalUpdateKey);
    this.socket.on("causal:get-kvs", onCausalGetKvs);
    this.socket.on("causal:update-kvs", onCausalUpdateKvs);
    this.socket.on("replication:converge", onReplicationConverge);
    this.socket.on("shard:proxy-request", onShardProxyRequest);
    this.socket.on("shard:proxy-response", onShardProxyResponse);
  }

  private onConnect() {
    logger.info(`🔗 connected to server at ${this.address}`);
    this.connected = true;
  }

  private onDisconnect(reason) {
    logger.info(`🔗 disconnected from server because ${reason}`);
    this.connected = false;
  }

  public broadcast(event: string, data: any) {
    logger.info(`broadcasting ${event} with data: ${JSON.stringify(data)}`);
    this.socket.timeout(10000).emit("broadcast-ping", async err => {
      if (err) {
        logger.info("IO server is down. Making view change");
        await viewService.changeIOServer();
        // rebroadcast change
        logger.info("Rebroadcasting change");
        broadcast(event, data);
      }
      else {
        logger.info(`relaying to server to broadcast ${event} with data: ${JSON.stringify(data)}`);
        // use server to relay the broadcast to all nodes
        this.socket.emit("relay-broadcast", { event, data });
        // send data to server too
        this.socket.emit(event, data);
      }
    });
  }

  public broadcastAck(event: string, data: any) {
    // send event to server first and wait for ack before broadcasting to all other nodes
    logger.info(`sending ${event} to server with data: ${JSON.stringify(data)}`);
    this.socket.timeout(10000).emit(event, data, async err => {
      if (err) {
        logger.info("IO server is down. Making view change");
        await viewService.changeIOServer();
        // rebroadcast change
        logger.info("Rebroadcasting change");
        broadcastAck(event, data);
      } else {
        logger.info(`relaying to server to broadcastAck ${event} with data: ${JSON.stringify(data)}`);
        // use server to relay the broadcast to all nodes
        this.socket.emit("relay-broadcastAck", { event, data });
      }
    });
  }

  public sendTo(replica: string, event: string, data: any) {
    logger.info(`sending ${event} to ${replica} with data: ${JSON.stringify(data)}`);
    if (replica === this.getIP()) {
      this.socket.emit(event, data);
    } else {
      this.socket.emit("relay-send-to", { replica, event, data });
    }
  }
}

const ioClient: IOClient = new IOClient();
export default ioClient;
