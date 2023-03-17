import { type Server, type Socket } from "socket.io";
import { logger } from "@utils/logger";
import kvsService from "@/services/kvs.service";
import viewService from "@/services/view.service";
import { onCausalGetKey, onCausalGetKvs, onCausalUpdateKey, onCausalUpdateKvs, onKvsDelete, onKvsWrite, onReplicationConverge, onShardProxyRequest, onShardProxyResponse, onViewChangeKill, onViewChangeUpdate } from "@io/handlers.io";
import { NODE_ENV } from "@/config";

// Handles socket.io server-side logic
class IOServer {
  private server: Server;
  private connections: { [address: string]: Socket };
  private listening: boolean;
  private existed: boolean;

  constructor() {
    this.connections = {};
    this.listening = false;
  }

  public setServer(server: Server) {
    this.server = server;
  }

  public getConnections() {
    return this.connections;
  }

  public isListening() {
    return this.listening;
  }

  // listen handling connections
  public listen(connections = []) {
    if (NODE_ENV === "test") {
      return;
    }
    this.connections = connections.reduce((acc, replica) => {
      acc[replica] = null;
      return acc;
    }, {});
    if (!this.existed) {
      this.server.on("connection", this.onConnection.bind(this));
    }
    this.existed = true;
    logger.info(`=================================`);
    logger.info(`🌐 IO Server handling connections`);
    logger.info(`=================================`);
    this.listening = true;
  }

  public shutdown() {
    this.server.disconnectSockets();
    this.listening = false;
    this.connections = {};
  }

  private async onConnection(socket: Socket) {
    await this.addConnection(socket);
    socket.on("relay-broadcast", this.relayBroadcast.bind(this, socket));
    socket.on("relay-broadcastAck", this.relayBroadcastAck.bind(this, socket));
    socket.on("relay-send-to", this.relaySendTo.bind(this, socket));
    socket.on("broadcast-ping", (ack) => ack());
    socket.on("disconnect", this.onDisconnect.bind(this, socket));
    // register event handlers
    this.handleEvents(socket);
  }

  private handleEvents(socket: Socket) {
    socket.on("viewchange:kill", onViewChangeKill);
    socket.on("viewchange:update", onViewChangeUpdate);
    socket.on("kvs:write", onKvsWrite);
    socket.on("kvs:delete", onKvsDelete);
    socket.on("causal:get-key", onCausalGetKey);
    socket.on("causal:update-key", onCausalUpdateKey);
    socket.on("causal:get-kvs", onCausalGetKvs);
    socket.on("causal:update-kvs", onCausalUpdateKvs);
    socket.on("shard:proxy-request", onShardProxyRequest);
    socket.on("shard:proxy-response", onShardProxyResponse);
    socket.on("replication:converge", onReplicationConverge);
  }

  private async addConnection(socket: Socket) {
    const replica = `${socket.handshake.address}:8080`;
    const isNewConnection = !this.connections.hasOwnProperty(replica);
    this.connections[replica] = socket;
    if (isNewConnection) {
      // only send kvs to new replicas
      await this.sendKvsToReplica(replica);
    }
    logger.info(`current connections: ${Object.keys(this.connections)}`);
  }

  private async sendKvsToReplica(replica: string) {
    const kvs = await kvsService.getCurrentKvs();
    this.sendTo(replica, "kvs:all", { kvs, shard_id: viewService.getShardIndex() });
  }

  private relayBroadcast(socket: Socket, data) {
    const { event, data: broadcastData } = data;
    logger.info(`relaying broadcast event ${event} with data: ${JSON.stringify(broadcastData)} from ${socket.handshake.address}`);
    // don't relay the broadcast to the sender
    socket.broadcast.emit(event, broadcastData);
  }

  private relayBroadcastAck(socket: Socket, data) {
    const { event, data: broadcastData } = data;
    logger.info(`relaying broadcastAck event ${event} with data: ${JSON.stringify(broadcastData)} from ${socket.handshake.address}`);
    // don't relay the broadcast to the sender
    socket.broadcast.timeout(10000).emit(event, broadcastData, async (err, responses) => {
      await viewService.checkReplicas(err, responses);
    });
  }

  private relaySendTo(socket: Socket, data) {
    const { replica, event, data: eventData } = data;
    logger.info(`relaying message from ${socket.handshake.address} to ${replica} with event ${event} and data: ${JSON.stringify(eventData)}`);
    const targetSocket = this.connections[replica];
    targetSocket.emit(event, eventData);
  }

  private onDisconnect(socket: Socket, reason: string) {
    const replica = socket.handshake.address;
    delete this.connections[replica];
    logger.info(`${replica} disconnected - ${reason}`);
  }

  public broadcast(event: string, data: any) {
    logger.info(`broadcasting ${event} with data: ${JSON.stringify(data)}`);
    this.server.emit(event, data);
  }

  public broadcastAck(event: string, data: any) {
    logger.info(`broadcasting with ack ${event} with data: ${JSON.stringify(data)}`);
    this.server.timeout(10000).emit(event, data, async (err, responses) => {
      await viewService.checkReplicas(err, responses);
    });
  }

  public sendTo(replica: string, event: string, data: any) {
    logger.info(`sending ${event} to ${replica} with data: ${JSON.stringify(data)}`);
    const socket = this.connections[replica];
    socket.emit(event, data);
  }
}

const ioServer: IOServer = new IOServer();
export default ioServer;
