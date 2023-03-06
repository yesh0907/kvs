import ioServer from "@/io/server/index.server";
import ioClient from "@/io/client/index.client";
import { logger } from "@/utils/logger";
import { EventEmitter } from "events";

export const broadcast = (event: string, data: any) => {
  if (ioServer.isListening()) {
    ioServer.broadcast(event, data);
  } else if (ioClient.isConnected()) {
    ioClient.broadcast(event, data);
  } else {
    logger.warn("can't broadcast");
  }
};

export const broadcastAck = (event: string, data: any) => {
  if (ioServer.isListening()) {
    ioServer.broadcastAck(event, data);
  } else if (ioClient.isConnected()) {
    ioClient.broadcastAck(event, data);
  }else {
    logger.warn("can't use broadcastAck as client");
  }
}

export const sendTo = (replica: string, event: string, data: any) => {
  if (ioServer.isListening()) {
    ioServer.sendTo(replica, event, data);
  } else {
    logger.warn("can't use sendTo as client");
  }
}

export const IORunning = () => {
  return ioServer.isListening() || ioClient.isConnected();
}

// passing data from IO back to service
export const IOEventEmitter = new EventEmitter();
