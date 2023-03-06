import { ADDRESS } from "@/config";
import { broadcast, IORunning } from "@/io/index.io";
import kvsService from "@/services/kvs.service";
import clockService from "@/services/clock.service";
import viewService from "@/services/view.service";
import ioServer from "@/io/server/index.server";

class ReplicationService {
  begin() {
    setInterval(async () => {
      await this.tryDownReplicas();
      this.converge();
    }, 10000);
  }

  // try to get down replicas to connect (if they are up)
  public async tryDownReplicas() {
    const {view} = await viewService.getView();
    if (ioServer.isListening()) {
      const connections = Object.keys(ioServer.getConnections());
      const downReplicas = view.filter(replica => replica !== ADDRESS && !connections.includes(replica));
      if (downReplicas.length > 0) {
        await viewService.sendViewChange(downReplicas, view);
      }
    }
  }

  public converge() {
    if (IORunning()) {
      // broadcast converge
      const kvs = Array.from(kvsService.getCurrentKvs());
      const vc = Array.from(clockService.getVectorClock().getClock());
      broadcast("replication:converge", { sender: ADDRESS, kvs, vc });
    }
  }
}

export default ReplicationService;
