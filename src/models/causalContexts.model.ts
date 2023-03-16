import { ADDRESS } from "@/config";
import { CausalContexts } from "@/interfaces/causalContext.interface";

const causalContexts: CausalContexts = {
  [ADDRESS]: { timestamp: Date.now(), replicaId: ADDRESS },
}

export default causalContexts;
