import { CausalContext, CausalContexts } from "@/interfaces/causalContext.interface";

export const getLatestCausalContext = (causalContexts: CausalContexts): CausalContext => {
  let latestCausalContext: CausalContext = { timestamp: 0, causalMetadata: {} };
  Object.keys(causalContexts).forEach(replica => {
    const causalContext = causalContexts[replica];
    if (causalContext.timestamp > latestCausalContext.timestamp) {
      latestCausalContext = causalContext;
    }
  });
  return latestCausalContext;
};
