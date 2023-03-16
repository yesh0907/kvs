export interface CausalContext {
  timestamp: number;
  causalMetadata: CausalMetadata;
}

export interface CausalContexts {
  [replicaId: string]: CausalContext;
}

export interface CausalMetadata {
  // number is the timestamp
  [key: string]: number;
}
