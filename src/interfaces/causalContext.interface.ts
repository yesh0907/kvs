export interface CausalContext {
  timestamp: number;
  causalMetadata: CausalMetadata;
}

export interface CausalMetadata {
  // number is the timestamp
  [key: string]: number;
}
