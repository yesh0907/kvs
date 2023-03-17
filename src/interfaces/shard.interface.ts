export interface Shard {
  shard_id: number;
  nodes: string[];
}

export interface KeyDistribution {
  [new_shard_id: number]: {
    // number represents the old shard index
    [old_shard_id: number]: string[];
  };
}

export interface ShardIdKeyPairing {
  [shard_id: number]: string[];
}
