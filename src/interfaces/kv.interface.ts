import { CausalContext, CausalMetadata } from "@/interfaces/causalContext.interface";

export interface KV {
  key: string;
  val: string;
}
export interface ValWithCausalContext{
  val: string;
  causalContext: CausalContext;
}

export interface KvStore {
  [key: string]: ValWithCausalContext;
}

export interface KvRequest {
  type: "write" | "delete" | "read" | "readall";
  metadata: CausalMetadata;
  key?: string;
  val?: string;
}
