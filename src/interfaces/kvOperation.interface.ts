import { CausalMetadata } from "./causalContext.interface";

export interface KvOperation {
  type: "write" | "delete" | "read" | "readall";
  metadata: CausalMetadata;
  key?: string;
  val?: string;
}
