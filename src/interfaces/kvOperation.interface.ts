import { CausalContext } from "./causalContext.interface";

export interface KvOperation {
  type: "write" | "delete" | "read" | "readall";
  causalContext: CausalContext;
  key?: string;
  val?: string;
}
