import { CausalMetadata } from "./causalContext.interface";

export interface ProxyResponse {
  id: number;
  status: number;
  metadata?: CausalMetadata;
  key?: string;
  val?: string;
  exists?: boolean;
}
