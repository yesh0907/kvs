import { CausalContext } from "./causalContext.interface";

export interface ProxyResponse {
  id: number;
  status: number;
  metadata?: CausalContext;
  key?: string;
  val?: string;
  exists?: boolean;
}
