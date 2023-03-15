export interface ProxyResponse {
  id: number;
  status: number;
  metadata: [string, number][];
  key?: string;
  val?: string;
  exists?: boolean;
}
