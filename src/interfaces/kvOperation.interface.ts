export interface KvOperation {
  type: "write" | "delete" | "read" | "readall";
  metadata: [string, number][];
  key?: string;
  val?: string;
}
