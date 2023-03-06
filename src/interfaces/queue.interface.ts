export interface Queue {
  vc: [string, number][];
  action: 'write' | 'delete' | 'kvs';
  key?: string;
  val?: string;
  sender: string;
  kvs?: [string, string][];
}
