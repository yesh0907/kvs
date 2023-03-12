import { type Shard } from "@/interfaces/shard.interface";

const viewModel:{ view: Shard[], shard_index: number } = { view: [], shard_index: -1 }; // Uninitialized when array is empty

export default viewModel;
