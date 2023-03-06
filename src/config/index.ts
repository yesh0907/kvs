import { config } from "dotenv";
config({ path: `.env.${process.env.NODE_ENV || "development"}.local` });

export const CREDENTIALS = process.env.CREDENTIALS === "true";
export const { NODE_ENV, LOG_FORMAT, LOG_DIR, ORIGIN, ADDRESS } = process.env;

// export ip and port as separate variables for convenience
const [ip, port] = ADDRESS.split(":");
export const IP = ip || "0.0.0.0";
export const PORT = parseInt(port, 10) || 8080;
