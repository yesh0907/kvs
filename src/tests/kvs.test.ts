import request from "supertest";
import App from "@/app";
import KvsRoute from "@routes/kvs.route";
import ViewRoute from "@routes/view.route";
import { UpdateKVDto } from "@/dtos/kv.dto";

let kvsRoute: KvsRoute;
let viewRoute: ViewRoute;
let agent;

beforeAll(async () => {
  kvsRoute = new KvsRoute();
  viewRoute = new ViewRoute();
  const app = new App([kvsRoute, viewRoute]);
  agent = request.agent(app.getServer());
  await agent.put(viewRoute.path).send({ "view": ["0.0.0.0:8080"] });
});

afterAll(async () => {
  await new Promise<void>(resolve => setTimeout(() => resolve(), 10000));
});

describe("Testing KVS Main Instance", () => {
  let metadata = {};
  let clockCount = 1;
  describe("[PUT] /kvs/data/:key", () => {
    const key = "key1";
    const kvData: UpdateKVDto = { val: "value1" };

    it("creates new kv", async () => {
      const resp = await agent.put(`${kvsRoute.path}/${key}`).send({ "causal-metadata": metadata, ...kvData });
      expect(resp.status).toBe(201);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual([["0.0.0.0:8080", clockCount]]);
      metadata = resp.body["causal-metadata"];
      clockCount++;
    });
    it("updates existing kv", async () => {
      const resp = await agent.put(`${kvsRoute.path}/${key}`).send({ "causal-metadata": metadata, val: "value2" });
      expect(resp.status).toBe(200);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual([["0.0.0.0:8080", clockCount]]);
      metadata = resp.body["causal-metadata"];
    });
    it("returns 400 if key is missing", () => {
      return agent.put(`${kvsRoute.path}/`).send({ "causal-metadata": metadata, val: "value1" }).expect(400, { error: "bad request" });
    });
    it("returns 400 if val is missing", () => {
      return agent.put(`${kvsRoute.path}/${key}`).send({ "causal-metadata": metadata }).expect(400, { error: "bad request" });
    });
    it("returns 400 if val is not a string", () => {
      return agent.put(`${kvsRoute.path}/${key}`).send({ "causal-metadata": metadata, val: 1 }).expect(400, { error: "bad request" });
    });
    it("returns 400 if val > 8MB", () => {
      return agent
        .put(`${kvsRoute.path}/${key}`)
        .send({ "causal-metadata": metadata, val: "a".repeat(8 * 1024 * 1024 + 1) })
        .expect(400, { error: "val too large" });
    });
  });

  describe("[GET] /kvs/data/:key", () => {
    it("returns 404 if key is not found", async () => {
      const resp = await agent.get(`${kvsRoute.path}/key2`).send({ "causal-metadata": metadata });
      expect(resp.status).toBe(404);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual(metadata);
    });
    it("returns 200 if key is found", async () => {
      const resp = await agent.get(`${kvsRoute.path}/key1`).send({ "causal-metadata": metadata });
      expect(resp.status).toBe(200);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual(metadata);
    });
  });

  describe("[GET] /kvs/data", () => {
    beforeAll(async () => {
      const r1 = await agent.put(`${kvsRoute.path}/key1`).send({ "causal-metadata": metadata, val: "value1" });
      metadata = r1.body["causal-metadata"];
      const r2 = await agent.put(`${kvsRoute.path}/key4`).send({ "causal-metadata": metadata, val: "value4" });
      metadata = r2.body["causal-metadata"];
      const r3 = await agent.put(`${kvsRoute.path}/key3`).send({ "causal-metadata": metadata, val: "value3" });
      metadata = r3.body["causal-metadata"];
      clockCount += 4;
    });

    it("returns 200 and 3 keys", async () => {
      const keys = ["key1", "key3", "key4"];
      const resp = await agent.get(`${kvsRoute.path}`).send({ "causal-metadata": metadata });
      expect(resp.status).toBe(200);
      expect(resp.body.count).toEqual(keys.length);
      expect(resp.body.keys).toEqual(expect.arrayContaining(keys));
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual(metadata);
    });
  });

  describe("[DELETE] /kvs/data/:key", () => {
    it("returns 400 if key is missing", () => {
      return agent.delete(`${kvsRoute.path}/`).send({ "causal-metadata": metadata }).expect(400, { error: "bad request" });
    });
    it("returns 404 if key is not found", async () => {
      const resp = await agent.delete(`${kvsRoute.path}/key2`).send({ "causal-metadata": metadata })
      expect(resp.status).toBe(404);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual(metadata);
    });
    it("returns 200 if key is found", async () => {
      const resp = await agent.delete(`${kvsRoute.path}/key1`).send({ "causal-metadata": metadata });
      expect(resp.status).toBe(200);
      expect(resp.body).toHaveProperty("causal-metadata");
      expect(resp.body["causal-metadata"]).toStrictEqual([["0.0.0.0:8080", clockCount]]);
    });
  });

  describe("concurrent requests", () => {
    it("concurrent PUT and GET requests", async () => {
      const [write, read] = await Promise.all([agent.put(`${kvsRoute.path}/con`).send({ val: "value1" }), agent.get(`${kvsRoute.path}/con`)]);
      expect(write.status).toBe(201);
      expect(read.status).toBe(200);
      expect(read.body.val).toBe("value1");
    });

    it("concurrent PUT and DELETE requests", async () => {
      const [write, remove] = await Promise.all([agent.put(`${kvsRoute.path}/con1`).send({ val: "value1" }), agent.delete(`${kvsRoute.path}/con1`)]);
      expect(write.status).toBe(201);
      expect(remove.status).toBe(200);
    });

    it("concurrent DELETE and GET requests", async () => {
      const [remove, read] = await Promise.all([agent.delete(`${kvsRoute.path}/con`), agent.get(`${kvsRoute.path}/con`)]);
      expect(remove.status).toBe(200);
      expect(read.status).toBe(404);
    });
  });
});
