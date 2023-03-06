import request from "supertest";
import App from "@/app";
import ViewRoute from "@routes/view.route";

let viewRoute: ViewRoute;
let agent;

beforeAll(async () => {
  viewRoute = new ViewRoute();
  const app = new App([viewRoute]);
  agent = request.agent(app.getServer());
});

afterAll(async () => {
  await new Promise<void>(resolve => setTimeout(() => resolve(), 10000));
});

describe("Testing View", () => {
  describe("[GET]/[PUT] /kvs/admin/view", () => {
    it("returns 200 if view is returned correctly", async () => {
      const data = { view: ["127.0.0.1:9000", "127.0.0.1:3000"]};
      await agent.put(`${viewRoute.path}`).send(data).expect(200);
      return agent.get(`${viewRoute.path}`).expect(200, { view: ["127.0.0.1:9000", "127.0.0.1:3000"]});
    });
    it("testing updating put twice", async () => {
      const data = { view: ["127.0.0.1:9000", "127.0.0.1:3000"]};
      await agent.put(`${viewRoute.path}`).send(data).expect(200);
      await agent.put(`${viewRoute.path}`).send({ view: ["127.0.0.1:9000"]}).expect(200);
      return agent.get(`${viewRoute.path}`).expect(200, { view: ["127.0.0.1:9000"]});
    });
  });

  describe("[DELETE] /kvs/admin/view", () => {
    it("returns 200 if view was cleared", async () => {
      const data = { view: ["127.0.0.1:9000", "127.0.0.1:3000"]};
      await agent.put(`${viewRoute.path}`).send(data).expect(200);
      return agent.delete(`${viewRoute.path}`).expect(200);
    });
    it("returns 418 if uninitialized delete", async () => {
      await agent.get(`${viewRoute.path}`).expect(200, { view: []});
      return agent.delete(`${viewRoute.path}`).expect(418, {error: "uninitialized"});
    });
  });
});
