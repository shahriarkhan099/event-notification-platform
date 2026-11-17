import assert from "node:assert/strict";
import { test } from "node:test";
import { deliverOrder } from "../src/application/deliver-order.js";

test("an already sent delivery is skipped", async () => {
  const subscription = {
    id: "sub_1",
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
    createdAt: new Date(),
  };
  let claims = 0;
  const sent = await deliverOrder(
    { async create() { return subscription; }, async findByTopic() { return [subscription]; } },
    { async claim() { claims += 1; return true; }, async release() {} },
    {
      async state() { return "sent" as const; },
      async claim() { throw new Error("should not claim"); },
      async markSent() {},
      async release() {},
    },
    { async send() { throw new Error("should not send"); } },
    { id: "evt_1", type: "order.placed", orderId: "ord_1", sku: "mug", qty: 1 },
  );
  assert.deepEqual(sent, []);
  assert.equal(claims, 0);
});
