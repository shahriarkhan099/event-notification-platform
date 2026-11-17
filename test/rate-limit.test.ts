import assert from "node:assert/strict";
import { test } from "node:test";
import { deliverOrder } from "../src/application/deliver-order.js";
import { MemoryRateLimit } from "../src/application/rate-limit.js";

test("a subscription past the limit is not sent", async () => {
  const subscription = {
    id: "sub_1",
    userId: "u1",
    channel: "email" as const,
    address: "a@b.co",
    topic: "order.placed",
    createdAt: new Date(),
  };
  let sends = 0;
  const sent = await deliverOrder(
    { async create() { return subscription; }, async findByTopic() { return [subscription]; } },
    { async claim() { return true; }, async release() {} },
    { async claim() { return true; }, async markSent() {}, async release() {} },
    { async send() { sends += 1; } },
    { id: "evt_1", type: "order.placed", orderId: "ord_1", sku: "mug", qty: 1 },
    new MemoryRateLimit(0),
  );
  assert.deepEqual(sent, []);
  assert.equal(sends, 0);
});
