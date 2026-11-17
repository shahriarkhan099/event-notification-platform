import assert from "node:assert/strict";
import { test } from "node:test";
import { handleDelivery } from "../src/interfaces/lambda/handler.js";

const body = JSON.stringify({ id: "1", type: "order.placed", orderId: "ord_1", sku: "mug", qty: 1 });

test("a thrown delivery is reported as a batch failure", async () => {
  const result = await handleDelivery({ Records: [{ messageId: "m1", body }] }, async () => {
    throw new Error("sns down");
  });
  assert.deepEqual(result.batchItemFailures, [{ itemIdentifier: "m1" }]);
});

test("junk is skipped", async () => {
  let calls = 0;
  const result = await handleDelivery({ Records: [{ messageId: "m1", body: "nope" }] }, async () => {
    calls += 1;
  });
  assert.equal(calls, 0);
  assert.deepEqual(result.batchItemFailures, []);
});
