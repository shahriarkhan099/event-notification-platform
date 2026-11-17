import assert from "node:assert/strict";
import { test } from "node:test";
import { shouldDeadLetter } from "../src/application/dead-letter.js";
import { handleDelivery } from "../src/interfaces/lambda/handler.js";

test("the sixth receive is the dead-letter point", () => {
  assert.equal(shouldDeadLetter(5), false);
  assert.equal(shouldDeadLetter(6), true);
});

test("a record past five receives is failed without delivery", async () => {
  const body = JSON.stringify({ id: "1", type: "order.placed", orderId: "ord_1", sku: "mug", qty: 1 });
  let calls = 0;
  const result = await handleDelivery(
    { Records: [{ messageId: "m1", body, attributes: { ApproximateReceiveCount: "6" } }] },
    async () => {
      calls += 1;
    },
  );
  assert.equal(calls, 0);
  assert.deepEqual(result.batchItemFailures, [{ itemIdentifier: "m1" }]);
});
