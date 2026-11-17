import assert from "node:assert/strict";
import { test } from "node:test";
import { parseOrderPlaced } from "../src/domain/order-event.js";

test("a quantity below 1 is not an order event", () => {
  const raw = JSON.stringify({ id: "1", type: "order.placed", orderId: "ord_1", sku: "mug", qty: 0 });
  assert.equal(parseOrderPlaced(raw), null);
});
