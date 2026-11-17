import assert from "node:assert/strict";
import { test } from "node:test";
import { parseOrderPlaced } from "../src/domain/order-event.js";

test("an orders.events payload parses", () => {
  const raw = JSON.stringify({
    id: "42",
    type: "order.placed",
    orderId: "ord_42",
    sku: "mug",
    qty: 1,
  });
  const event = parseOrderPlaced(raw);
  assert.equal(event?.id, "42");
  assert.equal(event?.orderId, "ord_42");
  assert.equal(event?.sku, "mug");
});
