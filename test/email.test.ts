import assert from "node:assert/strict";
import { test } from "node:test";
import { assertSubscription } from "../src/domain/subscription.js";

test("an email address has to contain @", () => {
  assert.throws(
    () => assertSubscription({ userId: "u1", channel: "email", address: "not-an-email", topic: "order.placed" }),
    /@/,
  );
});
