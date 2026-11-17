import assert from "node:assert/strict";
import { test } from "node:test";
import { assertSubscription, InvalidSubscription } from "../src/domain/subscription.js";

test("only email and sms are accepted", () => {
  assert.throws(
    () => assertSubscription({ userId: "u1", channel: "push", address: "device", topic: "order.placed" }),
    InvalidSubscription,
  );
  const sms = assertSubscription({ userId: "u1", channel: "sms", address: "+8801", topic: "order.placed" });
  assert.equal(sms.channel, "sms");
});
