import assert from "node:assert/strict";
import { test } from "node:test";
import { resources } from "../src/infrastructure/resources.js";

test("the queue names match the template", () => {
  assert.equal(resources.deliveryQueue, "notification-delivery");
  assert.equal(resources.deadLetterQueue, "notification-delivery-dlq");
  assert.equal(resources.snsTopic, "notification-fanout");
});
