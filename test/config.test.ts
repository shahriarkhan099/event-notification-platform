import assert from "node:assert/strict";
import { test } from "node:test";
import { readConfig } from "../src/config.js";

test("the redis claim ttl defaults to one day", () => {
  assert.equal(readConfig({}).redisTtlSeconds, 86400);
});

test("the outbox batch defaults to 20", () => {
  assert.equal(readConfig({}).outboxBatch, 20);
});
