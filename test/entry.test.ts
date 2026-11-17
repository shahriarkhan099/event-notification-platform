import assert from "node:assert/strict";
import { test } from "node:test";
import { prisma } from "../src/infrastructure/db.js";
import { handler } from "../src/interfaces/lambda/entry.js";

test("the lambda entry is a function", async () => {
  assert.equal(typeof handler, "function");
  await prisma.$disconnect();
});
