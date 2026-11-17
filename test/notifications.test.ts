import assert from "node:assert/strict";
import { after, beforeEach, test } from "node:test";
import { createSubscription } from "../src/application/create-subscription.js";
import { InvalidSubscription } from "../src/domain/subscription.js";
import { prisma } from "../src/infrastructure/db.js";
import {
  PrismaSubscriptionStore,
  writeSubscriptionWithOutbox,
} from "../src/infrastructure/prisma-subscription-store.js";

const store = new PrismaSubscriptionStore(prisma);

beforeEach(async () => {
  await prisma.delivery.deleteMany();
  await prisma.outbox.deleteMany();
  await prisma.subscription.deleteMany();
});

after(async () => {
  await prisma.$disconnect();
});
test("a failed subscription write leaves no row and no outbox record", async () => {
  await assert.rejects(
    prisma.$transaction(async (tx) => {
      await writeSubscriptionWithOutbox(tx, {
        id: "sub_rollback",
        userId: "u1",
        channel: "email",
        address: "a@b.co",
        topic: "order.placed",
      });
      throw new Error("nope");
    }),
    /nope/,
  );
  assert.equal(await prisma.subscription.count(), 0);
  assert.equal(await prisma.outbox.count(), 0);
});

test("an empty user id is rejected and writes nothing", async () => {
  await assert.rejects(
    () => createSubscription(store, { userId: "", channel: "email", address: "a@b.co", topic: "order.placed" }),
    InvalidSubscription,
  );
  assert.equal(await prisma.subscription.count(), 0);
});
