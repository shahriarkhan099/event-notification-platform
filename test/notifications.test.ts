import assert from "node:assert/strict";
import { after, beforeEach, test } from "node:test";
import { createSubscription } from "../src/application/create-subscription.js";
import { subscriptionsTopic } from "../src/application/ports.js";
import { InvalidSubscription } from "../src/domain/subscription.js";
import { prisma } from "../src/infrastructure/db.js";
import { PrismaOutbox } from "../src/infrastructure/prisma-outbox.js";
import {
  PrismaSubscriptionStore,
  writeSubscriptionWithOutbox,
} from "../src/infrastructure/prisma-subscription-store.js";

const store = new PrismaSubscriptionStore(prisma);
const outbox = new PrismaOutbox(prisma);

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

test("publisher stamps the subscription row only after send succeeds", async () => {
  const subscription = await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  const sent: { topic: string; key?: string; value: string }[] = [];
  const count = await outbox.publishPending({
    async send(batch) {
      for (const message of batch.messages) {
        sent.push({ topic: batch.topic, key: message.key, value: message.value });
      }
    },
  });
  assert.equal(count, 1);
  assert.equal(sent[0]?.topic, subscriptionsTopic);
  assert.equal(sent[0]?.key, subscription.id);
  assert.ok((await prisma.outbox.findFirstOrThrow()).publishedAt);
});

test("a rejected publish leaves the outbox row unpublished", async () => {
  await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  await assert.rejects(
    () => outbox.publishPending({ async send() { throw new Error("broker down"); } }),
    /broker down/,
  );
  assert.equal((await prisma.outbox.findFirstOrThrow()).publishedAt, null);
});

test("an empty user id is rejected and writes nothing", async () => {
  await assert.rejects(
    () => createSubscription(store, { userId: "", channel: "email", address: "a@b.co", topic: "order.placed" }),
    InvalidSubscription,
  );
  assert.equal(await prisma.subscription.count(), 0);
});
