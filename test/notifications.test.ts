import assert from "node:assert/strict";
import { after, beforeEach, test } from "node:test";
import type { AddressInfo } from "node:net";
import { createSubscription } from "../src/application/create-subscription.js";
import { deliverOrder } from "../src/application/deliver-order.js";
import { subscriptionsTopic } from "../src/application/ports.js";
import { InvalidSubscription } from "../src/domain/subscription.js";
import { prisma } from "../src/infrastructure/db.js";
import { memoryClaimStore } from "../src/infrastructure/memory-claim.js";
import { PrismaDeliveryLog } from "../src/infrastructure/prisma-delivery-log.js";
import { PrismaOutbox } from "../src/infrastructure/prisma-outbox.js";
import {
  PrismaSubscriptionStore,
  writeSubscriptionWithOutbox,
} from "../src/infrastructure/prisma-subscription-store.js";
import { createApp } from "../src/interfaces/http/app.js";
import { parseOrderPlaced } from "../src/domain/order-event.js";

const store = new PrismaSubscriptionStore(prisma);
const outbox = new PrismaOutbox(prisma);
const deliveries = new PrismaDeliveryLog(prisma);

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

test("one event is sent once per subscription, including a retry", async () => {
  const first = await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  const second = await createSubscription(store, {
    userId: "u2",
    channel: "sms",
    address: "+8801",
    topic: "order.placed",
  });
  await createSubscription(store, {
    userId: "u3",
    channel: "email",
    address: "other@b.co",
    topic: "something.else",
  });

  const claims = memoryClaimStore();
  const seen: string[] = [];
  const event = { id: "evt_1", type: "order.placed" as const, orderId: "ord_1", sku: "mug", qty: 1 };
  const notifier = {
    async send(subscription: { id: string }) {
      seen.push(subscription.id);
    },
  };

  const sent = await deliverOrder(store, claims, deliveries, notifier, event);
  const again = await deliverOrder(store, claims, deliveries, notifier, event);
  assert.deepEqual(sent.sent.sort(), [first.id, second.id].sort());
  assert.deepEqual(again.sent, []);
  assert.equal(seen.length, 2);
  assert.equal(await prisma.delivery.count({ where: { status: "sent" } }), 2);
});

test("a failed send can be retried", async () => {
  const subscription = await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  const claims = memoryClaimStore();
  const event = { id: "evt_2", type: "order.placed" as const, orderId: "ord_2", sku: "mug", qty: 1 };
  let attempts = 0;
  const notifier = {
    async send() {
      attempts += 1;
      if (attempts === 1) throw new Error("sns down");
    },
  };
  await assert.rejects(
    () => deliverOrder(store, claims, deliveries, notifier, event),
    /sns down/,
  );
  const sent = await deliverOrder(store, claims, deliveries, notifier, event);
  assert.deepEqual(sent.sent, [subscription.id]);
  assert.equal(attempts, 2);
});

test("junk payloads are ignored and a real order event parses", () => {
  assert.equal(parseOrderPlaced("not-json"), null);
  assert.equal(parseOrderPlaced(JSON.stringify({ type: "other" })), null);
  const event = parseOrderPlaced(JSON.stringify({
    id: "1",
    type: "order.placed",
    orderId: "ord_1",
    sku: "mug",
    qty: 1,
  }));
  assert.equal(event?.orderId, "ord_1");
});

test("the same user, channel, address, and topic returns the original subscription", async () => {
  const first = await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  const second = await createSubscription(store, {
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  assert.equal(second.id, first.id);
  assert.equal(await prisma.subscription.count(), 1);
  assert.equal(await prisma.outbox.count(), 1);
});

test("POST /subscriptions rejects an empty address", async () => {
  const app = createApp((input) => createSubscription(store, input));
  const server = app.listen(0);
  const { port } = server.address() as AddressInfo;
  try {
    const response = await fetch(`http://127.0.0.1:${port}/subscriptions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ userId: "u1", channel: "email", address: "  ", topic: "order.placed" }),
    });
    assert.equal(response.status, 400);
    await assert.rejects(
      () => createSubscription(store, { userId: "", channel: "email", address: "a@b.co", topic: "order.placed" }),
      InvalidSubscription,
    );
  } finally {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
});
