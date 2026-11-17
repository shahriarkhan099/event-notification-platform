import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { PrismaClient } from "@prisma/client";
import { test } from "node:test";
import type { Redis } from "ioredis";
import { assertSubscription } from "../src/domain/subscription.js";
import { parseOrderPlaced } from "../src/domain/order-event.js";
import { deliverOrder } from "../src/application/deliver-order.js";
import { shouldDeadLetter } from "../src/application/dead-letter.js";
import { MemoryRateLimit } from "../src/application/rate-limit.js";
import { readConfig } from "../src/config.js";
import { memoryClaimStore } from "../src/infrastructure/memory-claim.js";
import { redisClaimStore } from "../src/infrastructure/redis-claim.js";
import { loggingNotifier } from "../src/infrastructure/notifier.js";
import { PrismaSubscriptionStore } from "../src/infrastructure/prisma-subscription-store.js";
import { PrismaDeliveryLog } from "../src/infrastructure/prisma-delivery-log.js";
import { PrismaOutbox } from "../src/infrastructure/prisma-outbox.js";
import { consumeOrders, createKafkaConsumer, createKafkaSender, handleKafkaMessage, resolveConsumer } from "../src/infrastructure/kafka.js";
import { ordersTopic, subscriptionsTopic } from "../src/application/ports.js";
import { clientStatus, createApp } from "../src/interfaces/http/app.js";
import { InvalidSubscription } from "../src/domain/subscription.js";
import { deliverParsed, handler, runDelivery, runtimeFromEnv } from "../src/interfaces/lambda/entry.js";
import { partialBatchFailure } from "../src/interfaces/lambda/failures.js";
import { handleDelivery } from "../src/interfaces/lambda/handler.js";
import { publishOutbox, runPublisher, selectPublisher } from "../src/publish-main.js";
import { resolveConsumerDeps, runConsumer, startConsumer } from "../src/consume-main.js";
import { launchIfMain } from "../src/runtime.js";
import { startNotifications } from "../src/main.js";

const event = { id: "evt_1", type: "order.placed" as const, orderId: "ord_1", sku: "mug", qty: 1 };
const subscription = {
  id: "sub_1",
  userId: "u1",
  channel: "email",
  address: "a@b.co",
  topic: "order.placed",
  createdAt: new Date("2026-01-01T00:00:00.000Z"),
};

test("subscription rules trim fields and reject the bad ones", () => {
  assert.throws(() => assertSubscription({ userId: "  ", channel: "email", address: "a@b.co", topic: "t" }), /userId/);
  assert.throws(() => assertSubscription({ userId: "u", channel: "push", address: "x", topic: "t" }), /channel/);
  assert.throws(() => assertSubscription({ userId: "u", channel: "sms", address: "  ", topic: "t" }), /address/);
  assert.throws(() => assertSubscription({ userId: "u", channel: "email", address: "a@b.co", topic: " " }), /topic/);
  const sms = assertSubscription({ userId: " u ", channel: "sms", address: " +8801 ", topic: " order.placed " });
  assert.equal(sms.address, "+8801");
  assert.equal(assertSubscription({ userId: "u", channel: "email", address: " a@b.co ", topic: "t" }).address, "a@b.co");
});

test("config rejects empty brokers, bad ports, and bad ttls", () => {
  assert.equal(readConfig({}).redisUrl, "redis://localhost:6379");
  assert.deepEqual(readConfig({ KAFKA_BROKERS: " a:1 , b:2 " }).kafkaBrokers, ["a:1", "b:2"]);
  assert.throws(() => readConfig({ KAFKA_BROKERS: "," }), /KAFKA_BROKERS/);
  assert.throws(() => readConfig({ PORT: "0" }), /PORT/);
  assert.throws(() => readConfig({ PORT: "2.5" }), /PORT/);
  assert.throws(() => readConfig({ REDIS_TTL_SECONDS: "0" }), /REDIS_TTL_SECONDS/);
  assert.throws(() => readConfig({ REDIS_TTL_SECONDS: "1.2" }), /REDIS_TTL_SECONDS/);
  assert.throws(() => readConfig({ OUTBOX_BATCH: "-3" }), /OUTBOX_BATCH/);
});

test("order payloads fail one rule at a time", () => {
  const bad = [
    "not-json",
    "null",
    "[]",
    "1",
    JSON.stringify({ type: "other", id: "1", orderId: "o", sku: "s", qty: 1 }),
    JSON.stringify({ type: "order.placed", orderId: "o", sku: "s", qty: 1 }),
    JSON.stringify({ type: "order.placed", id: "", orderId: "o", sku: "s", qty: 1 }),
    JSON.stringify({ type: "order.placed", id: "1", orderId: "", sku: "s", qty: 1 }),
    JSON.stringify({ type: "order.placed", id: "1", orderId: "o", sku: "", qty: 1 }),
    JSON.stringify({ type: "order.placed", id: "1", orderId: "o", sku: "s", qty: 0 }),
    JSON.stringify({ type: "order.placed", id: "1", orderId: "o", sku: "s", qty: 1.5 }),
    JSON.stringify({ type: "order.placed", id: 1, orderId: "o", sku: "s", qty: 1 }),
  ];
  for (const raw of bad) assert.equal(parseOrderPlaced(raw), null);
  assert.equal(parseOrderPlaced(JSON.stringify(event))?.orderId, "ord_1");
});

test("a lost claim is skipped and a lost row releases the fast claim", async () => {
  const lostRedis = await deliverOrder(subs([subscription]), claims(false), log(), notifier(), event);
  assert.deepEqual(lostRedis, { sent: [], skipped: ["sub_1"] });

  let released = false;
  const lostRow = await deliverOrder(
    subs([subscription]),
    { async claim() { return true; }, async release() { released = true; } },
    { async state() { return "missing" as const; }, async claim() { return false; }, async markSent() {}, async release() {} },
    notifier(),
    event,
  );
  assert.equal(released, true);
  assert.deepEqual(lostRow.skipped, ["sub_1"]);

  const pending = await deliverOrder(
    subs([subscription]),
    { async claim() { return true; }, async release() { released = true; } },
    { async state() { return "pending" as const; }, async claim() { return false; }, async markSent() {}, async release() {} },
    notifier(),
    event,
  );
  assert.deepEqual(pending.sent, []);
});

test("the rate limit allows the first send and blocks the next", async () => {
  const limit = new MemoryRateLimit(1);
  assert.equal(await limit.take("sub_1"), true);
  assert.equal(await limit.take("sub_1"), false);
  const claimsHeld = memoryClaimStore();
  assert.equal(await claimsHeld.claim("k"), true);
  assert.equal(await claimsHeld.claim("k"), false);
  await claimsHeld.release("k");
  assert.equal(await claimsHeld.claim("k"), true);
});

test("dead letter and batch helpers cover the remaining edges", async () => {
  assert.equal(shouldDeadLetter(Number.NaN), false);
  assert.equal(shouldDeadLetter(2, 1), true);
  assert.deepEqual(partialBatchFailure([]), { batchItemFailures: [] });
  const result = await handleDelivery({ Records: [{ messageId: "m", body: "{}" }] }, async () => {});
  assert.deepEqual(result.batchItemFailures, []);
  const missing = await handleDelivery({}, async () => {});
  assert.deepEqual(missing.batchItemFailures, []);
});

test("http subscriptions cover defaults, bad bodies, and failed checks", async () => {
  await withServer(async () => {
    throw new InvalidSubscription("nope");
  }, async () => [], async () => true, async (port) => {
    const health = await fetch(`http://127.0.0.1:${port}/health`);
    assert.equal(health.status, 200);
    const generated = await fetch(`http://127.0.0.1:${port}/health`, { headers: { "x-request-id": "" } });
    assert.match(generated.headers.get("x-request-id") ?? "", /^[0-9a-f-]{36}$/);
    const blank = await fetch(`http://127.0.0.1:${port}/health`, { headers: { "x-request-id": "  " } });
    assert.match(blank.headers.get("x-request-id") ?? "", /^[0-9a-f-]{36}$/);
  });

  await withServer(async (input) => {
    if (!input.userId) throw new InvalidSubscription("userId is required");
    if (input.userId === "boom") throw new Error("db down");
    if (input.userId === "text") throw "x";
    if (input.userId === "auth") throw { status: 401 };
    return { ...subscription, ...input, id: "sub_9" };
  }, async (topic) => [{ ...subscription, topic }], async () => false, async (port) => {
    assert.equal((await fetch(`http://127.0.0.1:${port}/health`)).status, 503);
    const listed = await fetch(`http://127.0.0.1:${port}/subscriptions?topic=a&topic=b`);
    const rows = await listed.json() as { topic: string }[];
    assert.equal(rows[0]?.topic, "");
    const created = await post(port, { userId: "u1", channel: "email", address: "a@b.co", topic: "order.placed" });
    assert.equal(created.status, 201);
    assert.equal((await post(port, null)).status, 400);
    assert.equal((await post(port, [])).status, 400);
    assert.equal((await postRaw(port, "{")).status, 400);
    assert.equal((await post(port, { userId: 1, channel: "email", address: "a@b.co", topic: "t" })).status, 400);
    const failed = await post(port, { userId: "boom", channel: "email", address: "a@b.co", topic: "t" });
    assert.equal(failed.status, 500);
    assert.equal(failed.body.error, "request failed");
    assert.equal((await post(port, { userId: "text", channel: "email", address: "a@b.co", topic: "t" })).status, 500);
    assert.equal((await post(port, { userId: "auth", channel: "email", address: "a@b.co", topic: "t" })).status, 500);
    assert.equal(clientStatus(null), undefined);
    assert.equal(clientStatus({}), undefined);
  });

  await withServer(async () => subscription, async () => {
    throw new Error("list down");
  }, async () => {
    throw new Error("db down");
  }, async (port) => {
    assert.equal((await fetch(`http://127.0.0.1:${port}/health`)).status, 500);
    assert.equal((await fetch(`http://127.0.0.1:${port}/subscriptions`)).status, 500);
  });

  await withServer(async () => subscription, undefined, undefined, async (port) => {
    const listed = await (await fetch(`http://127.0.0.1:${port}/subscriptions`)).json() as unknown[];
    assert.deepEqual(listed, []);
    assert.equal((await fetch(`http://127.0.0.1:${port}/health`)).status, 200);
  });
});

test("subscription and delivery stores cover conflicts and states", async () => {
  const created = await new PrismaSubscriptionStore(fakeSubs({})).create({
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  assert.equal(created.userId, "u1");
  const again = await new PrismaSubscriptionStore(fakeSubs({ throwValue: { code: "P2002" }, existing: subscription })).create({
    userId: "u1",
    channel: "email",
    address: "a@b.co",
    topic: "order.placed",
  });
  assert.equal(again.id, "sub_1");
  await assert.rejects(
    () => new PrismaSubscriptionStore(fakeSubs({ throwValue: { code: "P2002" }, existing: null })).create({
      userId: "u1", channel: "email", address: "a@b.co", topic: "order.placed",
    }),
  );
  await assert.rejects(
    () => new PrismaSubscriptionStore(fakeSubs({ throwValue: new Error("db down") })).create({
      userId: "u1", channel: "email", address: "a@b.co", topic: "order.placed",
    }),
    /db down/,
  );
  for (const throwValue of [null, "x", {}, { code: "NO" }]) {
    await assert.rejects(() => new PrismaSubscriptionStore(fakeSubs({ throwValue })).create({
      userId: "u1", channel: "email", address: "a@b.co", topic: "order.placed",
    }));
  }
  const found = await new PrismaSubscriptionStore(fakeSubs({ rows: [subscription] })).findByTopic("order.placed");
  assert.equal(found[0]?.id, "sub_1");

  const log = new PrismaDeliveryLog(fakeDeliveries({ row: null }));
  assert.equal(await log.state("e", "s"), "missing");
  assert.equal(await new PrismaDeliveryLog(fakeDeliveries({ row: { status: "sent" } })).state("e", "s"), "sent");
  assert.equal(await new PrismaDeliveryLog(fakeDeliveries({ row: { status: "pending" } })).state("e", "s"), "pending");
  assert.equal(await new PrismaDeliveryLog(fakeDeliveries({ row: { status: "other" } })).state("e", "s"), "pending");
  assert.equal(await new PrismaDeliveryLog(fakeDeliveries({})).claim("e", "s"), true);
  assert.equal(await new PrismaDeliveryLog(fakeDeliveries({ throwValue: { code: "P2002" } })).claim("e", "s"), false);
  await assert.rejects(() => new PrismaDeliveryLog(fakeDeliveries({ throwValue: new Error("db down") })).claim("e", "s"), /db down/);
  for (const throwValue of [null, "x", {}]) {
    await assert.rejects(() => new PrismaDeliveryLog(fakeDeliveries({ throwValue })).claim("e", "s"));
  }
  await new PrismaDeliveryLog(fakeDeliveries({})).markSent("e", "s");
  await new PrismaDeliveryLog(fakeDeliveries({})).release("e", "s");
});

test("the subscription outbox keeps object payloads and drops the rest", async () => {
  const sent: string[] = [];
  const sender = {
    async send(batch: { topic: string; messages: { value: string }[] }) {
      assert.equal(batch.topic, subscriptionsTopic);
      for (const message of batch.messages) sent.push(message.value);
    },
  };
  const outbox = new PrismaOutbox(fakeOutbox([
    { id: 3n, aggregate_id: "sub_1", type: "subscription.created", payload: { userId: "u1" } },
    { id: 4, aggregate_id: "sub_2", type: "subscription.created", payload: ["x"] },
    { id: "5", aggregate_id: "sub_3", type: "subscription.created", payload: "text" },
  ]) as unknown as PrismaClient);
  assert.equal(await outbox.publishPending(sender), 3);
  assert.match(sent[0] ?? "", /u1/);
  assert.equal(await new PrismaOutbox(fakeOutbox([]) as unknown as PrismaClient).publishPending(sender), 0);
  await assert.rejects(() => new PrismaOutbox(fakeOutbox([{ id: 1n, aggregate_id: "sub_4", type: "subscription.created", payload: {} }]) as unknown as PrismaClient).publishPending({
    async send() { throw new Error("down"); },
  }), /down/);
});

test("kafka helpers parse messages and a fake consumer receives them", async () => {
  assert.equal(await handleKafkaMessage({}, async () => {}), "empty");
  assert.equal(await handleKafkaMessage({ value: null }, async () => {}), "empty");
  const lines: string[] = [];
  const original = console.log;
  console.log = (line?: unknown) => lines.push(String(line));
  try {
    assert.equal(await handleKafkaMessage({ value: Buffer.from("") }, async () => {}), "skipped");
    assert.equal(await handleKafkaMessage({ value: Buffer.from("nope") }, async () => {}), "skipped");
    assert.match(lines[0] ?? "", /skipped/);
  } finally {
    console.log = original;
  }
  const seen: string[] = [];
  assert.equal(await handleKafkaMessage({ value: Buffer.from(JSON.stringify(event)) }, async (item) => { seen.push(item.orderId); }), "delivered");
  assert.deepEqual(seen, ["ord_1"]);

  const calls: string[] = [];
  const consumer = {
    async connect() { calls.push("connect"); },
    async subscribe(options: { topic: string; fromBeginning?: boolean }) { calls.push(options.topic); },
    async run(config: { eachMessage: (payload: { message: { value?: Buffer | null } }) => Promise<void> }) {
      await config.eachMessage({ message: {} });
      await config.eachMessage({ message: { value: Buffer.from("nope") } });
      await config.eachMessage({ message: { value: Buffer.from(JSON.stringify(event)) } });
    },
    async disconnect() { calls.push("disconnect"); },
  };
  const delivered: string[] = [];
  const active = await consumeOrders(["localhost:9092"], async (item) => { delivered.push(item.id); }, consumer as never);
  assert.equal(active, consumer);
  assert.equal(calls.includes(ordersTopic), true);
  assert.deepEqual(delivered, ["evt_1"]);
  const real = resolveConsumer(["localhost:9092"]);
  assert.notEqual(real, consumer);
  assert.equal(resolveConsumer(["localhost:9092"], consumer as never), consumer);
  createKafkaConsumer(["localhost:9092"]);
  const sender = createKafkaSender(["localhost:9092"], {
    async send() { return []; },
    async connect() {},
    async disconnect() {},
  } as never);
  await sender.connect();
  await sender.sender.send({ topic: ordersTopic, messages: [] });
  await sender.disconnect();
  createKafkaSender(["localhost:9092"]);
});

test("redis claims use the ttl and a lazy client can quit", async () => {
  const sets: unknown[][] = [];
  const client = {
    async set(...args: unknown[]) {
      sets.push(args);
      return sets.length === 1 ? "OK" : null;
    },
    async del() { return 1; },
    async quit() { return "OK"; },
  };
  const store = redisClaimStore("redis://127.0.0.1:6399", undefined, client as unknown as Redis);
  assert.equal(await store.claim("a"), true);
  assert.equal(await store.claim("b"), false);
  await store.release("a");
  assert.equal(sets[0]?.[3], 86400);
  const live = redisClaimStore("redis://127.0.0.1:1", 5);
  await live.quit();
});

test("the default logging notifier writes one line", async () => {
  const lines: string[] = [];
  const original = console.log;
  console.log = (line?: unknown) => lines.push(String(line));
  try {
    await loggingNotifier().send(subscription, event);
    assert.equal(lines[0], "send email a@b.co for evt_1");
  } finally {
    console.log = original;
  }
});

test("publisher and consumer processes disconnect even when send fails", async () => {
  const lines: string[] = [];
  await publishOutbox({
    async connect() {},
    async publish() { return 2; },
    async disconnect() {},
    log: (message) => lines.push(message),
  });
  assert.deepEqual(lines, ["published 2"]);
  let disconnected = false;
  await assert.rejects(() => publishOutbox({
    async connect() { throw new Error("no broker"); },
    async publish() { return 0; },
    async disconnect() { disconnected = true; },
  }), /no broker/);
  assert.equal(disconnected, true);

  const real = selectPublisher();
  assert.ok(real.config.outboxBatch >= 1);
  await runPublisher({
    config: real.config,
    kafka: { sender: { async send() {} }, async connect() {}, async disconnect() {} },
    outbox: { async publishPending() { return 1; } },
  } as never);

  let quit = false;
  let dbClosed = false;
  const deps = {
    brokers: ["localhost:9092"],
    subscriptions: subs([subscription]),
    claims: { async claim() { return true; }, async release() {}, async quit() { quit = true; } },
    deliveries: log(),
    notifier: notifier(),
    async consume(_brokers: string[], onEvent: (item: typeof event) => Promise<void>) {
      await onEvent(event);
      return { async disconnect() {} };
    },
    async disconnectDb() { dbClosed = true; },
  };
  const started = await startConsumer(deps);
  const resolved = resolveConsumerDeps(deps);
  assert.equal(resolved, deps);
  const built = resolveConsumerDeps();
  await built.claims.quit();
  await built.disconnectDb();
  const realExit = process.exit;
  let exitCode: number | undefined;
  process.exit = ((code?: number) => { exitCode = code; }) as typeof process.exit;
  try {
    await started.onSignal();
    assert.equal(quit && dbClosed && exitCode === 0, true);
    const again = await runConsumer(deps);
    await again.shutdown();
  } finally {
    process.exit = realExit;
  }
});

test("the lambda runtime delivers a parsed body and skips junk", async () => {
  let quit = false;
  let closed = false;
  const sent: string[] = [];
  const runtime = {
    subscriptions: subs([subscription]),
    deliveries: {
      async state() { return "missing" as const; },
      async claim() { return true; },
      async markSent() {},
      async release() {},
    },
    claims: { async claim() { return true; }, async release() {}, async quit() { quit = true; } },
    notifier: { async send(_subscription: typeof subscription, item: typeof event) { sent.push(item.id); } },
    async disconnect() { closed = true; },
  };
  const body = JSON.stringify(event);
  const result = await runDelivery({
    Records: [
      { messageId: "m1", body },
      { messageId: "m2", body: "nope" },
    ],
  }, runtime);
  assert.deepEqual(result.batchItemFailures, []);
  assert.deepEqual(sent, ["evt_1"]);
  assert.equal(quit && closed, true);
  await deliverParsed("nope", runtime);

  const logging = runtimeFromEnv({ ...process.env, SNS_TOPIC_ARN: "" }, { async claim() { return true; }, async release() {}, async quit() {} });
  assert.equal(typeof logging.disconnect, "function");
  const sns = runtimeFromEnv({ ...process.env, SNS_TOPIC_ARN: "arn:aws:sns:us-east-1:0:notification-fanout" }, {
    async claim() { return true; },
    async release() {},
    async quit() {},
  });
  assert.notEqual(sns.notifier, logging.notifier);
  const empty = await handler({ Records: [] }, runtime);
  assert.deepEqual(empty.batchItemFailures, []);
  const defaults = await handler({ Records: [] });
  assert.deepEqual(defaults.batchItemFailures, []);
});

test("launch and the http process shut down cleanly", async () => {
  const file = path.resolve("/tmp/notifications-main.ts");
  const url = pathToFileURL(file).href;
  assert.equal(await launchIfMain(undefined, url, async () => {}), false);
  assert.equal(await launchIfMain(file, url, async () => {}), true);
  const previousCode = process.exitCode;
  const original = console.error;
  console.error = () => {};
  try {
    assert.equal(await launchIfMain(file, url, async () => { throw new Error("boom"); }), false);
  } finally {
    console.error = original;
    process.exitCode = previousCode;
  }

  const previousPort = process.env.PORT;
  process.env.PORT = "34522";
  const exits: number[] = [];
  try {
    const configured = await startNotifications({ exit: (code) => exits.push(code) });
    assert.equal(configured.port, 34522);
    const health = await fetch(`http://127.0.0.1:${configured.port}/health`);
    assert.equal(health.status, 200);
    const created = await post(configured.port, { userId: "scenario", channel: "sms", address: "+8801", topic: "order.placed" });
    assert.equal(created.status, 201);
    const listed = await fetch(`http://127.0.0.1:${configured.port}/subscriptions?topic=order.placed`);
    assert.equal(listed.status, 200);
    await configured.shutdown();
    const ephemeral = await startNotifications({ port: 0, exit: (code) => exits.push(code) });
    await ephemeral.shutdown();
    assert.deepEqual(exits, [0, 0]);
    const realExit = process.exit;
    let code: number | undefined;
    process.exit = ((value?: number) => {
      code = value;
    }) as typeof process.exit;
    try {
      const started = await startNotifications({ port: 0 });
      await started.shutdown();
      assert.equal(code, 0);
    } finally {
      process.exit = realExit;
    }
  } finally {
    if (previousPort === undefined) delete process.env.PORT;
    else process.env.PORT = previousPort;
  }
});

function subs(rows: (typeof subscription)[]) {
  return {
    async create() { return rows[0] ?? subscription; },
    async findByTopic() { return rows; },
  };
}

function claims(won: boolean) {
  return { async claim() { return won; }, async release() {} };
}

function log() {
  return {
    async state() { return "missing" as const; },
    async claim() { return true; },
    async markSent() {},
    async release() {},
  };
}

function notifier() {
  return { async send() {} };
}

function fakeSubs(behavior: { throwValue?: unknown; existing?: typeof subscription | null; rows?: (typeof subscription)[] }) {
  return {
    subscription: {
      async findFirst() { return behavior.existing ?? null; },
      async findMany() { return behavior.rows ?? []; },
    },
    async $transaction(fn: (tx: unknown) => Promise<unknown>) {
      if ("throwValue" in behavior) throw behavior.throwValue;
      return fn({
        subscription: {
          async create({ data }: { data: typeof subscription }) {
            return { ...subscription, ...data };
          },
        },
        outbox: { async create() { return {}; } },
      });
    },
  } as unknown as PrismaClient;
}

function fakeDeliveries(behavior: { row?: { status: string } | null; throwValue?: unknown }) {
  return {
    delivery: {
      async findFirst() { return behavior.row ?? null; },
      async create() {
        if ("throwValue" in behavior) throw behavior.throwValue;
        return {};
      },
      async updateMany() { return { count: 1 }; },
      async deleteMany() { return { count: 1 }; },
    },
  } as unknown as PrismaClient;
}

function fakeOutbox(rows: { id: bigint | number | string; aggregate_id: string; type: string; payload: unknown }[]) {
  return {
    async $transaction(fn: (tx: unknown) => Promise<unknown>) {
      return fn({
        async $queryRaw() { return rows; },
        outbox: { async updateMany() { return { count: rows.length }; } },
      });
    },
  };
}

async function withServer(
  create: Parameters<typeof createApp>[0],
  list: Parameters<typeof createApp>[1],
  health: Parameters<typeof createApp>[2],
  run: (port: number) => Promise<void>,
) {
  const server = createApp(create, list, health).listen(0);
  const { port } = server.address() as AddressInfo;
  try {
    await run(port);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
}

async function postRaw(port: number, raw: string) {
  const response = await fetch(`http://127.0.0.1:${port}/subscriptions`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: raw,
  });
  return { status: response.status };
}

async function post(port: number, body: unknown) {
  const response = await fetch(`http://127.0.0.1:${port}/subscriptions`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) as { error?: string } : {} };
}
