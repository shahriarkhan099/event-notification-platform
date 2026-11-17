import "./infrastructure/env.js";
import { deliverOrder } from "./application/deliver-order.js";
import type { ClaimStore, DeliveryLog, Notifier, SubscriptionStore } from "./application/ports.js";
import type { OrderPlaced } from "./domain/order-event.js";
import { readConfig } from "./config.js";
import { prisma } from "./infrastructure/db.js";
import { consumeOrders } from "./infrastructure/kafka.js";
import { PrismaDeliveryLog } from "./infrastructure/prisma-delivery-log.js";
import { PrismaSubscriptionStore } from "./infrastructure/prisma-subscription-store.js";
import { loggingNotifier } from "./infrastructure/notifier.js";
import { redisClaimStore } from "./infrastructure/redis-claim.js";
import { launchIfMain } from "./runtime.js";

export type ConsumerDeps = {
  brokers: string[];
  subscriptions: SubscriptionStore;
  claims: ClaimStore & { quit: () => Promise<unknown> };
  deliveries: DeliveryLog;
  notifier: Notifier;
  consume: (
    brokers: string[],
    onEvent: (event: OrderPlaced) => Promise<void>,
  ) => Promise<{ disconnect: () => Promise<void> }>;
  disconnectDb: () => Promise<void>;
};

export function buildConsumer(env: NodeJS.ProcessEnv = process.env): ConsumerDeps {
  const config = readConfig(env);
  return {
    brokers: config.kafkaBrokers,
    subscriptions: new PrismaSubscriptionStore(prisma),
    deliveries: new PrismaDeliveryLog(prisma),
    claims: redisClaimStore(config.redisUrl, config.redisTtlSeconds),
    notifier: loggingNotifier(),
    consume: consumeOrders,
    disconnectDb: () => prisma.$disconnect(),
  };
}

export function resolveConsumerDeps(deps?: ConsumerDeps) {
  return deps ?? buildConsumer();
}

export async function startConsumer(deps: ConsumerDeps) {
  const consumer = await deps.consume(deps.brokers, (event) =>
    deliverOrder(deps.subscriptions, deps.claims, deps.deliveries, deps.notifier, event).then(() => undefined),
  );

  async function shutdown() {
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
    await consumer.disconnect();
    await deps.claims.quit();
    await deps.disconnectDb();
  }

  function onSignal() {
    return shutdown().finally(() => process.exit(0));
  }

  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  return { shutdown, onSignal };
}

export function runConsumer(deps?: ConsumerDeps) {
  return startConsumer(resolveConsumerDeps(deps));
}

void launchIfMain(process.argv[1], import.meta.url, runConsumer);
