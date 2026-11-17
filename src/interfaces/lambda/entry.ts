import { SNSClient } from "@aws-sdk/client-sns";
import { deliverOrder } from "../../application/deliver-order.js";
import type { ClaimStore, DeliveryLog, Notifier, SubscriptionStore } from "../../application/ports.js";
import { readConfig } from "../../config.js";
import { parseOrderPlaced } from "../../domain/order-event.js";
import { prisma } from "../../infrastructure/db.js";
import { loggingNotifier, snsNotifier } from "../../infrastructure/notifier.js";
import { PrismaDeliveryLog } from "../../infrastructure/prisma-delivery-log.js";
import { PrismaSubscriptionStore } from "../../infrastructure/prisma-subscription-store.js";
import { redisClaimStore } from "../../infrastructure/redis-claim.js";
import { handleDelivery, type SqsEvent } from "./handler.js";

export type DeliveryRuntime = {
  subscriptions: SubscriptionStore;
  deliveries: DeliveryLog;
  claims: ClaimStore & { quit: () => Promise<unknown> };
  notifier: Notifier;
  disconnect: () => Promise<void>;
};

export function runtimeFromEnv(env: NodeJS.ProcessEnv = process.env, claims = redisClaimStore(readConfig(env).redisUrl, readConfig(env).redisTtlSeconds)): DeliveryRuntime {
  const topicArn = env.SNS_TOPIC_ARN;
  return {
    subscriptions: new PrismaSubscriptionStore(prisma),
    deliveries: new PrismaDeliveryLog(prisma),
    claims,
    notifier: topicArn ? snsNotifier(new SNSClient({}), topicArn) : loggingNotifier(),
    disconnect: () => prisma.$disconnect(),
  };
}

export async function deliverParsed(body: string, runtime: DeliveryRuntime) {
  const parsed = parseOrderPlaced(body);
  if (!parsed) return;
  await deliverOrder(runtime.subscriptions, runtime.claims, runtime.deliveries, runtime.notifier, parsed);
}

export async function runDelivery(event: SqsEvent, runtime: DeliveryRuntime) {
  try {
    return await handleDelivery(event, (body) => deliverParsed(body, runtime));
  } finally {
    await runtime.claims.quit();
    await runtime.disconnect();
  }
}

export function handler(event: SqsEvent, runtime?: DeliveryRuntime) {
  return runDelivery(event, runtime ?? runtimeFromEnv());
}
