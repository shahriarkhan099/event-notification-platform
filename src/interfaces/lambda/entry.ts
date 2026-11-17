import { SNSClient } from "@aws-sdk/client-sns";
import { deliverOrder } from "../../application/deliver-order.js";
import { readConfig } from "../../config.js";
import { parseOrderPlaced } from "../../domain/order-event.js";
import { prisma } from "../../infrastructure/db.js";
import { loggingNotifier, snsNotifier } from "../../infrastructure/notifier.js";
import { PrismaDeliveryLog } from "../../infrastructure/prisma-delivery-log.js";
import { PrismaSubscriptionStore } from "../../infrastructure/prisma-subscription-store.js";
import { redisClaimStore } from "../../infrastructure/redis-claim.js";
import { handleDelivery, type SqsEvent } from "./handler.js";

export async function handler(event: SqsEvent) {
  const { redisUrl, redisTtlSeconds } = readConfig();
  const subscriptions = new PrismaSubscriptionStore(prisma);
  const deliveries = new PrismaDeliveryLog(prisma);
  const claims = redisClaimStore(redisUrl, redisTtlSeconds);
  const topicArn = process.env.SNS_TOPIC_ARN;
  const notifier = topicArn ? snsNotifier(new SNSClient({}), topicArn) : loggingNotifier();
  try {
    return await handleDelivery(event, async (body) => {
      const parsed = parseOrderPlaced(body);
      if (!parsed) return;
      await deliverOrder(subscriptions, claims, deliveries, notifier, parsed);
    });
  } finally {
    await claims.quit();
    await prisma.$disconnect();
  }
}
