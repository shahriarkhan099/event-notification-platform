import "./infrastructure/env.js";
import { deliverOrder } from "./application/deliver-order.js";
import { readConfig } from "./config.js";
import { prisma } from "./infrastructure/db.js";
import { consumeOrders } from "./infrastructure/kafka.js";
import { PrismaDeliveryLog } from "./infrastructure/prisma-delivery-log.js";
import { PrismaSubscriptionStore } from "./infrastructure/prisma-subscription-store.js";
import { loggingNotifier } from "./infrastructure/notifier.js";
import { redisClaimStore } from "./infrastructure/redis-claim.js";

const { kafkaBrokers, redisUrl, redisTtlSeconds } = readConfig();
const subscriptions = new PrismaSubscriptionStore(prisma);
const deliveries = new PrismaDeliveryLog(prisma);
const claims = redisClaimStore(redisUrl, redisTtlSeconds);

const notifier = loggingNotifier();
const consumer = await consumeOrders(kafkaBrokers, (event) =>
  deliverOrder(subscriptions, claims, deliveries, notifier, event).then(() => undefined),
);

function shutdown() {
  void consumer.disconnect().finally(() => {
    void claims.quit().finally(() => {
      void prisma.$disconnect().finally(() => process.exit(0));
    });
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
