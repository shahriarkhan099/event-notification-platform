import "./infrastructure/env.js";
import { createSubscription } from "./application/create-subscription.js";
import { readConfig } from "./config.js";
import { prisma } from "./infrastructure/db.js";
import { PrismaSubscriptionStore } from "./infrastructure/prisma-subscription-store.js";
import { createApp } from "./interfaces/http/app.js";

const { port } = readConfig();
const store = new PrismaSubscriptionStore(prisma);
const server = createApp((input) => createSubscription(store, input)).listen(port, () => {
  console.log(`notifications listening on ${port}`);
});

function shutdown() {
  server.close(() => {
    void prisma.$disconnect().finally(() => process.exit(0));
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
