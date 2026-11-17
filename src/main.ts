import "./infrastructure/env.js";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { createSubscription } from "./application/create-subscription.js";
import { readConfig } from "./config.js";
import { prisma } from "./infrastructure/db.js";
import { PrismaSubscriptionStore } from "./infrastructure/prisma-subscription-store.js";
import { createApp } from "./interfaces/http/app.js";
import { launchIfMain } from "./runtime.js";

export async function startNotifications(options: { port?: number; exit?: (code: number) => void } = {}) {
  const port = options.port ?? readConfig().port;
  const exit = options.exit ?? process.exit;
  const store = new PrismaSubscriptionStore(prisma);
  const app = createApp(
    (input) => createSubscription(store, input),
    (topic) => store.findByTopic(topic),
    async () => {
      await prisma.$queryRaw`SELECT 1`;
      return true;
    },
  );
  const server = await new Promise<Server>((resolve) => {
    const listening = app.listen(port, () => {
      const bound = listening.address() as AddressInfo;
      console.log(`notifications listening on ${bound.port}`);
      resolve(listening);
    });
  });
  const actualPort = (server.address() as AddressInfo).port;

  function shutdown() {
    process.off("SIGINT", shutdown);
    process.off("SIGTERM", shutdown);
    return new Promise<void>((resolve) => {
      server.close(() => {
        void prisma.$disconnect().finally(() => {
          exit(0);
          resolve();
        });
      });
    });
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  return { port: actualPort, shutdown };
}

void launchIfMain(process.argv[1], import.meta.url, startNotifications);
