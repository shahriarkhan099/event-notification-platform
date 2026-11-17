import { randomUUID } from "node:crypto";
import type { Prisma, PrismaClient } from "@prisma/client";
import type { SubscriptionStore } from "../application/ports.js";
import type { NewSubscription, SavedSubscription } from "../domain/subscription.js";

export function writeSubscriptionWithOutbox(
  tx: Prisma.TransactionClient,
  input: NewSubscription & { id: string },
) {
  return tx.subscription.create({ data: input }).then(async (subscription) => {
    await tx.outbox.create({
      data: {
        aggregateId: subscription.id,
        type: "subscription.created",
        payload: {
          userId: subscription.userId,
          channel: subscription.channel,
          address: subscription.address,
          topic: subscription.topic,
        },
      },
    });
    return subscription;
  });
}

export class PrismaSubscriptionStore implements SubscriptionStore {
  constructor(private readonly db: PrismaClient) {}

  async create(input: NewSubscription): Promise<SavedSubscription> {
    try {
      return await this.db.$transaction((tx) =>
        writeSubscriptionWithOutbox(tx, { ...input, id: randomUUID() }),
      );
    } catch (err) {
      if (!isUniqueViolation(err)) throw err;
      const existing = await this.db.subscription.findFirst({
        where: {
          userId: input.userId,
          channel: input.channel,
          address: input.address,
          topic: input.topic,
        },
      });
      if (!existing) throw err;
      return existing;
    }
  }

  findByTopic(topic: string) {
    return this.db.subscription.findMany({ where: { topic } });
  }
}

function isUniqueViolation(err: unknown) {
  return typeof err === "object" && err !== null && "code" in err && err.code === "P2002";
}
