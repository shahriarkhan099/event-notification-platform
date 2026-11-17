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
    return this.db.$transaction((tx) =>
      writeSubscriptionWithOutbox(tx, { ...input, id: randomUUID() }),
    );
  }

  findByTopic(topic: string) {
    return this.db.subscription.findMany({ where: { topic } });
  }
}
