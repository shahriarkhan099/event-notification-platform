import { randomUUID } from "node:crypto";
import type { PrismaClient } from "@prisma/client";
import type { DeliveryLog } from "../application/ports.js";

export class PrismaDeliveryLog implements DeliveryLog {
  constructor(private readonly db: PrismaClient) {}

  async state(eventId: string, subscriptionId: string) {
    const row = await this.db.delivery.findFirst({ where: { eventId, subscriptionId } });
    if (!row) return "missing" as const;
    return row.status === "sent" ? "sent" as const : "pending" as const;
  }

  async claim(eventId: string, subscriptionId: string) {
    try {
      await this.db.delivery.create({
        data: { id: randomUUID(), eventId, subscriptionId, status: "pending" },
      });
      return true;
    } catch (err) {
      if (isUniqueViolation(err)) return false;
      throw err;
    }
  }

  async markSent(eventId: string, subscriptionId: string) {
    await this.db.delivery.updateMany({
      where: { eventId, subscriptionId },
      data: { status: "sent", sentAt: new Date() },
    });
  }

  async release(eventId: string, subscriptionId: string) {
    await this.db.delivery.deleteMany({
      where: { eventId, subscriptionId, status: "pending" },
    });
  }
}

function isUniqueViolation(err: unknown) {
  return typeof err === "object" && err !== null && "code" in err && err.code === "P2002";
}
