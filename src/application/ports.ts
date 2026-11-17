import type { NewSubscription, SavedSubscription } from "../domain/subscription.js";
import type { OrderPlaced } from "../domain/order-event.js";

export const subscriptionsTopic = "notification.subscriptions";
export const ordersTopic = "orders.events";

export type SubscriptionStore = {
  create(input: NewSubscription): Promise<SavedSubscription>;
  findByTopic(topic: string): Promise<SavedSubscription[]>;
};

export type OutboundBatch = {
  topic: string;
  messages: { key: string; value: string }[];
};

export type MessageSender = {
  send(batch: OutboundBatch): Promise<void>;
};

export type OutboxPublisher = {
  publishPending(sender: MessageSender, limit?: number): Promise<number>;
};

export type ClaimStore = {
  claim(key: string): Promise<boolean>;
  release(key: string): Promise<void>;
};

export type DeliveryState = "missing" | "pending" | "sent";

export type DeliveryLog = {
  state(eventId: string, subscriptionId: string): Promise<DeliveryState>;
  claim(eventId: string, subscriptionId: string): Promise<boolean>;
  markSent(eventId: string, subscriptionId: string): Promise<void>;
  release(eventId: string, subscriptionId: string): Promise<void>;
};

export type Notifier = {
  send(subscription: SavedSubscription, event: OrderPlaced): Promise<void>;
};
