import type { OrderPlaced } from "../domain/order-event.js";
import type { ClaimStore, DeliveryLog, Notifier, SubscriptionStore } from "./ports.js";
import type { MemoryRateLimit } from "./rate-limit.js";

export async function deliverOrder(
  subscriptions: SubscriptionStore,
  claims: ClaimStore,
  deliveries: DeliveryLog,
  notifier: Notifier,
  event: OrderPlaced,
  limit?: MemoryRateLimit,
) {
  const matching = await subscriptions.findByTopic("order.placed");
  const sent: string[] = [];
  for (const subscription of matching) {
    if (limit && !(await limit.take(subscription.id))) continue;
    if ((await deliveries.state(event.id, subscription.id)) === "sent") continue;
    const key = `delivery:${event.id}:${subscription.id}`;
    const cached = await claims.claim(key);
    if (!cached) continue;
    const owned = await deliveries.claim(event.id, subscription.id);
    if (!owned) continue;
    try {
      await notifier.send(subscription, event);
      await deliveries.markSent(event.id, subscription.id);
      sent.push(subscription.id);
    } catch (err) {
      await deliveries.release(event.id, subscription.id);
      await claims.release(key);
      throw err;
    }
  }
  return sent;
}
