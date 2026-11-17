import { assertSubscription, type NewSubscription } from "../domain/subscription.js";
import type { SubscriptionStore } from "./ports.js";

export async function createSubscription(store: SubscriptionStore, input: NewSubscription) {
  return store.create(assertSubscription(input));
}
