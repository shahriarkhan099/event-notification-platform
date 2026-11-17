import { parseOrderPlaced } from "../../domain/order-event.js";
import { partialBatchFailure } from "./failures.js";

export type SqsRecord = {
  messageId: string;
  body: string;
  attributes?: { ApproximateReceiveCount?: string };
};

export type SqsEvent = { Records?: SqsRecord[] };

export async function handleDelivery(event: SqsEvent, deliver: (body: string) => Promise<void>) {
  const failed: string[] = [];
  for (const record of event.Records ?? []) {
    if (!parseOrderPlaced(record.body)) continue;
    try {
      await deliver(record.body);
    } catch {
      failed.push(record.messageId);
    }
  }
  return partialBatchFailure(failed);
}
