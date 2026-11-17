import { parseOrderPlaced } from "../../domain/order-event.js";

export type SqsRecord = {
  messageId: string;
  body: string;
  attributes?: { ApproximateReceiveCount?: string };
};

export type SqsEvent = { Records?: SqsRecord[] };

export async function handleDelivery(event: SqsEvent, deliver: (body: string) => Promise<void>) {
  const batchItemFailures: { itemIdentifier: string }[] = [];
  for (const record of event.Records ?? []) {
    if (!parseOrderPlaced(record.body)) continue;
    try {
      await deliver(record.body);
    } catch {
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
}
