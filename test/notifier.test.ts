import assert from "node:assert/strict";
import { test } from "node:test";
import { loggingNotifier, snsNotifier } from "../src/infrastructure/notifier.js";

const subscription = {
  id: "sub_1",
  userId: "u1",
  channel: "email",
  address: "a@b.co",
  topic: "order.placed",
  createdAt: new Date(),
};
const event = { id: "evt_1", type: "order.placed" as const, orderId: "ord_1", sku: "mug", qty: 1 };

test("the logging notifier records the channel and the event", async () => {
  const lines: string[] = [];
  await loggingNotifier((line) => lines.push(line)).send(subscription, event);
  assert.equal(lines[0], "send email a@b.co for evt_1");
});

test("the sns notifier publishes the subscription and the order", async () => {
  const sent: { TopicArn?: string; Message?: string }[] = [];
  const client = {
    async send(command: { input: { TopicArn?: string; Message?: string } }) {
      sent.push(command.input);
    },
  };
  await snsNotifier(client as never, "arn:aws:sns:us-east-1:000:notification-fanout").send(subscription, event);
  assert.equal(sent[0]?.TopicArn, "arn:aws:sns:us-east-1:000:notification-fanout");
  assert.match(sent[0]?.Message ?? "", /ord_1/);
});
