import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";
import type { Notifier } from "../application/ports.js";

export function loggingNotifier(log: (line: string) => void = console.log): Notifier {
  return {
    async send(subscription, event) {
      log(`send ${subscription.channel} ${subscription.address} for ${event.id}`);
    },
  };
}

export function snsNotifier(client: SNSClient, topicArn: string): Notifier {
  return {
    async send(subscription, event) {
      await client.send(
        new PublishCommand({
          TopicArn: topicArn,
          Message: JSON.stringify({
            subscriptionId: subscription.id,
            channel: subscription.channel,
            address: subscription.address,
            eventId: event.id,
            orderId: event.orderId,
          }),
          MessageAttributes: {
            channel: { DataType: "String", StringValue: subscription.channel },
          },
        }),
      );
    },
  };
}
