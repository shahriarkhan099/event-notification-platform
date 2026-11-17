import { Kafka } from "kafkajs";
import { ordersTopic, type MessageSender } from "../application/ports.js";
import { parseOrderPlaced, type OrderPlaced } from "../domain/order-event.js";

export function createKafkaSender(brokers: string[]) {
  const kafka = new Kafka({ clientId: "notifications", brokers });
  const producer = kafka.producer();
  const sender: MessageSender = {
    send(batch) {
      return producer.send({ topic: batch.topic, messages: batch.messages }).then(() => undefined);
    },
  };
  return {
    sender,
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
  };
}

export async function consumeOrders(
  brokers: string[],
  onEvent: (event: OrderPlaced) => Promise<void>,
) {
  const kafka = new Kafka({ clientId: "notifications", brokers });
  const consumer = kafka.consumer({ groupId: "notification-delivery" });
  await consumer.connect();
  await consumer.subscribe({ topic: ordersTopic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const event = parseOrderPlaced(message.value.toString("utf8"));
      if (!event) {
        console.log("skipped unreadable order event");
        return;
      }
      await onEvent(event);
    },
  });
  return consumer;
}
