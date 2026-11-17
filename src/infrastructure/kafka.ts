import { Kafka, type Consumer, type Producer } from "kafkajs";
import { ordersTopic, type MessageSender } from "../application/ports.js";
import { parseOrderPlaced, type OrderPlaced } from "../domain/order-event.js";

export type KafkaProducer = Pick<Producer, "send" | "connect" | "disconnect">;
export type OrderMessage = { value?: Buffer | null };
export type OrderConsumer = Pick<Consumer, "connect" | "subscribe" | "run" | "disconnect">;

export function createKafkaSender(brokers: string[], producer?: KafkaProducer) {
  const active =
    producer ??
    new Kafka({
      clientId: "notifications",
      brokers,
      connectionTimeout: 1000,
      retry: { retries: 0 },
    }).producer();
  const sender: MessageSender = {
    send(batch) {
      return active.send({ topic: batch.topic, messages: batch.messages }).then(() => undefined);
    },
  };
  return {
    sender,
    connect: () => active.connect(),
    disconnect: () => active.disconnect(),
  };
}

export function createKafkaConsumer(brokers: string[]) {
  const kafka = new Kafka({
    clientId: "notifications",
    brokers,
    connectionTimeout: 1000,
    retry: { retries: 0 },
  });
  return kafka.consumer({ groupId: "notification-delivery" });
}

export async function handleKafkaMessage(message: OrderMessage, onEvent: (event: OrderPlaced) => Promise<void>) {
  if (!message.value) return "empty";
  const event = parseOrderPlaced(message.value.toString("utf8"));
  if (!event) {
    console.log("skipped unreadable order event");
    return "skipped";
  }
  await onEvent(event);
  return "delivered";
}

export function resolveConsumer(brokers: string[], consumer?: OrderConsumer) {
  return consumer ?? createKafkaConsumer(brokers);
}

export async function consumeOrders(
  brokers: string[],
  onEvent: (event: OrderPlaced) => Promise<void>,
  consumer?: OrderConsumer,
) {
  const active = resolveConsumer(brokers, consumer);
  await active.connect();
  await active.subscribe({ topic: ordersTopic, fromBeginning: false });
  await active.run({
    eachMessage: async ({ message }) => {
      await handleKafkaMessage(message, onEvent);
    },
  });
  return active;
}
