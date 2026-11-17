export const resources = {
  ordersTopic: "orders.events",
  subscriptionsTopic: "notification.subscriptions",
  deliveryQueue: "notification-delivery",
  deadLetterQueue: "notification-delivery-dlq",
  snsTopic: "notification-fanout",
} as const;
