export class InvalidSubscription extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidSubscription";
  }
}

export type NewSubscription = {
  userId: string;
  channel: string;
  address: string;
  topic: string;
};

export type SavedSubscription = NewSubscription & {
  id: string;
  createdAt: Date;
};

export function assertSubscription(input: NewSubscription): NewSubscription {
  const userId = input.userId.trim();
  const channel = input.channel.trim();
  const address = input.address.trim();
  const topic = input.topic.trim();
  if (!userId) throw new InvalidSubscription("userId is required");
  if (channel !== "email" && channel !== "sms") throw new InvalidSubscription("channel must be email or sms");
  if (!address) throw new InvalidSubscription("address is required");
  if (channel === "email" && !address.includes("@")) {
    throw new InvalidSubscription("email address must contain @");
  }
  if (!topic) throw new InvalidSubscription("topic is required");
  return { userId, channel, address, topic };
}
