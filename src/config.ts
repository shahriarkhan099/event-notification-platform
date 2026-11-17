export type AppConfig = {
  kafkaBrokers: string[];
  redisUrl: string;
  port: number;
};

export function readConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const port = Number(env.PORT ?? 3002);
  if (!Number.isInteger(port) || port < 1) throw new Error("PORT must be a positive integer");
  const kafkaBrokers = (env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  const redisUrl = env.REDIS_URL ?? "redis://localhost:6379";
  return { kafkaBrokers, redisUrl, port };
}
