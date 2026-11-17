export type AppConfig = {
  kafkaBrokers: string[];
  redisUrl: string;
  port: number;
  redisTtlSeconds: number;
  outboxBatch: number;
};

export function readConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const port = Number(env.PORT ?? 3002);
  if (!Number.isInteger(port) || port < 1) throw new Error("PORT must be a positive integer");
  const kafkaBrokers = (env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  const redisUrl = env.REDIS_URL ?? "redis://localhost:6379";
  const redisTtlSeconds = Number(env.REDIS_TTL_SECONDS ?? 86400);
  if (!Number.isInteger(redisTtlSeconds) || redisTtlSeconds < 1) {
    throw new Error("REDIS_TTL_SECONDS must be a positive integer");
  }
  const outboxBatch = Number(env.OUTBOX_BATCH ?? 20);
  if (!Number.isInteger(outboxBatch) || outboxBatch < 1) {
    throw new Error("OUTBOX_BATCH must be a positive integer");
  }
  return { kafkaBrokers, redisUrl, port, redisTtlSeconds, outboxBatch };
}
