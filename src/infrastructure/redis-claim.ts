import { Redis } from "ioredis";
import type { ClaimStore } from "../application/ports.js";

export function redisClaimStore(url: string, ttlSeconds = 86400): ClaimStore & { quit: () => Promise<unknown> } {
  const client = new Redis(url);
  return {
    async claim(key) {
      const result = await client.set(key, "1", "EX", ttlSeconds, "NX");
      return result === "OK";
    },
    async release(key) {
      await client.del(key);
    },
    quit: () => client.quit(),
  };
}
