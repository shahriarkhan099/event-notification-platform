import { Redis } from "ioredis";
import type { ClaimStore } from "../application/ports.js";

const daySeconds = 60 * 60 * 24;

export function redisClaimStore(url: string): ClaimStore & { quit: () => Promise<unknown> } {
  const client = new Redis(url);
  return {
    async claim(key) {
      const result = await client.set(key, "1", "EX", daySeconds, "NX");
      return result === "OK";
    },
    async release(key) {
      await client.del(key);
    },
    quit: () => client.quit(),
  };
}
