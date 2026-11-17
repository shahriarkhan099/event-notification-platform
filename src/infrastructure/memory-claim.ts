import type { ClaimStore } from "../application/ports.js";

export function memoryClaimStore(): ClaimStore {
  const held = new Set<string>();
  return {
    async claim(key) {
      if (held.has(key)) return false;
      held.add(key);
      return true;
    },
    async release(key) {
      held.delete(key);
    },
  };
}
