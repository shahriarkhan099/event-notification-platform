import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { createApp } from "../src/interfaces/http/app.js";

test("GET /subscriptions filters by topic", async () => {
  const app = createApp(async () => {
    throw new Error("not used");
  }, async (topic) => [
    {
      id: "sub_1",
      userId: "u1",
      channel: "email",
      address: "a@b.co",
      topic,
      createdAt: new Date("2026-01-01T00:00:00.000Z"),
    },
  ]);
  const server = app.listen(0);
  const { port } = server.address() as AddressInfo;
  try {
    const response = await fetch(`http://127.0.0.1:${port}/subscriptions?topic=order.placed`);
    const body = (await response.json()) as { topic: string }[];
    assert.equal(response.status, 200);
    assert.equal(body[0]?.topic, "order.placed");
  } finally {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
});
