import express, { type NextFunction, type Request, type Response } from "express";
import type { NewSubscription, SavedSubscription } from "../../domain/subscription.js";
import { InvalidSubscription } from "../../domain/subscription.js";
import { assignRequestId } from "./request-id.js";

type Create = (input: NewSubscription) => Promise<SavedSubscription>;
type List = (topic: string) => Promise<SavedSubscription[]>;

type Health = () => Promise<boolean>;

export function createApp(create: Create, list: List = async () => [], health: Health = async () => true) {
  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: "16kb" }));
  app.use(assignRequestId);

  app.get("/health", async (_req, res) => {
    const ok = await health();
    res.status(ok ? 200 : 503).json({ ok });
  });

  app.get("/subscriptions", async (req, res, next) => {
    try {
      const topic = typeof req.query.topic === "string" ? req.query.topic : "";
      const rows = await list(topic);
      res.json(rows.map((subscription) => ({
        id: subscription.id,
        userId: subscription.userId,
        channel: subscription.channel,
        address: subscription.address,
        topic: subscription.topic,
        createdAt: subscription.createdAt.toISOString(),
      })));
    } catch (err) {
      next(err);
    }
  });

  app.post("/subscriptions", async (req, res, next) => {
    try {
      const subscription = await create(readBody(req.body));
      res.status(201).json({
        id: subscription.id,
        userId: subscription.userId,
        channel: subscription.channel,
        address: subscription.address,
        topic: subscription.topic,
        createdAt: subscription.createdAt.toISOString(),
      });
    } catch (err) {
      next(err);
    }
  });

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof InvalidSubscription) {
      res.status(400).json({ error: err.message });
      return;
    }
    console.error(err);
    res.status(500).json({ error: "failed to create subscription" });
  });

  return app;
}

function readBody(body: unknown): NewSubscription {
  const record = typeof body === "object" && body !== null ? (body as Record<string, unknown>) : {};
  return {
    userId: typeof record.userId === "string" ? record.userId : "",
    channel: typeof record.channel === "string" ? record.channel : "",
    address: typeof record.address === "string" ? record.address : "",
    topic: typeof record.topic === "string" ? record.topic : "",
  };
}
