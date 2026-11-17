# Build

Notification side of the order pipeline. It does not place orders. It stores who wants `order.placed`, and a consumer of Kafka topic `orders.events` sends those events once.

## Subscription write

`POST /subscriptions` validates the body, then one Postgres transaction inserts `subscriptions` and an `outbox` row of type `subscription.created`. The publisher process is separate. It locks unpublished rows, sends them to `notification.subscriptions` with the subscription id as the key, then stamps `published_at`. A failed send rolls the stamp back.

## Delivery

`consume-main` reads `orders.events`. For each `order.placed` it loads subscriptions on that topic and sends once per `(event id, subscription id)`.

The claim is two layers:

- Redis `SET NX` is the fast path for a Kafka redelivery.
- `deliveries` has a unique `(event_id, subscription_id)`. That is the record that survives a Redis flush.

The send runs only after both claims succeed. If the send throws, both claims are released so the retry can try again. If the process dies after the claim and before the send, the row stays `pending` and a later retry will not send. That window is small. A sweeper for stale `pending` rows is the follow-up, not this pass.

Junk payloads are skipped. A throw from the notifier is not skipped. Kafka should retry that message.

## Layout

- `src/domain` is validation and parsing.
- `src/application` is `createSubscription` and `deliverOrder`. They take ports.
- `src/infrastructure` is Prisma, Redis, and Kafka.
- `src/interfaces/http` is Express.

## Local

```sh
createdb notifications_test
cp .env.example .env
npm install
npx prisma migrate deploy
npm test
npm run dev
npm run publish
npm run consume
```

Run Kafka from the order repo (`docker compose up` there) and point `KAFKA_BROKERS` at it. `docker compose up` in this repo starts Postgres on 54330 and Redis on 6379.

SNS is the production notifier. The consumer currently logs the send. The `Notifier` port is where SNS goes, without changing the claim logic.

## Not here yet

The Lambda handler and the SNS publish. The delivery use case does not know about either.
