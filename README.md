# notifications

Stores a subscription and an outbox row in one transaction. A consumer of `orders.events` sends each `order.placed` once per matching subscription.

```sh
createdb notifications_test
cp .env.example .env
npm install
npx prisma migrate deploy
npm test
```

`POST /subscriptions` takes `userId`, `channel`, `address`, and `topic`.
