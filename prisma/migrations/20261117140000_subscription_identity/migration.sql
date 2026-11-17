CREATE UNIQUE INDEX "subscriptions_identity" ON "subscriptions"("user_id", "channel", "address", "topic");
