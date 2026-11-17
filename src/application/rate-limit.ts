export class MemoryRateLimit {
  private readonly counts = new Map<string, number>();

  constructor(private readonly limit: number) {}

  async take(key: string) {
    const next = (this.counts.get(key) ?? 0) + 1;
    this.counts.set(key, next);
    return next <= this.limit;
  }
}
