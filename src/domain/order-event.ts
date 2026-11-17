export type OrderPlaced = {
  id: string;
  type: "order.placed";
  orderId: string;
  sku: string;
  qty: number;
};

export function parseOrderPlaced(raw: string): OrderPlaced | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const record = value as Record<string, unknown>;
  if (record.type !== "order.placed") return null;
  if (typeof record.id !== "string" || !record.id) return null;
  if (typeof record.orderId !== "string" || !record.orderId) return null;
  if (typeof record.sku !== "string" || !record.sku) return null;
  if (typeof record.qty !== "number" || !Number.isInteger(record.qty)) return null;
  return {
    id: record.id,
    type: "order.placed",
    orderId: record.orderId,
    sku: record.sku,
    qty: record.qty,
  };
}
