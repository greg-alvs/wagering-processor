import { createHash } from 'node:crypto';

export function canonicalizeJson(value: unknown): string {
  return JSON.stringify(sortObject(value));
}

export function hashCanonicalJson(value: unknown): string {
  return createHash('sha256').update(canonicalizeJson(value)).digest('hex');
}

function sortObject(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortObject);
  }

  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left.localeCompare(right));
    const sorted: Record<string, unknown> = {};

    for (const [key, item] of entries) {
      sorted[key] = sortObject(item);
    }

    return sorted;
  }

  return value;
}
