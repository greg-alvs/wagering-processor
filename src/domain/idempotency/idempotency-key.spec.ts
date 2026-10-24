import { describe, expect, test } from 'bun:test';
import { canonicalizeJson } from './canonical-payload';
import { IdempotencyKeyRecord } from './idempotency-key';

describe('Idempotency', () => {
  test('canonicalizes payload deterministically', () => {
    const payloadA = { amount: '80.00', currency: 'BRL', roundId: 'r1', gameId: 'g1' };
    const payloadB = { gameId: 'g1', roundId: 'r1', currency: 'BRL', amount: '80.00' };

    expect(canonicalizeJson(payloadA)).toBe(canonicalizeJson(payloadB));
  });

  test('creates a valid idempotency record', () => {
    const record = IdempotencyKeyRecord.create({
      id: 'idempotency-1',
      key: 'player-1:BET:123',
      payloadHash: 'hash-1',
      responseHash: 'response-1',
    });

    expect(record.key).toBe('player-1:BET:123');
    expect(record.payloadHash).toBe('hash-1');
  });
});
