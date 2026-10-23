import { describe, expect, test } from 'bun:test';
import { IdempotencyService } from './idempotency-service';

describe('IdempotencyService', () => {
  test('same payload produces replay decision', () => {
    const payload = { playerId: 'p1', amount: '80.00', currency: 'BRL' };
    const hash = IdempotencyService.computePayloadHash(payload);

    expect(IdempotencyService.evaluate(payload, hash)).toEqual({ type: 'REPLAY', payloadHash: hash });
  });

  test('different payload produces conflict', () => {
    const original = { amount: '80.00', currency: 'BRL' };
    const changed = { amount: '90.00', currency: 'BRL' };

    expect(IdempotencyService.evaluate(changed, IdempotencyService.computePayloadHash(original))).toMatchObject({ type: 'CONFLICT' });
  });
});
