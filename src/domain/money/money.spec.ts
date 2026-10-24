import { describe, expect, test } from 'bun:test';
import { Money } from './money';

describe('Money', () => {
  test('keeps exact decimal precision and serializes with two decimal places', () => {
    const result = Money.from({ amount: '9999999999999999.99', currency: 'brl' })
      .add(Money.from({ amount: '0.01', currency: 'BRL' }).negate());

    expect(result.toJSON()).toEqual({ amount: '9999999999999999.98', currency: 'BRL' });
  });

  test('rejects invalid, scientific, negative, over-scale, and out-of-range inputs', () => {
    for (const amount of ['', 'NaN', 'Infinity', '1e2', '-1.00', '+1.00', '1.', '1.001', '10000000000000000.00']) {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow();
    }
  });

  test('rejects arithmetic between different currencies', () => {
    expect(() =>
      Money.from({ amount: '1.00', currency: 'BRL' }).add(
        Money.from({ amount: '1.00', currency: 'USD' }),
      ),
    ).toThrow('Currency mismatch');
  });
});
