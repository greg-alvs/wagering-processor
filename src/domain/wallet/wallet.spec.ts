import { describe, expect, test } from 'bun:test';
import { Money } from '../money/money';
import { Wallet } from './wallet';

describe('Money', () => {
  test('creates decimal strings with 2 decimal places', () => {
    const money = Money.from({ amount: '25.00', currency: 'BRL' });
    expect(money.toString()).toBe('25.00');
    expect(money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
  });

  test('rejects invalid amounts', () => {
    expect(() => Money.from({ amount: '-10.00', currency: 'BRL' })).toThrow();
    expect(() => Money.from({ amount: '10.001', currency: 'BRL' })).toThrow();
    expect(() => Money.from({ amount: '1e2', currency: 'BRL' })).toThrow();
  });

  test('enforces currency consistency', () => {
    const brl = Money.from({ amount: '10.00', currency: 'BRL' });
    const usd = Money.from({ amount: '10.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow();
  });
});

describe('Wallet', () => {
  test('opens with valid balance and increments version on mutation', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    expect(wallet.balance.toString()).toBe('100.00');
    expect(wallet.version).toBe(1);

    wallet.debit(Money.from({ amount: '80.00', currency: 'BRL' }));

    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);
  });

  test('rejects negative balance and invalid credit values', () => {
    const wallet = Wallet.open({
      id: 'wallet-2',
      playerId: 'player-2',
      initialBalance: Money.from({ amount: '10.00', currency: 'BRL' }),
    });

    expect(() => wallet.debit(Money.from({ amount: '20.00', currency: 'BRL' }))).toThrow('Insufficient funds');
    expect(() => wallet.credit(Money.from({ amount: '0.00', currency: 'BRL' }))).toThrow();
  });

  test('requires same currency for all wallet operations', () => {
    const wallet = Wallet.open({
      id: 'wallet-3',
      playerId: 'player-3',
      initialBalance: Money.from({ amount: '15.00', currency: 'BRL' }),
    });

    expect(() => wallet.credit(Money.from({ amount: '1.00', currency: 'USD' }))).toThrow();
  });
});
