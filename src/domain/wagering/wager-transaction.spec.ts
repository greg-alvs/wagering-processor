import { describe, expect, test } from 'bun:test';
import { Money } from '../money/money';
import { FailureCode, LedgerDirection, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';

describe('WagerTransaction', () => {
  test('requires reference for refund and rollback', () => {
    expect(() =>
      WagerTransaction.create({
        id: 'tx-1',
        providerId: 'prov-1',
        externalTransactionId: 'ext-1',
        idempotencyKey: 'key-1',
        payloadHash: 'hash-1',
        walletId: 'wallet-1',
        playerId: 'player-1',
        roundId: 'round-1',
        gameId: 'game-1',
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: '10.00', currency: 'BRL' }),
      }),
    ).toThrow();
  });

  test('moves to processed terminal state only if not already terminal', () => {
    const tx = WagerTransaction.create({
      id: 'tx-2',
      providerId: 'prov-1',
      externalTransactionId: 'ext-2',
      idempotencyKey: 'key-2',
      payloadHash: 'hash-2',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '50.00', currency: 'BRL' }),
    });

    tx.markProcessed(undefined, new Date());
    expect(tx.status).toBe(WagerTransactionStatus.Processed);

    expect(() => tx.reject(FailureCode.InvalidReference)).toThrow();
  });

  test('derives ledger direction for refund or rollback', () => {
    const refund = WagerTransaction.create({
      id: 'tx-3',
      providerId: 'prov-1',
      externalTransactionId: 'ext-3',
      idempotencyKey: 'key-3',
      payloadHash: 'hash-3',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Refund,
      money: Money.from({ amount: '20.00', currency: 'BRL' }),
      referenceExternalTransactionId: 'ref-1',
    });

    expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);

    const bet = WagerTransaction.create({
      id: 'tx-4',
      providerId: 'prov-1',
      externalTransactionId: 'ext-4',
      idempotencyKey: 'key-4',
      payloadHash: 'hash-4',
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
  });
});
