import { describe, expect, test } from 'bun:test';
import { Money } from '../money/money';
import { Wallet } from '../wallet/wallet';
import { WagerTransaction, WagerTransactionKind } from '../wagering/wager-transaction';
import { WagerTransactionProcessor } from '../wagering/wager-transaction-processor';

describe('Wallet concurrency', () => {
  test('two simultaneous bets on same wallet keep only one processed', async () => {
    const wallet = Wallet.open({
      id: 'wallet-concurrency',
      playerId: 'player-concurrency',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const first = WagerTransaction.create({
      id: 'tx-first',
      providerId: 'provider-a',
      externalTransactionId: 'ext-first',
      idempotencyKey: 'idem-first',
      payloadHash: 'hash-first',
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    const second = WagerTransaction.create({
      id: 'tx-second',
      providerId: 'provider-a',
      externalTransactionId: 'ext-second',
      idempotencyKey: 'idem-second',
      payloadHash: 'hash-second',
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    const [firstResult, secondResult] = await Promise.all([
      WagerTransactionProcessor.processSerialized(wallet, first),
      WagerTransactionProcessor.processSerialized(wallet, second),
    ]);

    const processedCount = [firstResult.transaction.status, secondResult.transaction.status].filter((status) => status === 'PROCESSED').length;
    const rejectedCount = [firstResult.transaction.status, secondResult.transaction.status].filter((status) => status === 'REJECTED').length;

    expect(processedCount).toBe(1);
    expect(rejectedCount).toBe(1);
    expect(wallet.balance.toString()).toBe('20.00');
  });
});
