import { describe, expect, test } from 'bun:test';
import { Money } from '../money/money';
import { Wallet } from '../wallet/wallet';
import { LedgerDirection } from '../ledger/ledger-entry';
import { FailureCode, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';
import { WagerTransactionProcessor } from './wager-transaction-processor';

describe('WagerTransactionProcessor', () => {
  test('bet is processed when funds are sufficient', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    const transaction = WagerTransaction.create({
      id: 'tx-bet',
      providerId: 'provider-1',
      externalTransactionId: 'ext-bet',
      idempotencyKey: 'idem-bet',
      payloadHash: 'hash-bet',
      walletId: wallet.id,
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    const result = WagerTransactionProcessor.process(wallet, transaction);

    expect(result.wallet.balance.toString()).toBe('20.00');
    expect(result.transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(result.ledgerEntry?.direction).toBe(LedgerDirection.Debit);
  });

  test('bet is rejected for insufficient funds', () => {
    const wallet = Wallet.open({
      id: 'wallet-2',
      playerId: 'player-2',
      initialBalance: Money.from({ amount: '20.00', currency: 'BRL' }),
    });

    const transaction = WagerTransaction.create({
      id: 'tx-bet-2',
      providerId: 'provider-2',
      externalTransactionId: 'ext-bet-2',
      idempotencyKey: 'idem-bet-2',
      payloadHash: 'hash-bet-2',
      walletId: wallet.id,
      playerId: 'player-2',
      roundId: 'round-2',
      gameId: 'game-2',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });

    const result = WagerTransactionProcessor.process(wallet, transaction);

    expect(result.transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(result.transaction.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(result.wallet.balance.toString()).toBe('20.00');
  });

  test('refund can restore balance when reference is valid', () => {
    const wallet = Wallet.open({
      id: 'wallet-3',
      playerId: 'player-3',
      initialBalance: Money.from({ amount: '40.00', currency: 'BRL' }),
    });

    const bet = WagerTransaction.create({
      id: 'tx-bet-ref',
      providerId: 'provider-3',
      externalTransactionId: 'ext-bet-ref',
      idempotencyKey: 'idem-bet-ref',
      payloadHash: 'hash-bet-ref',
      walletId: wallet.id,
      playerId: 'player-3',
      roundId: 'round-3',
      gameId: 'game-3',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '40.00', currency: 'BRL' }),
    });

    const betResult = WagerTransactionProcessor.process(wallet, bet);
    expect(betResult.transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(betResult.wallet.balance.toString()).toBe('0.00');

    const refund = WagerTransaction.create({
      id: 'tx-refund',
      providerId: 'provider-3',
      externalTransactionId: 'ext-refund',
      idempotencyKey: 'idem-refund',
      payloadHash: 'hash-refund',
      walletId: wallet.id,
      playerId: 'player-3',
      roundId: 'round-3',
      gameId: 'game-3',
      kind: WagerTransactionKind.Refund,
      money: Money.from({ amount: '40.00', currency: 'BRL' }),
      referenceExternalTransactionId: 'ext-bet-ref',
    });

    const result = WagerTransactionProcessor.process(wallet, refund, bet);

    expect(result.transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(result.wallet.balance.toString()).toBe('40.00');
    expect(result.ledgerEntry?.direction).toBe(LedgerDirection.Credit);
  });

  test('rejects a rollback that would make the wallet balance negative', () => {
    const wallet = Wallet.open({
      id: 'wallet-rollback-insufficient',
      playerId: 'player-rollback-insufficient',
      initialBalance: Money.zero('BRL'),
    });
    const win = WagerTransaction.create({
      id: 'tx-win-rollback',
      providerId: 'provider-rollback',
      externalTransactionId: 'ext-win-rollback',
      idempotencyKey: 'idem-win-rollback',
      payloadHash: 'hash-win-rollback',
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-rollback',
      gameId: 'game-rollback',
      kind: WagerTransactionKind.Win,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
    });
    win.markProcessed(undefined, new Date());

    const rollback = WagerTransaction.create({
      id: 'tx-rollback-win',
      providerId: 'provider-rollback',
      externalTransactionId: 'ext-rollback-win',
      idempotencyKey: 'idem-rollback-win',
      payloadHash: 'hash-rollback-win',
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-rollback',
      gameId: 'game-rollback',
      kind: WagerTransactionKind.Rollback,
      money: Money.from({ amount: '80.00', currency: 'BRL' }),
      referenceExternalTransactionId: win.externalTransactionId,
    });

    const result = WagerTransactionProcessor.process(wallet, rollback, win);

    expect(result.transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(result.transaction.failureCode).toBe(FailureCode.ReversalInsufficientFunds);
    expect(result.wallet.balance.toString()).toBe('0.00');
    expect(result.ledgerEntry).toBeUndefined();
  });

  test('rejects a transaction whose currency does not match the wallet', () => {
    const wallet = Wallet.open({
      id: 'wallet-currency-mismatch',
      playerId: 'player-currency-mismatch',
      initialBalance: Money.from({ amount: '10.00', currency: 'BRL' }),
    });
    const transaction = WagerTransaction.create({
      id: 'tx-currency-mismatch',
      providerId: 'provider-currency-mismatch',
      externalTransactionId: 'ext-currency-mismatch',
      idempotencyKey: 'idem-currency-mismatch',
      payloadHash: 'hash-currency-mismatch',
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-currency-mismatch',
      gameId: 'game-currency-mismatch',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '1.00', currency: 'USD' }),
    });

    const result = WagerTransactionProcessor.process(wallet, transaction);

    expect(result.transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(result.transaction.failureCode).toBe(FailureCode.CurrencyMismatch);
    expect(result.wallet.balance.toString()).toBe('10.00');
    expect(result.ledgerEntry).toBeUndefined();
  });
});
