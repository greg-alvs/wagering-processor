import { Money } from '../money/money';
import { Wallet } from '../wallet/wallet';
import { WalletLedgerEntry, LedgerDirection } from '../ledger/ledger-entry';
import { FailureCode, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';

export type TransactionProcessingResult = {
  wallet: Wallet;
  transaction: WagerTransaction;
  ledgerEntry?: WalletLedgerEntry;
};

export class WagerTransactionProcessor {
  private static walletLocks = new Map<string, Promise<void>>();

  static async processSerialized(wallet: Wallet, transaction: WagerTransaction, reference?: WagerTransaction): Promise<TransactionProcessingResult> {
    const lockKey = wallet.id;
    const previous = this.walletLocks.get(lockKey) ?? Promise.resolve();

    let release: (() => void) | undefined;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });

    this.walletLocks.set(lockKey, previous.then(() => current));

    await previous;
    try {
      return this.process(wallet, transaction, reference);
    } finally {
      release?.();
      const next = this.walletLocks.get(lockKey);
      if (next === current) {
        this.walletLocks.delete(lockKey);
      }
    }
  }

  static process(wallet: Wallet, transaction: WagerTransaction, reference?: WagerTransaction): TransactionProcessingResult {
    if (transaction.kind === WagerTransactionKind.Loss) {
      transaction.markProcessed(undefined, new Date());
      return { wallet, transaction };
    }

    if (transaction.requiresReference()) {
      if (!reference) {
        transaction.markPendingReference();
        return { wallet, transaction };
      }

      try {
        transaction.validateReference(reference);
      } catch {
        transaction.reject(FailureCode.InvalidReference);
        return { wallet, transaction };
      }
    }

    if (transaction.kind === WagerTransactionKind.Bet) {
      if (!transaction.canApplyToBalance(wallet.balance)) {
        transaction.reject(FailureCode.InsufficientFunds);
        return { wallet, transaction };
      }

      const before = wallet.balance;
      wallet.debit(transaction.money);
      const after = wallet.balance;
      transaction.markProcessed(undefined, new Date());

      const entry = WalletLedgerEntry.create({
        id: `ledger-${transaction.id}`,
        walletId: wallet.id,
        transactionId: transaction.id,
        direction: LedgerDirection.Debit,
        money: transaction.money,
        balanceBefore: before,
        balanceAfter: after,
      });

      return { wallet, transaction, ledgerEntry: entry };
    }

    if (transaction.kind === WagerTransactionKind.Win) {
      const before = wallet.balance;
      wallet.credit(transaction.money);
      const after = wallet.balance;
      transaction.markProcessed(undefined, new Date());

      const entry = WalletLedgerEntry.create({
        id: `ledger-${transaction.id}`,
        walletId: wallet.id,
        transactionId: transaction.id,
        direction: LedgerDirection.Credit,
        money: transaction.money,
        balanceBefore: before,
        balanceAfter: after,
      });

      return { wallet, transaction, ledgerEntry: entry };
    }

    if (transaction.kind === WagerTransactionKind.Refund || transaction.kind === WagerTransactionKind.Rollback) {
      if (!reference) {
        transaction.markPendingReference();
        return { wallet, transaction };
      }

      try {
        transaction.validateReference(reference);
      } catch {
        transaction.reject(FailureCode.InvalidReference);
        return { wallet, transaction };
      }

      const before = wallet.balance;
      const direction = transaction.ledgerDirectionFor(reference);
      if (direction === LedgerDirection.Credit) {
        wallet.credit(reference.money);
      } else {
        wallet.debit(reference.money);
      }
      const after = wallet.balance;
      transaction.markProcessed(reference.id, new Date());

      const entry = WalletLedgerEntry.create({
        id: `ledger-${transaction.id}`,
        walletId: wallet.id,
        transactionId: transaction.id,
        direction,
        money: reference.money,
        balanceBefore: before,
        balanceAfter: after,
      });

      return { wallet, transaction, ledgerEntry: entry };
    }

    transaction.markProcessed(undefined, new Date());
    return { wallet, transaction };
  }
}
