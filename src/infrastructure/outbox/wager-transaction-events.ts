import { LedgerDirection } from '../../domain/ledger/ledger-entry';
import { WagerTransaction, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { EventContext, IntegrationEvent } from './integration-event';

export class WagerTransactionProcessed extends IntegrationEvent<{
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: { amount: string; currency: string };
  referenceTransactionId?: string;
}> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(
      IntegrationEvent.baseProps(transaction.walletId, {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        ...(transaction.referenceTransactionId
          ? { referenceTransactionId: transaction.referenceTransactionId }
          : {}),
      }, context),
    );
  }
}

export class WagerTransactionRejected extends IntegrationEvent<{
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  failureCode?: string;
}> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected(
      IntegrationEvent.baseProps(transaction.walletId, {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
      }, context),
    );
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<{
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  referenceExternalTransactionId?: string;
}> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(
      IntegrationEvent.baseProps(transaction.walletId, {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        ...(transaction.referenceExternalTransactionId
          ? { referenceExternalTransactionId: transaction.referenceExternalTransactionId }
          : {}),
      }, context),
    );
  }
}

export class WalletBalanceChanged extends IntegrationEvent<{
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: { amount: string; currency: string };
  balanceBefore: { amount: string; currency: string };
  balanceAfter: { amount: string; currency: string };
  walletVersion: number;
}> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    entry: {
      direction: LedgerDirection;
      money: { toJSON(): { amount: string; currency: string } };
      balanceBefore: { toJSON(): { amount: string; currency: string } };
      balanceAfter: { toJSON(): { amount: string; currency: string } };
    },
    walletVersion: number,
    context: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged(
      IntegrationEvent.baseProps(transaction.walletId, {
        walletId: transaction.walletId,
        transactionId: transaction.id,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion,
      }, context),
    );
  }
}

export function eventsForTransaction(
  transaction: WagerTransaction,
  context: EventContext,
): IntegrationEvent<unknown>[] {
  switch (transaction.status) {
    case WagerTransactionStatus.Processed:
      return [WagerTransactionProcessed.from(transaction, context)];
    case WagerTransactionStatus.Rejected:
      return [WagerTransactionRejected.from(transaction, context)];
    case WagerTransactionStatus.PendingReference:
      return [WagerTransactionPendingReference.from(transaction, context)];
    default:
      throw new Error(`Cannot create an integration event for status ${transaction.status}.`);
  }
}
