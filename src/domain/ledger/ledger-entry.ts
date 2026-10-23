import { Money } from '../money/money';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export type LedgerEntryState = {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: { amount: string; currency: string };
  balanceBefore: { amount: string; currency: string };
  balanceAfter: { amount: string; currency: string };
  createdAt: Date | string;
};

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: {
    id: string;
    walletId: string;
    transactionId: string;
    direction: LedgerDirection;
    money: Money;
    balanceBefore: Money;
    balanceAfter: Money;
    createdAt?: Date;
  }): WalletLedgerEntry {
    const id = props.id?.trim();
    const walletId = props.walletId?.trim();
    const transactionId = props.transactionId?.trim();

    if (!id || !walletId || !transactionId) {
      throw new Error('Ledger entry requires id, walletId and transactionId.');
    }

    if (props.money.isZero()) {
      throw new Error('Ledger entries cannot be zero-valued.');
    }

    if (props.money.currency !== props.balanceBefore.currency || props.money.currency !== props.balanceAfter.currency) {
      throw new Error('Ledger entry currency must be consistent across wallet and balances.');
    }

    if (props.balanceBefore.currency !== props.balanceAfter.currency) {
      throw new Error('Balance before and after must share the same currency.');
    }

    const expectedBalance =
      props.direction === LedgerDirection.Debit
        ? props.balanceBefore.subtract(props.money)
        : props.balanceBefore.add(props.money);

    if (!expectedBalance.equals(props.balanceAfter)) {
      throw new Error('Ledger entry is not balanced.');
    }

    return new WalletLedgerEntry(
      id,
      walletId,
      transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ?? new Date(),
    );
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return WalletLedgerEntry.create({
      id: state.id,
      walletId: state.walletId,
      transactionId: state.transactionId,
      direction: state.direction,
      money: Money.from(state.money),
      balanceBefore: Money.from(state.balanceBefore),
      balanceAfter: Money.from(state.balanceAfter),
      createdAt: new Date(state.createdAt),
    });
  }

  isBalanced(): boolean {
    const expectedBalance =
      this.direction === LedgerDirection.Debit
        ? this.balanceBefore.subtract(this.money)
        : this.balanceBefore.add(this.money);

    return expectedBalance.equals(this.balanceAfter);
  }
}
