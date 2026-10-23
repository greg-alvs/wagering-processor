import { Money, MoneyProps } from '../money/money';

export type WalletState = {
  id: string;
  playerId: string;
  currency: string;
  balance: MoneyProps;
  version: number;
  createdAt: Date | string;
  updatedAt: Date | string;
};

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: { id: string; playerId: string; initialBalance: Money }): Wallet {
    if (!props.id || !props.playerId) {
      throw new Error('Wallet id and playerId are required.');
    }

    if (props.initialBalance.isNegative()) {
      throw new Error('Initial balance cannot be negative.');
    }

    const now = new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      Money.from(state.balance),
      state.version,
      new Date(state.createdAt),
      new Date(state.updatedAt),
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  credit(amount: Money): Wallet {
    this.assertSameCurrency(amount);
    if (amount.isNegative() || amount.isZero()) {
      throw new Error('Credit amount must be positive.');
    }

    this._balance = this._balance.add(amount);
    this._version += 1;
    this._updatedAt = new Date();
    return this;
  }

  debit(amount: Money): Wallet {
    this.assertSameCurrency(amount);
    if (amount.isNegative() || amount.isZero()) {
      throw new Error('Debit amount must be positive.');
    }

    const nextBalance = this._balance.subtract(amount);
    if (nextBalance.isNegative()) {
      throw new Error('Insufficient funds.');
    }

    this._balance = nextBalance;
    this._version += 1;
    this._updatedAt = new Date();
    return this;
  }

  private assertSameCurrency(amount: Money): void {
    if (this.currency !== amount.currency) {
      throw new Error(`Currency mismatch: wallet ${this.currency} != ${amount.currency}`);
    }
  }
}
