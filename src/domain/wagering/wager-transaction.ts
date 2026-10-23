import { Money } from '../money/money';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  InvalidReference = 'INVALID_REFERENCE',
  DuplicatePayload = 'DUPLICATE_PAYLOAD',
  InvalidAmount = 'INVALID_AMOUNT',
  InvalidTransactionKind = 'INVALID_TRANSACTION_KIND',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
}

export type CreateWagerTransactionProps = {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt?: Date;
};

export type WagerTransactionState = {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
  createdAt: Date | string;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date | string;
};

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {}

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!props.id || !props.providerId || !props.externalTransactionId || !props.idempotencyKey || !props.payloadHash) {
      throw new Error('Transaction requires id, providerId, externalTransactionId, idempotencyKey and payloadHash.');
    }

    if (!props.walletId || !props.playerId || !props.roundId || !props.gameId) {
      throw new Error('Transaction requires walletId, playerId, roundId and gameId.');
    }

    if (props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback) {
      if (!props.referenceExternalTransactionId) {
        throw new Error(`Transaction kind ${props.kind} requires referenceExternalTransactionId.`);
      }
    }

    if (props.money.isZero()) {
      throw new Error('Transaction amount cannot be zero.');
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt ?? new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      Money.from(state.money),
      state.referenceExternalTransactionId,
      new Date(state.createdAt),
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt ? new Date(state.processedAt) : undefined,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    if (this.isTerminal()) {
      throw new Error(`Cannot transition terminal transaction ${this.id} to processed.`);
    }

    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  markPendingReference(): void {
    if (this.isTerminal()) {
      throw new Error(`Cannot transition terminal transaction ${this.id} to pending reference.`);
    }

    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    if (this.isTerminal()) {
      throw new Error(`Cannot reject terminal transaction ${this.id}.`);
    }

    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  fail(code: FailureCode): void {
    if (this.isTerminal()) {
      throw new Error(`Cannot fail terminal transaction ${this.id}.`);
    }

    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind === WagerTransactionKind.Bet || this.kind === WagerTransactionKind.Win || this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  canApplyToBalance(balance: Money): boolean {
    if (!this.affectsBalance()) {
      return true;
    }

    if (this.kind === WagerTransactionKind.Win || this.kind === WagerTransactionKind.Refund) {
      return true;
    }

    if (this.kind === WagerTransactionKind.Bet) {
      return !balance.isLessThan(this.money);
    }

    return true;
  }

  validateReference(reference: WagerTransaction | undefined): void {
    if (!this.requiresReference()) {
      return;
    }

    if (!reference) {
      throw new Error(`Transaction kind ${this.kind} requires a reference.`);
    }

    if (reference.isTerminal() === false) {
      throw new Error(`Reference transaction ${reference.id} must be processed before ${this.kind}.`);
    }

    if (reference.providerId !== this.providerId) {
      throw new Error('Reference must belong to the same provider.');
    }

    if (reference.playerId !== this.playerId || reference.walletId !== this.walletId) {
      throw new Error('Reference must belong to the same player and wallet.');
    }

    if (this.money.currency !== reference.money.currency) {
      throw new Error('Reference and current transaction must share the same currency.');
    }

    if (reference.roundId !== this.roundId) {
      throw new Error('Reference must belong to the same round.');
    }

    if (this.kind === WagerTransactionKind.Refund && reference.kind !== WagerTransactionKind.Bet) {
      throw new Error('Refund can only reference a BET transaction.');
    }

    if (this.kind === WagerTransactionKind.Rollback) {
      const validRollbackReferences = new Set([
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
      ]);

      if (!validRollbackReferences.has(reference.kind)) {
        throw new Error('Rollback can only reference BET, WIN or REFUND transactions.');
      }
    }

    if (reference.money.equals(this.money) === false) {
      throw new Error('Referenced transaction and current transaction must have the same amount.');
    }

    if (reference.referenceExternalTransactionId === this.referenceExternalTransactionId) {
      throw new Error('Reference cannot be reversed twice with the same operation.');
    }
  }

  resolveLedgerImpact(reference?: WagerTransaction): { direction: LedgerDirection; amount: Money; changesBalance: boolean } {
    if (this.kind === WagerTransactionKind.Loss) {
      return { direction: LedgerDirection.Debit, amount: this.money, changesBalance: false };
    }

    if (this.kind === WagerTransactionKind.Rollback) {
      if (!reference) {
        throw new Error('Rollback requires a reference.');
      }

      const direction = reference.kind === WagerTransactionKind.Bet || reference.kind === WagerTransactionKind.Rollback
        ? LedgerDirection.Credit
        : LedgerDirection.Debit;

      return { direction, amount: reference.money, changesBalance: true };
    }

    return {
      direction: this.ledgerDirectionFor(reference),
      amount: this.money,
      changesBalance: this.affectsBalance(),
    };
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (this.kind === WagerTransactionKind.Bet) return LedgerDirection.Debit;
    if (this.kind === WagerTransactionKind.Win) return LedgerDirection.Credit;
    if (this.kind === WagerTransactionKind.Refund) return LedgerDirection.Credit;
    if (this.kind === WagerTransactionKind.Rollback) {
      if (!reference) {
        throw new Error('Rollback requires a reference to determine direction.');
      }
      return reference.kind === WagerTransactionKind.Bet || reference.kind === WagerTransactionKind.Rollback
        ? LedgerDirection.Credit
        : LedgerDirection.Debit;
    }

    return LedgerDirection.Credit;
  }
}

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}
