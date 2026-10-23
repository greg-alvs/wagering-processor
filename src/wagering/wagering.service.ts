import { HttpException, Inject, Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';
import { randomUUID } from 'node:crypto';
import { hashCanonicalJson } from '../domain/idempotency/canonical-payload';
import { Money } from '../domain/money/money';
import { Wallet } from '../domain/wallet/wallet';
import { FailureCode, WagerTransaction, WagerTransactionStatus } from '../domain/wagering/wager-transaction';
import { TransactionProcessingResult, WagerTransactionProcessor } from '../domain/wagering/wager-transaction-processor';
import { InboxRepository } from '../infrastructure/messaging/inbox.repository';
import { EventContext } from '../infrastructure/outbox/integration-event';
import { OutboxRepository } from '../infrastructure/outbox/outbox.repository';
import { eventsForTransaction, WalletBalanceChanged } from '../infrastructure/outbox/wager-transaction-events';
import { CreateWagerTransactionDto } from './dto/create-transaction.dto';

@Injectable()
export class WageringService {
  private readonly inboxRepository = new InboxRepository();
  private readonly outboxRepository = new OutboxRepository();

  constructor(@Inject('DATABASE_POOL') private readonly pool: Pool) {}

  async createTransaction(dto: CreateWagerTransactionDto) {
    return this.processTransaction(dto, undefined, { correlationId: randomUUID() });
  }

  async processQueueTransaction(dto: CreateWagerTransactionDto, messageId: string, consumerName: string) {
    const result = await this.processTransaction(dto, { messageId, consumerName }, {
      correlationId: messageId,
      causationId: messageId,
    });
    if ('duplicate' in result) {
      return result;
    }

    return { duplicate: false, result };
  }

  private async processTransaction(
    dto: CreateWagerTransactionDto,
    inbox?: { messageId: string; consumerName: string },
    eventContext: EventContext = { correlationId: randomUUID() },
  ) {
    const client = await this.pool.connect();
    let transactionStarted = false;

    try {
      await client.query('BEGIN');
      transactionStarted = true;

      if (inbox) {
        const inserted = await this.inboxRepository.tryInsert(client, inbox.consumerName, inbox.messageId);
        if (!inserted) {
          await client.query('COMMIT');
          transactionStarted = false;
          return { duplicate: true as const };
        }
      }

      let result: Awaited<ReturnType<WageringService['processWithinTransaction']>>;
      try {
        result = await this.processWithinTransaction(client, dto, eventContext);
      } catch (error) {
        if (inbox && error instanceof HttpException && error.getStatus() >= 400 && error.getStatus() < 500) {
          await client.query('COMMIT');
          transactionStarted = false;
          return { terminalFailure: true as const };
        }
        throw error;
      }
      await client.query('COMMIT');
      transactionStarted = false;
      return result;
    } catch (error) {
      if (transactionStarted) {
        await client.query('ROLLBACK');
      }
      throw error;
    } finally {
      client.release();
    }
  }

  private async processWithinTransaction(client: PoolClient, dto: CreateWagerTransactionDto, eventContext: EventContext) {
    const money = Money.from(dto.money);
    const payloadHash = hashCanonicalJson(dto);

    const keyCheck = await client.query(
      'SELECT * FROM idempotency_keys WHERE key = $1 FOR UPDATE',
      [dto.idempotencyKey],
    );

    if (keyCheck.rowCount && keyCheck.rowCount > 0) {
      const row = keyCheck.rows[0];
      const samePayload = row.payload_hash === payloadHash;
      if (samePayload) {
        return {
          id: row.id,
          status: row.response_hash ? 'PROCESSED' : 'PENDING',
          responseHash: row.response_hash,
        };
      }

      throw new BadRequestException('Idempotency key already used with different payload.');
    }

    const walletRow = await client.query('SELECT * FROM wallets WHERE id = $1 FOR UPDATE', [dto.walletId]);
    if (!walletRow.rowCount || walletRow.rowCount === 0) {
      throw new NotFoundException(`Wallet ${dto.walletId} not found.`);
    }

    const wallet = Wallet.rehydrate({
      id: walletRow.rows[0].id,
      playerId: walletRow.rows[0].player_id,
      currency: walletRow.rows[0].currency,
      balance: { amount: walletRow.rows[0].balance, currency: walletRow.rows[0].currency },
      version: Number(walletRow.rows[0].version),
      createdAt: walletRow.rows[0].created_at,
      updatedAt: walletRow.rows[0].updated_at,
    });

    const transaction = WagerTransaction.create({
      id: randomUUID(),
      providerId: dto.providerId,
      externalTransactionId: dto.externalTransactionId,
      idempotencyKey: dto.idempotencyKey,
      payloadHash,
      walletId: dto.walletId,
      playerId: dto.playerId,
      roundId: dto.roundId,
      gameId: dto.gameId,
      kind: dto.kind,
      money,
      referenceExternalTransactionId: dto.referenceExternalTransactionId,
    });

    let reference: WagerTransaction | undefined;
    if (transaction.requiresReference() && transaction.referenceExternalTransactionId) {
      const referenceRow = await client.query(
        `SELECT * FROM wager_transactions
         WHERE provider_id = $1 AND external_transaction_id = $2`,
        [transaction.providerId, transaction.referenceExternalTransactionId],
      );
      if (referenceRow.rowCount) {
        reference = this.rehydrateTransaction(referenceRow.rows[0]);
      }
    }

    const reversalAlreadyApplied =
      reference?.status === WagerTransactionStatus.Processed &&
      this.isValidReference(transaction, reference) &&
      (await this.hasProcessedReversal(client, reference.id));
    const result: TransactionProcessingResult = reversalAlreadyApplied
      ? this.rejectAlreadyReversed(wallet, transaction)
      : reference && reference.status !== WagerTransactionStatus.Processed
        ? reference.isTerminal()
          ? this.rejectInvalidReference(wallet, transaction)
          : WagerTransactionProcessor.process(wallet, transaction)
        : WagerTransactionProcessor.process(wallet, transaction, reference);
    const events = eventsForTransaction(transaction, eventContext);
    if (result.ledgerEntry) {
      events.push(
        WalletBalanceChanged.from(transaction, result.ledgerEntry, wallet.version + 1, eventContext),
      );
    }

    if (result.ledgerEntry) {
      await client.query(
        `UPDATE wallets SET balance = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [result.wallet.balance.toString(), dto.walletId],
      );
    }

    await client.query(
      `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, status, reference_transaction_id, failure_code, created_at, processed_at, reference_attempts, reference_next_attempt_at, reference_expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP, $17,
                 CASE WHEN $18 THEN 1 ELSE 0 END,
                 CASE WHEN $18 THEN CURRENT_TIMESTAMP + INTERVAL '1 second' END,
                 CASE WHEN $18 THEN CURRENT_TIMESTAMP + INTERVAL '24 hours' END)`,
      [
        transaction.id,
        dto.providerId,
        dto.externalTransactionId,
        dto.idempotencyKey,
        payloadHash,
        dto.walletId,
        dto.playerId,
        dto.roundId,
        dto.gameId,
        dto.kind,
        money.toString(),
        money.currency,
        dto.referenceExternalTransactionId,
        transaction.status,
        transaction.referenceTransactionId,
        transaction.failureCode,
        transaction.processedAt ?? null,
        transaction.status === WagerTransactionStatus.PendingReference,
      ],
    );

    if (result.ledgerEntry) {
      await client.query(
        `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)`,
        [
          randomUUID(),
          dto.walletId,
          transaction.id,
          result.ledgerEntry.direction,
          result.ledgerEntry.money.toString(),
          result.ledgerEntry.money.currency,
          result.ledgerEntry.balanceBefore.toString(),
          result.ledgerEntry.balanceAfter.toString(),
        ],
      );
    }

    const responseHash = hashCanonicalJson({
      transactionId: transaction.id,
      status: transaction.status,
      walletBalance: result.wallet.balance.toString(),
    });

    await client.query(
      `INSERT INTO idempotency_keys (id, key, payload_hash, response_hash, created_at)
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)`,
      [randomUUID(), dto.idempotencyKey, payloadHash, responseHash],
    );

    for (const event of events) {
      await this.outboxRepository.append(client, event);
    }

    return {
      transactionId: transaction.id,
      walletId: dto.walletId,
      status: transaction.status,
      balance: result.wallet.balance.toString(),
      responseHash,
    };
  }

  async reprocessPendingReference(
    client: PoolClient,
    transactionId: string,
    maxAttempts: number,
  ): Promise<'rescheduled' | 'processed' | 'rejected'> {
    const pendingResult = await client.query(
      `SELECT * FROM wager_transactions
       WHERE id = $1 AND status = $2
       FOR UPDATE`,
      [transactionId, WagerTransactionStatus.PendingReference],
    );
    const pendingRow = pendingResult.rows[0];
    if (!pendingRow) {
      return 'rejected';
    }

    const transaction = this.rehydrateTransaction(pendingRow);
    const attempts = Number(pendingRow.reference_attempts);
    const referenceResult = await client.query(
      `SELECT * FROM wager_transactions
       WHERE provider_id = $1 AND external_transaction_id = $2`,
      [transaction.providerId, transaction.referenceExternalTransactionId],
    );
    const referenceRow = referenceResult.rows[0];
    const reference = referenceRow ? this.rehydrateTransaction(referenceRow) : undefined;
    const expired =
      !pendingRow.reference_expires_at ||
      new Date(pendingRow.reference_expires_at).getTime() <= Date.now();

    if (expired || attempts >= maxAttempts) {
      transaction.reject(FailureCode.ReferenceNotFound);
      await this.persistPendingReferenceOutcome(client, transaction, undefined, {
        correlationId: transaction.id,
        causationId: transaction.id,
      });
      return 'rejected';
    }

    if (!reference || reference.status === WagerTransactionStatus.Pending || reference.status === WagerTransactionStatus.PendingReference) {
      if (attempts + 1 >= maxAttempts) {
        await client.query(
          'UPDATE wager_transactions SET reference_attempts = $2 WHERE id = $1',
          [transaction.id, maxAttempts],
        );
        transaction.reject(FailureCode.ReferenceNotFound);
        await this.persistPendingReferenceOutcome(client, transaction, undefined, {
          correlationId: transaction.id,
          causationId: transaction.id,
        });
        return 'rejected';
      }

      const delaySeconds = Math.min(2 ** Math.min(Math.max(attempts, 0), 30), 300);
      await client.query(
        `UPDATE wager_transactions
         SET reference_attempts = reference_attempts + 1,
             reference_next_attempt_at = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')
         WHERE id = $1`,
        [transaction.id, delaySeconds],
      );
      return 'rescheduled';
    }

    const context = { correlationId: transaction.id, causationId: reference.id };
    if (reference.status !== WagerTransactionStatus.Processed) {
      transaction.reject(FailureCode.InvalidReference);
      await this.persistPendingReferenceOutcome(client, transaction, undefined, context);
      return 'rejected';
    }

    const walletResult = await client.query('SELECT * FROM wallets WHERE id = $1 FOR UPDATE', [transaction.walletId]);
    if (!walletResult.rowCount) {
      throw new NotFoundException(`Wallet ${transaction.walletId} not found.`);
    }

    const wallet = Wallet.rehydrate({
      id: walletResult.rows[0].id,
      playerId: walletResult.rows[0].player_id,
      currency: walletResult.rows[0].currency,
      balance: { amount: walletResult.rows[0].balance, currency: walletResult.rows[0].currency },
      version: Number(walletResult.rows[0].version),
      createdAt: walletResult.rows[0].created_at,
      updatedAt: walletResult.rows[0].updated_at,
    });
    const reversalAlreadyApplied =
      this.isValidReference(transaction, reference) &&
      (await this.hasProcessedReversal(client, reference.id));
    if (reversalAlreadyApplied) {
      transaction.reject(FailureCode.ReferenceAlreadyReversed);
      await this.persistPendingReferenceOutcome(client, transaction, undefined, context);
      return 'rejected';
    }

    const result = WagerTransactionProcessor.process(wallet, transaction, reference);

    await this.persistPendingReferenceOutcome(client, result.transaction, result.ledgerEntry, context, result.wallet);
    return result.transaction.status === WagerTransactionStatus.Processed ? 'processed' : 'rejected';
  }

  private async persistPendingReferenceOutcome(
    client: PoolClient,
    transaction: WagerTransaction,
    ledgerEntry: ReturnType<typeof WagerTransactionProcessor.process>['ledgerEntry'],
    eventContext: EventContext,
    wallet?: Wallet,
  ): Promise<void> {
    if (ledgerEntry && wallet) {
      await client.query(
        `UPDATE wallets SET balance = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [wallet.balance.toString(), wallet.id],
      );
      await client.query(
        `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)`,
        [
          randomUUID(),
          wallet.id,
          transaction.id,
          ledgerEntry.direction,
          ledgerEntry.money.toString(),
          ledgerEntry.money.currency,
          ledgerEntry.balanceBefore.toString(),
          ledgerEntry.balanceAfter.toString(),
        ],
      );
    }

    await client.query(
      `UPDATE wager_transactions
       SET status = $2, reference_transaction_id = $3, failure_code = $4, processed_at = $5,
           reference_next_attempt_at = NULL, reference_expires_at = NULL
       WHERE id = $1`,
      [
        transaction.id,
        transaction.status,
        transaction.referenceTransactionId ?? null,
        transaction.failureCode ?? null,
        transaction.processedAt ?? null,
      ],
    );

    for (const event of eventsForTransaction(transaction, eventContext)) {
      await this.outboxRepository.append(client, event);
    }
    if (ledgerEntry && wallet) {
      await this.outboxRepository.append(
        client,
        WalletBalanceChanged.from(transaction, ledgerEntry, wallet.version + 1, eventContext),
      );
    }
  }

  private rejectInvalidReference(wallet: Wallet, transaction: WagerTransaction): TransactionProcessingResult {
    transaction.reject(FailureCode.InvalidReference);
    return { wallet, transaction };
  }

  private rejectAlreadyReversed(wallet: Wallet, transaction: WagerTransaction): TransactionProcessingResult {
    transaction.reject(FailureCode.ReferenceAlreadyReversed);
    return { wallet, transaction };
  }

  private isValidReference(transaction: WagerTransaction, reference: WagerTransaction): boolean {
    try {
      transaction.validateReference(reference);
      return true;
    } catch (error) {
      if (error instanceof Error) {
        return false;
      }
      throw error;
    }
  }

  private async hasProcessedReversal(client: PoolClient, referenceTransactionId: string): Promise<boolean> {
    const result = await client.query(
      `SELECT 1 FROM wager_transactions
       WHERE reference_transaction_id = $1
         AND status = $2
         AND kind IN ('REFUND', 'ROLLBACK')
       LIMIT 1`,
      [referenceTransactionId, WagerTransactionStatus.Processed],
    );
    return result.rowCount !== null && result.rowCount > 0;
  }

  private rehydrateTransaction(row: Record<string, unknown>): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: row.id as string,
      providerId: row.provider_id as string,
      externalTransactionId: row.external_transaction_id as string,
      idempotencyKey: row.idempotency_key as string,
      payloadHash: row.payload_hash as string,
      walletId: row.wallet_id as string,
      playerId: row.player_id as string,
      roundId: row.round_id as string,
      gameId: row.game_id as string,
      kind: row.kind as WagerTransaction['kind'],
      money: { amount: String(row.amount), currency: row.currency as string },
      referenceExternalTransactionId: row.reference_external_transaction_id as string | undefined,
      createdAt: row.created_at as Date | string,
      status: row.status as WagerTransactionStatus,
      referenceTransactionId: row.reference_transaction_id as string | undefined,
      failureCode: row.failure_code as FailureCode | undefined,
      processedAt: row.processed_at as Date | string | undefined,
    });
  }

  async getTransaction(transactionId: string) {
    const result = await this.pool.query('SELECT * FROM wager_transactions WHERE id = $1', [transactionId]);
    if (!result.rowCount || result.rowCount === 0) {
      throw new NotFoundException(`Transaction ${transactionId} not found.`);
    }

    const row = result.rows[0];
    return {
      id: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      status: row.status,
      kind: row.kind,
      amount: row.amount,
      currency: row.currency,
    };
  }

  async getTransactionByProvider(providerId: string, externalTransactionId: string) {
    const result = await this.pool.query(
      'SELECT * FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2',
      [providerId, externalTransactionId],
    );

    if (!result.rowCount || result.rowCount === 0) {
      throw new NotFoundException(`Transaction for provider ${providerId} and external id ${externalTransactionId} not found.`);
    }

    const row = result.rows[0];
    return {
      id: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      status: row.status,
      kind: row.kind,
      amount: row.amount,
      currency: row.currency,
      walletId: row.wallet_id,
    };
  }
}
