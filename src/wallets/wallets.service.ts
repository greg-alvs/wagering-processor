import {
  BadRequestException,
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { Pool, PoolClient } from 'pg';
import { randomUUID } from 'node:crypto';
import { Money } from '../domain/money/money';
import { LedgerDirection, WalletLedgerEntry } from '../domain/ledger/ledger-entry';
import { FailureCode, WagerTransaction, WagerTransactionKind } from '../domain/wagering/wager-transaction';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { ApplicationMetrics } from '../infrastructure/observability/application-metrics';
import { hashCanonicalJson } from '../domain/idempotency/canonical-payload';
import { OutboxRepository } from '../infrastructure/outbox/outbox.repository';
import { EventContext } from '../infrastructure/outbox/integration-event';
import { eventsForTransaction, WalletBalanceChanged } from '../infrastructure/outbox/wager-transaction-events';

@Injectable()
export class WalletsService {
  private readonly logger = new Logger(WalletsService.name);
  private readonly outboxRepository = new OutboxRepository();

  constructor(@Inject('DATABASE_POOL') private readonly pool: Pool) {}

  async createWallet(dto: CreateWalletDto) {
    const money = Money.from(dto.initialBalance);
    const walletId = randomUUID();
    const balance = money.toString();
    const client = await this.pool.connect();
    let transactionStarted = false;
    const correlationId = randomUUID();
    let openingTransactionId: string | undefined;
    let openingTransactionCreated = false;

    try {
      await client.query('BEGIN');
      transactionStarted = true;
      const result = await client.query(
        `INSERT INTO wallets (
           id, player_id, currency, balance, opening_balance, version, created_at, updated_at
         )
         VALUES ($1, $2, $3, $4, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         RETURNING id, player_id as "playerId", currency, balance, version`,
        [walletId, dto.playerId, money.currency, balance],
      );
      const row = result.rows[0];

      if (money.isPositive()) {
        const eventContext: EventContext = { correlationId };
        const payload = {
          walletId,
          playerId: dto.playerId,
          money: money.toJSON(),
        };
        const transaction = WagerTransaction.create({
          id: randomUUID(),
          providerId: 'internal',
          externalTransactionId: `opening-${walletId}`,
          idempotencyKey: `opening:${walletId}`,
          payloadHash: hashCanonicalJson(payload),
          walletId,
          playerId: dto.playerId,
          roundId: 'wallet-opening',
          gameId: 'wallet-opening',
          kind: WagerTransactionKind.Opening,
          money,
        });
        transaction.markProcessed(undefined, new Date());
        openingTransactionId = transaction.id;

        const ledgerEntry = WalletLedgerEntry.create({
          id: randomUUID(),
          walletId,
          transactionId: transaction.id,
          direction: LedgerDirection.Credit,
          money,
          balanceBefore: Money.zero(money.currency),
          balanceAfter: money,
        });

        await this.insertOpeningTransaction(client, transaction);
        await client.query(
          `INSERT INTO wallet_ledger_entries (
             id, wallet_id, transaction_id, direction, amount, currency,
             balance_before, balance_after, created_at
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)`,
          [
            ledgerEntry.id,
            ledgerEntry.walletId,
            ledgerEntry.transactionId,
            ledgerEntry.direction,
            ledgerEntry.money.toString(),
            ledgerEntry.money.currency,
            ledgerEntry.balanceBefore.toString(),
            ledgerEntry.balanceAfter.toString(),
          ],
        );

        for (const event of eventsForTransaction(transaction, eventContext)) {
          await this.outboxRepository.append(client, event);
        }
        await this.outboxRepository.append(
          client,
          WalletBalanceChanged.from(transaction, ledgerEntry, Number(row.version), eventContext),
        );
        openingTransactionCreated = true;
      }

      await client.query('COMMIT');
      transactionStarted = false;
      if (openingTransactionCreated) {
        ApplicationMetrics.increment('wager_transactions_total', { status: 'PROCESSED' });
      }
      this.logger.log(
        JSON.stringify({
          event: 'wallet_created',
          correlationId,
          transactionId: openingTransactionId,
          walletId,
          providerId: openingTransactionId ? 'internal' : undefined,
        }),
      );

      return {
        id: row.id,
        playerId: row.playerId,
        balance: {
          amount: String(row.balance),
          currency: row.currency,
        },
        version: Number(row.version),
      };
    } catch (error) {
      if (transactionStarted) {
        await client.query('ROLLBACK');
      }
      if (this.isWalletUniquenessViolation(error)) {
        throw new ConflictException('Wallet already exists for this player and currency.');
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async getWallet(walletId: string) {
    const result = await this.pool.query(
      `SELECT id, player_id as "playerId", currency, balance, version, created_at as "createdAt", updated_at as "updatedAt"
       FROM wallets WHERE id = $1`,
      [walletId],
    );

    if (!result.rowCount || result.rowCount === 0) {
      throw new NotFoundException(`Wallet ${walletId} was not found.`);
    }

    const row = result.rows[0];

    return {
      id: row.id,
      playerId: row.playerId,
      balance: {
        amount: String(row.balance),
        currency: row.currency,
      },
      version: Number(row.version),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  }

  async getLedger(walletId: string, cursor?: string, limitParam?: string) {
    const wallet = await this.getWallet(walletId);
    const limit = this.parseLedgerLimit(limitParam);
    const position = cursor ? this.decodeLedgerCursor(cursor) : undefined;

    const result = await this.pool.query(
      `SELECT id, wallet_id as "walletId", transaction_id as "transactionId", direction,
              amount, currency, balance_before as "balanceBefore", balance_after as "balanceAfter",
              created_at as "createdAt",
              to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as "cursorTimestamp"
       FROM wallet_ledger_entries
       WHERE wallet_id = $1
         AND ($2::timestamptz IS NULL OR (created_at, id) < ($2::timestamptz, $3::varchar))
       ORDER BY created_at DESC, id DESC
       LIMIT $4`,
      [walletId, position?.createdAt ?? null, position?.id ?? null, limit + 1],
    );
    const hasMore = result.rows.length > limit;
    const pageRows = hasMore ? result.rows.slice(0, limit) : result.rows;
    const lastRow = pageRows.at(-1);

    return {
      walletId,
      entries: pageRows.map((row) => ({
        id: row.id,
        transactionId: row.transactionId,
        direction: row.direction,
        amount: {
          amount: String(row.amount),
          currency: row.currency,
        },
        balanceBefore: {
          amount: String(row.balanceBefore),
          currency: row.currency,
        },
        balanceAfter: {
          amount: String(row.balanceAfter),
          currency: row.currency,
        },
        createdAt: row.createdAt,
      })),
      limit,
      hasMore,
      nextCursor: hasMore && lastRow
        ? Buffer.from(JSON.stringify({
            createdAt: lastRow.cursorTimestamp,
            id: lastRow.id,
          })).toString('base64url')
        : null,
    };
  }

  private parseLedgerLimit(limitParam?: string): number {
    if (limitParam === undefined) {
      return 50;
    }

    if (typeof limitParam !== 'string' || !/^\d+$/.test(limitParam)) {
      throw new BadRequestException('Ledger limit must be an integer between 1 and 100.');
    }

    const limit = Number(limitParam);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new BadRequestException('Ledger limit must be an integer between 1 and 100.');
    }

    return limit;
  }

  private decodeLedgerCursor(cursor: string): { createdAt: string; id: string } {
    if (
      typeof cursor !== 'string' ||
      cursor.length === 0 ||
      cursor.length > 512 ||
      !/^[A-Za-z0-9_-]+$/.test(cursor)
    ) {
      throw new BadRequestException('Ledger cursor is invalid.');
    }

    let parsed: unknown;
    try {
      const json = Buffer.from(cursor, 'base64url').toString('utf8');
      if (Buffer.from(json, 'utf8').toString('base64url') !== cursor) {
        throw new Error('Non-canonical base64url cursor.');
      }
      parsed = JSON.parse(json);
    } catch {
      throw new BadRequestException('Ledger cursor is invalid.');
    }

    if (
      !parsed ||
      typeof parsed !== 'object' ||
      !('createdAt' in parsed) ||
      typeof parsed.createdAt !== 'string' ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(parsed.createdAt) ||
      Number.isNaN(Date.parse(parsed.createdAt)) ||
      !('id' in parsed) ||
      typeof parsed.id !== 'string' ||
      parsed.id.length === 0 ||
      parsed.id.length > 36
    ) {
      throw new BadRequestException('Ledger cursor is invalid.');
    }

    return { createdAt: parsed.createdAt, id: parsed.id };
  }

  async reconcile(walletId: string) {
    const result = await this.pool.query(
      `SELECT
         wallet.id,
         wallet.currency,
         wallet.balance AS stored_balance,
         wallet.opening_balance
           + coalesce(
             sum(case when entry.direction = 'CREDIT' then entry.amount else -entry.amount end),
             0
           ) AS calculated_balance,
         count(entry.id) AS checked_entries
       FROM wallets wallet
       LEFT JOIN wallet_ledger_entries entry ON entry.wallet_id = wallet.id
       WHERE wallet.id = $1
       GROUP BY wallet.id`,
      [walletId],
    );

    if (result.rowCount === 0) {
      throw new NotFoundException(`Wallet ${walletId} was not found.`);
    }

    const row = result.rows[0];
    const currency = row.currency as string;
    const storedBalance = Money.from({ amount: String(row.stored_balance), currency });
    const calculatedBalance = Money.from({ amount: String(row.calculated_balance), currency });
    const difference = Money.fromMinorUnits(
      storedBalance.value - calculatedBalance.value,
      currency,
    );
    const consistent = difference.isZero();

    if (!consistent) {
      ApplicationMetrics.increment('wallet_reconciliation_divergences_total');
      this.logger.warn(
        JSON.stringify({
          event: 'wallet_reconciliation_divergence',
          correlationId: randomUUID(),
          walletId,
          currency,
          checkedEntries: Number(row.checked_entries),
        }),
      );
    }

    return {
      walletId,
      storedBalance: storedBalance.toJSON(),
      calculatedBalance: calculatedBalance.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: Number(row.checked_entries),
    };
  }

  private async insertOpeningTransaction(client: PoolClient, transaction: WagerTransaction): Promise<void> {
    await client.query(
      `INSERT INTO wager_transactions (
         id, provider_id, external_transaction_id, idempotency_key, payload_hash,
         wallet_id, player_id, round_id, game_id, kind, amount, currency,
         reference_external_transaction_id, status, reference_transaction_id,
         failure_code, created_at, processed_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, $13, NULL, $14, $15, $16)`,
      [
        transaction.id,
        transaction.providerId,
        transaction.externalTransactionId,
        transaction.idempotencyKey,
        transaction.payloadHash,
        transaction.walletId,
        transaction.playerId,
        transaction.roundId,
        transaction.gameId,
        transaction.kind,
        transaction.money.toString(),
        transaction.money.currency,
        transaction.status,
        transaction.failureCode ?? null,
        transaction.createdAt,
        transaction.processedAt,
      ],
    );
  }

  private isWalletUniquenessViolation(error: unknown): boolean {
    return (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      error.code === '23505' &&
      'constraint' in error &&
      error.constraint === 'wallets_player_currency_unique'
    );
  }
}
