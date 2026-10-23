import { ConflictException, Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import { Money } from '../domain/money/money';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { ApplicationMetrics } from '../infrastructure/observability/application-metrics';

@Injectable()
export class WalletsService {
  private readonly logger = new Logger(WalletsService.name);

  constructor(@Inject('DATABASE_POOL') private readonly pool: Pool) {}

  async createWallet(dto: CreateWalletDto) {
    const money = Money.from(dto.initialBalance);
    const existing = await this.pool.query(
      'SELECT id FROM wallets WHERE player_id = $1 AND currency = $2',
      [dto.playerId, dto.initialBalance.currency],
    );

    if (existing.rowCount && existing.rowCount > 0) {
      throw new ConflictException('Wallet already exists for this player and currency.');
    }

    const walletId = randomUUID();
    const balance = money.toString();

    const result = await this.pool.query(
      `INSERT INTO wallets (id, player_id, currency, balance, opening_balance, version, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $4, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
       RETURNING id, player_id as "playerId", currency, balance, version`,
      [walletId, dto.playerId, dto.initialBalance.currency, balance],
    );

    const row = result.rows[0];

    return {
      id: row.id,
      playerId: row.playerId,
      balance: {
        amount: String(row.balance),
        currency: row.currency,
      },
      version: Number(row.version),
    };
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

  async getLedger(walletId: string) {
    const wallet = await this.getWallet(walletId);

    const result = await this.pool.query(
      `SELECT id, wallet_id as "walletId", transaction_id as "transactionId", direction, amount, currency, balance_before as "balanceBefore", balance_after as "balanceAfter", created_at as "createdAt"
       FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY created_at DESC`,
      [walletId],
    );

    return {
      walletId,
      entries: result.rows.map((row) => ({
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
    };
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
}
