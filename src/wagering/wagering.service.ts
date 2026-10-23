import { Inject, Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import { hashCanonicalJson } from '../domain/idempotency/canonical-payload';
import { Money } from '../domain/money/money';
import { Wallet } from '../domain/wallet/wallet';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/wagering/wager-transaction';
import { WagerTransactionProcessor } from '../domain/wagering/wager-transaction-processor';
import { CreateWagerTransactionDto } from './dto/create-transaction.dto';

@Injectable()
export class WageringService {
  constructor(@Inject('DATABASE_POOL') private readonly pool: Pool) {}

  async createTransaction(dto: CreateWagerTransactionDto) {
    const money = Money.from(dto.money);
    const payloadHash = hashCanonicalJson(dto);
    const responseHashSeed = {
      transactionId: dto.externalTransactionId,
      status: WagerTransactionStatus.Pending,
      walletId: dto.walletId,
    };

    const keyCheck = await this.pool.query(
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

    const walletRow = await this.pool.query('SELECT * FROM wallets WHERE id = $1 FOR UPDATE', [dto.walletId]);
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

    const result = WagerTransactionProcessor.process(wallet, transaction);
    const transactionRecord = {
      id: transaction.id,
      providerId: dto.providerId,
      externalTransactionId: dto.externalTransactionId,
      idempotencyKey: dto.idempotencyKey,
      payloadHash,
      walletId: dto.walletId,
      playerId: dto.playerId,
      roundId: dto.roundId,
      gameId: dto.gameId,
      kind: dto.kind,
      amount: money.toString(),
      currency: money.currency,
      referenceExternalTransactionId: dto.referenceExternalTransactionId,
      status: transaction.status,
      referenceTransactionId: transaction.referenceTransactionId,
      failureCode: transaction.failureCode,
      processedAt: transaction.processedAt,
    };

    await this.pool.query('BEGIN');
    try {
      if (result.ledgerEntry) {
        await this.pool.query(
          `UPDATE wallets SET balance = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
          [result.wallet.balance.toString(), dto.walletId],
        );
      }

      await this.pool.query(
        `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, status, reference_transaction_id, failure_code, created_at, processed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP, $17)`,
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
        ],
      );

      if (result.ledgerEntry) {
        await this.pool.query(
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

      await this.pool.query(
        `INSERT INTO idempotency_keys (id, key, payload_hash, response_hash, created_at)
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)`,
        [randomUUID(), dto.idempotencyKey, payloadHash, responseHash],
      );

      await this.pool.query('COMMIT');

      return {
        transactionId: transaction.id,
        walletId: dto.walletId,
        status: transaction.status,
        balance: result.wallet.balance.toString(),
        responseHash,
      };
    } catch (error) {
      await this.pool.query('ROLLBACK');
      throw error;
    }
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
