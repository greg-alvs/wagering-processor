import { Pool } from 'pg';
import { hashCanonicalJson } from '../../domain/idempotency/canonical-payload';
import { Money } from '../../domain/money/money';
import { Wallet } from '../../domain/wallet/wallet';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { WagerTransactionProcessor } from '../../domain/wagering/wager-transaction-processor';

export type WageringRequest = {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
};

export type WageringResult = {
  transactionId: string;
  status: WagerTransactionStatus;
  walletBalance: string;
  responseHash: string;
};

export class WageringProcessorService {
  constructor(private readonly pool: Pool) {}

  async process(request: WageringRequest): Promise<WageringResult> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const payloadHash = hashCanonicalJson(request);
      const keyClause = 'SELECT * FROM idempotency_keys WHERE key = $1 FOR UPDATE';
      const existingKey = await client.query(keyClause, [request.idempotencyKey]);

      if (existingKey.rowCount && existingKey.rowCount > 0) {
        const row = existingKey.rows[0];
        const incomingPayloadHash = payloadHash;

        if (row.payload_hash === incomingPayloadHash) {
          await client.query('COMMIT');
          return {
            transactionId: row.id,
            status: WagerTransactionStatus.Processed,
            walletBalance: '0.00',
            responseHash: row.response_hash,
          };
        }

        throw new Error('Idempotency conflict: same key with different payload.');
      }

      const walletRow = await client.query('SELECT * FROM wallets WHERE id = $1 FOR UPDATE', [request.walletId]);
      if (walletRow.rowCount === 0) {
        throw new Error(`Wallet ${request.walletId} not found.`);
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
        id: request.externalTransactionId,
        providerId: request.providerId,
        externalTransactionId: request.externalTransactionId,
        idempotencyKey: request.idempotencyKey,
        payloadHash,
        walletId: request.walletId,
        playerId: request.playerId,
        roundId: request.roundId,
        gameId: request.gameId,
        kind: request.kind,
        money: Money.from(request.money),
        referenceExternalTransactionId: request.referenceExternalTransactionId,
      });

      const result = WagerTransactionProcessor.process(wallet, transaction);

      const walletBalance = result.wallet.balance.toString();
      await client.query(
        `UPDATE wallets SET balance = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [walletBalance, request.walletId],
      );

      await client.query(
        `INSERT INTO wager_transactions (
          id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
          player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id,
          status, reference_transaction_id, failure_code, created_at, processed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
        [
          request.externalTransactionId,
          request.providerId,
          request.externalTransactionId,
          request.idempotencyKey,
          payloadHash,
          request.walletId,
          request.playerId,
          request.roundId,
          request.gameId,
          request.kind,
          result.transaction.money.toString(),
          request.money.currency,
          request.referenceExternalTransactionId,
          result.transaction.status,
          result.transaction.referenceTransactionId,
          result.transaction.failureCode,
          new Date(),
          result.transaction.processedAt ?? null,
        ],
      );

      const responseHash = hashCanonicalJson({
        transactionId: request.externalTransactionId,
        status: result.transaction.status,
        walletBalance,
      });

      await client.query(
        `INSERT INTO idempotency_keys (id, key, payload_hash, response_hash, created_at) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)`,
        [request.externalTransactionId, request.idempotencyKey, payloadHash, responseHash],
      );

      await client.query('COMMIT');

      return {
        transactionId: request.externalTransactionId,
        status: result.transaction.status,
        walletBalance,
        responseHash,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}
