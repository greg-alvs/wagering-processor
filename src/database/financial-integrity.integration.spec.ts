import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { WagerTransactionKind } from '../domain/wagering/wager-transaction';
import { WageringService } from '../wagering/wagering.service';
import { WalletsService } from '../wallets/wallets.service';

const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const walletsService = new WalletsService(pool);
const wageringService = new WageringService(pool);

afterAll(async () => {
  await pool.end();
});

describe('PostgreSQL financial integrity constraints', () => {
  test('defers wallet-to-ledger balance validation until the financial transaction commits', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `financial-integrity-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('UPDATE wallets SET balance = $1 WHERE id = $2', ['1.00', wallet.id]);
      await expect(client.query('COMMIT')).rejects.toMatchObject({ code: '23514' });
      await client.query('ROLLBACK');
      expect((await walletsService.getWallet(wallet.id)).balance.amount).toBe('0.00');
    } finally {
      client.release();
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('prevents a LOSS transaction from receiving a ledger entry', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `financial-integrity-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const result = await wageringService.createTransaction(
      {
        providerId: 'financial-integrity-test',
        externalTransactionId: randomUUID(),
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'financial-integrity-round',
        gameId: 'financial-integrity-game',
        kind: WagerTransactionKind.Loss,
        money: { amount: '1.00', currency: 'BRL' },
      },
      idempotencyKey,
    );
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      await client.query('UPDATE wallets SET balance = $1 WHERE id = $2', ['1.00', wallet.id]);
      await client.query(
        `INSERT INTO wallet_ledger_entries (
           id, wallet_id, transaction_id, direction, amount, currency,
           balance_before, balance_after, created_at
         ) VALUES ($1, $2, $3, 'CREDIT', 1.00, 'BRL', 0.00, 1.00, CURRENT_TIMESTAMP)`,
        [randomUUID(), wallet.id, result.transactionId],
      );
      await expect(client.query('COMMIT')).rejects.toMatchObject({ code: '23514' });
      await client.query('ROLLBACK');
    } finally {
      client.release();
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });
});
