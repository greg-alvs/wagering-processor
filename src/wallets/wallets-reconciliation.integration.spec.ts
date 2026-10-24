import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { WagerTransactionKind } from '../domain/wagering/wager-transaction';
import { ApplicationMetrics } from '../infrastructure/observability/application-metrics';
import { WageringService } from '../wagering/wagering.service';
import { WalletsService } from './wallets.service';

const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const walletsService = new WalletsService(pool);
const wageringService = new WageringService(pool);

async function deleteLedgerEntriesForTest(walletId: string): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL session_replication_role = 'replica'");
    await client.query('DELETE FROM wallet_ledger_entries WHERE wallet_id = $1', [walletId]);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

afterAll(async () => {
  await pool.end();
});

describe('Wallet reconciliation', () => {
  test('compares the stored balance with the opening balance and ledger without correcting divergence', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const externalTransactionId = randomUUID();

    try {
      const opening = await pool.query(
        `SELECT transaction.id, transaction.kind, transaction.status, transaction.amount,
                entry.direction, entry.amount AS ledger_amount, entry.balance_before, entry.balance_after
         FROM wager_transactions transaction
         JOIN wallet_ledger_entries entry ON entry.transaction_id = transaction.id
         WHERE transaction.wallet_id = $1`,
        [wallet.id],
      );
      expect(opening.rows).toHaveLength(1);
      expect(opening.rows[0]).toMatchObject({
        kind: 'OPENING',
        status: 'PROCESSED',
        amount: '100.00',
        direction: 'CREDIT',
        ledger_amount: '100.00',
        balance_before: '0.00',
        balance_after: '100.00',
      });

      const emptyLedger = await walletsService.reconcile(wallet.id);
      expect(emptyLedger).toMatchObject({
        walletId: wallet.id,
        storedBalance: { amount: '100.00', currency: 'BRL' },
        calculatedBalance: { amount: '100.00', currency: 'BRL' },
        difference: { amount: '0.00', currency: 'BRL' },
        consistent: true,
        checkedEntries: 1,
      });

      await wageringService.createTransaction(
        {
          providerId: 'reconciliation-test',
          externalTransactionId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '20.00', currency: 'BRL' },
        },
        idempotencyKey,
      );

      const consistentLedger = await walletsService.reconcile(wallet.id);
      expect(consistentLedger).toMatchObject({
        storedBalance: { amount: '80.00', currency: 'BRL' },
        calculatedBalance: { amount: '80.00', currency: 'BRL' },
        difference: { amount: '0.00', currency: 'BRL' },
        consistent: true,
        checkedEntries: 2,
      });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query("SET LOCAL session_replication_role = 'replica'");
        await client.query('UPDATE wallets SET balance = $1 WHERE id = $2', ['79.50', wallet.id]);
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
      const divergence = await walletsService.reconcile(wallet.id);

      expect(divergence).toMatchObject({
        storedBalance: { amount: '79.50', currency: 'BRL' },
        calculatedBalance: { amount: '80.00', currency: 'BRL' },
        difference: { amount: '-0.50', currency: 'BRL' },
        consistent: false,
        checkedEntries: 2,
      });
      const storedWallet = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      expect(String(storedWallet.rows[0].balance)).toBe('79.50');
      expect(ApplicationMetrics.render()).toContain(
        'wallet_reconciliation_divergences_total 1',
      );
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('does not create an opening transaction or ledger entry for a zero balance wallet', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });

    try {
      const records = await pool.query(
        `SELECT
           (SELECT count(*) FROM wager_transactions WHERE wallet_id = $1) AS transactions,
           (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1) AS entries,
           (SELECT count(*) FROM outbox_events WHERE aggregate_id = $1) AS events`,
        [wallet.id],
      );
      expect(records.rows[0]).toMatchObject({ transactions: '0', entries: '0', events: '0' });
      await expect(walletsService.reconcile(wallet.id)).resolves.toMatchObject({
        storedBalance: { amount: '0.00', currency: 'BRL' },
        calculatedBalance: { amount: '0.00', currency: 'BRL' },
        consistent: true,
        checkedEntries: 0,
      });
    } finally {
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });
});
