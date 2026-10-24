import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { BadRequestException } from '@nestjs/common';
import { Pool } from 'pg';
import { WalletsController } from './wallets.controller';
import { WalletsService } from './wallets.service';

const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const walletsService = new WalletsService(pool);
const walletsController = new WalletsController(walletsService);

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

describe('Wallet ledger pagination', () => {
  test('paginates with a stable timestamp and ID cursor without skipping tied entries', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const entryIds = [randomUUID(), randomUUID(), randomUUID(), randomUUID()].sort();
    const transactionIds = entryIds.map(() => randomUUID());

    try {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        for (let index = 0; index < entryIds.length; index += 1) {
          await client.query(
            `INSERT INTO wager_transactions (
               id, provider_id, external_transaction_id, idempotency_key, payload_hash,
               wallet_id, player_id, round_id, game_id, kind, amount, currency,
               status, created_at, processed_at
             ) VALUES ($1, 'ledger-pagination-test', $2, $3, 'fixture', $4, $5, 'pagination-round',
                       'pagination-game', 'WIN', 1.00, 'BRL', 'PROCESSED',
                       '2026-10-23T12:34:56.123456Z', '2026-10-23T12:34:56.123456Z')`,
            [transactionIds[index], transactionIds[index], transactionIds[index], wallet.id, wallet.playerId],
          );
          await client.query(
            `INSERT INTO wallet_ledger_entries
               (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
             VALUES ($1, $2, $3, 'CREDIT', 1.00, 'BRL', $4, $5,
                     '2026-10-23T12:34:56.123456Z'::timestamptz)`,
            [entryIds[index], wallet.id, transactionIds[index], index.toFixed(2), (index + 1).toFixed(2)],
          );
        }
        await client.query('UPDATE wallets SET balance = $1 WHERE id = $2', ['4.00', wallet.id]);
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      const firstPage = await walletsController.getLedger(wallet.id, undefined, '2');
      expect(firstPage.entries.map((entry) => entry.id)).toEqual(entryIds.slice().reverse().slice(0, 2));
      expect(firstPage).toMatchObject({ limit: 2, hasMore: true });
      expect(firstPage.nextCursor).toEqual(expect.any(String));

      const secondPage = await walletsController.getLedger(wallet.id, firstPage.nextCursor!, '2');
      expect(secondPage.entries.map((entry) => entry.id)).toEqual(entryIds.slice().reverse().slice(2));
      expect(secondPage).toMatchObject({ limit: 2, hasMore: false, nextCursor: null });

      const allIds = [...firstPage.entries, ...secondPage.entries].map((entry) => entry.id);
      expect(allIds).toHaveLength(4);
      expect(new Set(allIds).size).toBe(4);
    } finally {
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rejects malformed cursors and out-of-range limits', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });

    try {
      await expect(walletsController.getLedger(wallet.id, 'not-a-cursor')).rejects.toBeInstanceOf(
        BadRequestException,
      );
      await expect(walletsController.getLedger(wallet.id, undefined, '0')).rejects.toBeInstanceOf(
        BadRequestException,
      );
      await expect(walletsController.getLedger(wallet.id, undefined, '101')).rejects.toBeInstanceOf(
        BadRequestException,
      );
      await expect(walletsController.getLedger(wallet.id, undefined, '2.5')).rejects.toBeInstanceOf(
        BadRequestException,
      );
    } finally {
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rejects row mutations and installs truncation protection in PostgreSQL', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '1.00', currency: 'BRL' },
    });

    try {
      const ledgerRow = await pool.query(
        'SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1 LIMIT 1',
        [wallet.id],
      );
      const ledgerEntryId = ledgerRow.rows[0].id as string;

      await expect(
        pool.query('UPDATE wallet_ledger_entries SET amount = $1 WHERE id = $2', ['2.00', ledgerEntryId]),
      ).rejects.toMatchObject({ code: '55000' });
      await expect(
        pool.query('DELETE FROM wallet_ledger_entries WHERE id = $1', [ledgerEntryId]),
      ).rejects.toMatchObject({ code: '55000' });

      const triggers = await pool.query(
        `SELECT tgname, pg_get_triggerdef(oid) AS definition
         FROM pg_trigger
         WHERE tgrelid = 'wallet_ledger_entries'::regclass AND NOT tgisinternal`,
      );
      const rowTrigger = triggers.rows.find((row) => row.tgname === 'wallet_ledger_entries_immutable_row');
      const truncateTrigger = triggers.rows.find((row) => row.tgname === 'wallet_ledger_entries_immutable_truncate');
      expect(rowTrigger?.definition).toContain('BEFORE DELETE OR UPDATE');
      expect(truncateTrigger?.definition).toContain('TRUNCATE');
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });
});
