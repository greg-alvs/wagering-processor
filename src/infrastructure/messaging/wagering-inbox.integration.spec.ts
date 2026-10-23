import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';
import { WageringService } from '../../wagering/wagering.service';
import { WalletsService } from '../../wallets/wallets.service';

const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const wageringService = new WageringService(pool);
const walletsService = new WalletsService(pool);

afterAll(async () => {
  await pool.end();
});

describe('WageringService SQS inbox transaction', () => {
  test('commits inbox and financial effects together and suppresses redelivery', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const externalTransactionId = randomUUID();
    const idempotencyKey = randomUUID();
    const request = {
      providerId: 'integration-test',
      externalTransactionId,
      idempotencyKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      const first = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(first).toMatchObject({
        duplicate: false,
        result: { status: 'PROCESSED', balance: '7.00' },
      });

      const redelivery = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(redelivery).toEqual({ duplicate: true });

      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      expect(String(walletRow.rows[0].balance)).toBe('7.00');
      expect(ledgerRows.rowCount).toBe(1);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rolls inbox and wallet changes back when financial persistence fails', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const idempotencyKey = randomUUID();
    const request = {
      providerId: 'p'.repeat(65),
      externalTransactionId: randomUUID(),
      idempotencyKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      await expect(
        wageringService.processQueueTransaction(request, messageId, consumerName),
      ).rejects.toThrow();

      const inboxRow = await pool.query(
        'SELECT message_id FROM message_inbox WHERE consumer_name = $1 AND message_id = $2',
        [consumerName, messageId],
      );
      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      expect(inboxRow.rowCount).toBe(0);
      expect(String(walletRow.rows[0].balance)).toBe('10.00');
    } finally {
      await pool.query('DELETE FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('commits a terminal business failure to the inbox before acknowledging it', async () => {
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const request = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      idempotencyKey: randomUUID(),
      playerId: `missing-player-${randomUUID()}`,
      walletId: randomUUID(),
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      const result = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(result).toEqual({ duplicate: false, result: { terminalFailure: true } });

      const redelivery = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(redelivery).toEqual({ duplicate: true });
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
    }
  });
});
