import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { BadRequestException } from '@nestjs/common';
import { WagerTransactionKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { PendingReferenceWorkerService } from './pending-reference-worker.service';
import { WageringController } from '../../wagering/wagering.controller';
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
const pendingReferenceWorker = new PendingReferenceWorkerService(pool, wageringService);
const walletsService = new WalletsService(pool);
const wageringController = new WageringController(wageringService);

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

describe('WageringService SQS inbox transaction', () => {
  test('replays the original HTTP response for an identical idempotency key and conflicts on a changed payload', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const secondIdempotencyKey = randomUUID();
    const payload = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      expect(() => wageringController.createTransaction(payload, undefined)).toThrow(BadRequestException);

      const responseStatuses: number[] = [];
      const response = { status: (statusCode: number) => responseStatuses.push(statusCode) };
      const original = await wageringController.createTransaction(payload, idempotencyKey, response);
      await wageringController.createTransaction(
        {
          ...payload,
          externalTransactionId: randomUUID(),
          kind: WagerTransactionKind.Win,
          money: { amount: '2.00', currency: 'BRL' },
        },
        secondIdempotencyKey,
      );

      const replay = await wageringController.createTransaction(payload, idempotencyKey, response);
      expect(replay).toEqual({ ...original, idempotentReplay: true });
      expect(responseStatuses).toEqual([201, 200]);

      try {
        await wageringController.createTransaction(
          { ...payload, money: { amount: '4.00', currency: 'BRL' } },
          idempotencyKey,
        );
        throw new Error('Expected a conflict for a changed payload.');
      } catch (error) {
        expect(error).toMatchObject({ status: 409 });
      }

      const transactionRows = await pool.query(
        'SELECT count(*) AS count FROM wager_transactions WHERE wallet_id = $1',
        [wallet.id],
      );
      const ledgerRows = await pool.query(
        'SELECT count(*) AS count FROM wallet_ledger_entries WHERE wallet_id = $1',
        [wallet.id],
      );
      expect(transactionRows.rows[0].count).toBe('3');
      expect(ledgerRows.rows[0].count).toBe('3');
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [
        [idempotencyKey, secondIdempotencyKey],
      ]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

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
        result: { status: 'PROCESSED', balance: { amount: '7.00', currency: 'BRL' } },
      });

      const redelivery = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(redelivery).toEqual({ duplicate: true });

      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      expect(String(walletRow.rows[0].balance)).toBe('7.00');
      expect(ledgerRows.rowCount).toBe(2);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
        'WalletBalanceChanged',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('returns one original result for concurrent requests sharing an idempotency key', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const payload = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-idempotency-race',
      gameId: 'game-idempotency-race',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      const results = await Promise.all(
        Array.from({ length: 20 }, () => wageringController.createTransaction(payload, idempotencyKey)),
      );
      expect(results.filter((result) => !('idempotentReplay' in result && result.idempotentReplay))).toHaveLength(1);
      expect(new Set(results.map((result) => result.transactionId)).size).toBe(1);
      expect(results.every((result) => result.balance.amount === '7.00')).toBe(true);

      const counts = await pool.query(
        `SELECT
           (SELECT count(*) FROM wager_transactions WHERE wallet_id = $1) AS transaction_count,
           (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT') AS debit_count`,
        [wallet.id],
      );
      expect(counts.rows[0]).toEqual({ transaction_count: '2', debit_count: '1' });
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('processes competing bets under PostgreSQL row locks without overspending', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const requests = [randomUUID(), randomUUID()].map((suffix) => ({
      idempotencyKey: randomUUID(),
      payload: {
        providerId: 'integration-test',
        externalTransactionId: `bet-${suffix}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-wallet-race',
        gameId: 'game-wallet-race',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      },
    }));

    try {
      const responseStatuses: number[] = [];
      const results = await Promise.all(
        requests.map(({ idempotencyKey, payload }) =>
          wageringController.createTransaction(payload, idempotencyKey, {
            status: (statusCode) => responseStatuses.push(statusCode),
          }),
        ),
      );
      expect(responseStatuses.sort()).toEqual([201, 422]);
      expect(results.filter((result) => result.status === 'PROCESSED')).toHaveLength(1);
      expect(results.filter((result) => result.status === 'REJECTED')).toHaveLength(1);
      expect(results.find((result) => result.status === 'REJECTED')?.failureCode).toBe('INSUFFICIENT_FUNDS');

      const state = await pool.query(
        `SELECT
           wallet.balance,
           count(entry.id) FILTER (WHERE entry.direction = 'DEBIT') AS debit_count
         FROM wallets wallet
         LEFT JOIN wallet_ledger_entries entry ON entry.wallet_id = wallet.id
         WHERE wallet.id = $1
         GROUP BY wallet.id`,
        [wallet.id],
      );
      expect(state.rows[0]).toEqual({ balance: '20.00', debit_count: '1' });
      expect((await walletsService.reconcile(wallet.id)).consistent).toBe(true);
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [
        requests.map(({ idempotencyKey }) => idempotencyKey),
      ]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rejects a transaction when playerId does not own the wallet', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const payload = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      playerId: `another-player-${randomUUID()}`,
      walletId: wallet.id,
      roundId: 'round-owner-check',
      gameId: 'game-owner-check',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      await expect(wageringController.createTransaction(payload, randomUUID())).rejects.toMatchObject({
        status: 400,
      });
      expect((await walletsService.getWallet(wallet.id)).balance.amount).toBe('10.00');
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('persists a currency mismatch as a rejected transaction without moving the balance', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const payload = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-currency-check',
      gameId: 'game-currency-check',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'USD' },
    };

    try {
      const result = await wageringController.createTransaction(payload, idempotencyKey);
      expect(result).toMatchObject({
        status: 'REJECTED',
        failureCode: 'CURRENCY_MISMATCH',
        balance: { amount: '10.00', currency: 'BRL' },
      });
      const ledger = await pool.query(
        `SELECT direction FROM wallet_ledger_entries
         WHERE wallet_id = $1 AND transaction_id IN (
           SELECT id FROM wager_transactions WHERE external_transaction_id = $2
         )`,
        [wallet.id, payload.externalTransactionId],
      );
      expect(ledger.rowCount).toBe(0);
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('audits a rollback rejected because it would overdraw the wallet', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const winExternalId = randomUUID();
    const betExternalId = randomUUID();
    const rollbackExternalId = randomUUID();
    const idempotencyKeys = [randomUUID(), randomUUID(), randomUUID()];
    const base = {
      providerId: 'integration-test',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-rollback-overdraw',
      gameId: 'game-rollback-overdraw',
    };

    try {
      await wageringController.createTransaction(
        {
          ...base,
          externalTransactionId: winExternalId,
          kind: WagerTransactionKind.Win,
          money: { amount: '80.00', currency: 'BRL' },
        },
        idempotencyKeys[0],
      );
      await wageringController.createTransaction(
        {
          ...base,
          externalTransactionId: betExternalId,
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        },
        idempotencyKeys[1],
      );
      const rollback = await wageringController.createTransaction(
        {
          ...base,
          externalTransactionId: rollbackExternalId,
          kind: WagerTransactionKind.Rollback,
          referenceExternalTransactionId: winExternalId,
          money: { amount: '80.00', currency: 'BRL' },
        },
        idempotencyKeys[2],
      );

      expect(rollback).toMatchObject({
        status: 'REJECTED',
        failureCode: 'REVERSAL_INSUFFICIENT_FUNDS',
        balance: { amount: '0.00', currency: 'BRL' },
      });
      const transaction = await pool.query(
        'SELECT status, failure_code FROM wager_transactions WHERE external_transaction_id = $1',
        [rollbackExternalId],
      );
      expect(transaction.rows[0]).toEqual({
        status: 'REJECTED',
        failure_code: 'REVERSAL_INSUFFICIENT_FUNDS',
      });
      expect((await walletsService.reconcile(wallet.id)).consistent).toBe(true);
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [idempotencyKeys]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('returns HTTP accepted for a transaction waiting on its reference', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const idempotencyKey = randomUUID();
    const responseStatuses: number[] = [];

    try {
      const result = await wageringController.createTransaction(
        {
          providerId: 'integration-test',
          externalTransactionId: randomUUID(),
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-pending-http',
          gameId: 'game-pending-http',
          kind: WagerTransactionKind.Refund,
          money: { amount: '1.00', currency: 'BRL' },
          referenceExternalTransactionId: randomUUID(),
        },
        idempotencyKey,
        { status: (statusCode) => responseStatuses.push(statusCode) },
      );
      expect(result.status).toBe('PENDING_REFERENCE');
      expect(responseStatuses).toEqual([202]);
    } finally {
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('suppresses 50 concurrent redeliveries of the same message', async () => {
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
      const results = await Promise.all(
        Array.from({ length: 50 }, () =>
          wageringService.processQueueTransaction(request, messageId, consumerName),
        ),
      );
      const accepted = results.filter((result) => 'duplicate' in result && !result.duplicate);
      const duplicates = results.filter((result) => 'duplicate' in result && result.duplicate);

      expect(accepted).toHaveLength(1);
      expect(duplicates).toHaveLength(49);

      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      const inboxRows = await pool.query(
        'SELECT count(*) AS count FROM message_inbox WHERE consumer_name = $1 AND message_id = $2',
        [consumerName, messageId],
      );
      const transactionRows = await pool.query(
        'SELECT count(*) AS count FROM wager_transactions WHERE wallet_id = $1 AND external_transaction_id = $2',
        [wallet.id, externalTransactionId],
      );
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );

      expect(String(walletRow.rows[0].balance)).toBe('7.00');
      expect(inboxRows.rows[0].count).toBe('1');
      expect(transactionRows.rows[0].count).toBe('1');
      expect(ledgerRows.rowCount).toBe(2);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
        'WalletBalanceChanged',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
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
      const outboxRows = await pool.query('SELECT event_id FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      const openingLedger = await pool.query(
        `SELECT transaction.kind, entry.direction
         FROM wallet_ledger_entries entry
         JOIN wager_transactions transaction ON transaction.id = entry.transaction_id
         WHERE entry.wallet_id = $1`,
        [wallet.id],
      );
      expect(inboxRow.rowCount).toBe(0);
      expect(String(walletRow.rows[0].balance)).toBe('10.00');
      expect(outboxRows.rowCount).toBe(2);
      expect(openingLedger.rows).toEqual([{ kind: 'OPENING', direction: 'CREDIT' }]);
    } finally {
      await deleteLedgerEntriesForTest(wallet.id);
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

  test('does not allow external OPENING transactions', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const idempotencyKey = randomUUID();
    const externalTransactionId = randomUUID();

    try {
      const result = await wageringService.processQueueTransaction(
        {
          providerId: 'integration-test',
          externalTransactionId,
          idempotencyKey,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Opening,
          money: { amount: '5.00', currency: 'BRL' },
        },
        messageId,
        consumerName,
      );
      expect(result).toEqual({ duplicate: false, result: { terminalFailure: true } });

      const records = await pool.query(
        `SELECT
           (SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $1) AS transactions,
           (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $2) AS entries`,
        [externalTransactionId, wallet.id],
      );
      expect(records.rows[0]).toMatchObject({ transactions: '0', entries: '0' });
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('writes a processed event for LOSS without a balance-change event', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const idempotencyKey = randomUUID();
    const request = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      idempotencyKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Loss,
      money: { amount: '3.00', currency: 'BRL' },
    };

    try {
      const result = await wageringService.processQueueTransaction(request, messageId, consumerName);
      expect(result).toMatchObject({
        duplicate: false,
        result: { status: 'PROCESSED', balance: { amount: '10.00', currency: 'BRL' } },
      });

      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
      ]);
      expect(ledgerRows.rowCount).toBe(1);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('writes rejection and pending-reference events without balance-change events', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const rejectionMessageId = randomUUID();
    const pendingMessageId = randomUUID();
    const rejectionKey = randomUUID();
    const pendingKey = randomUUID();
    const rejectionExternalId = randomUUID();
    const pendingExternalId = randomUUID();

    try {
      const rejected = await wageringService.processQueueTransaction(
        {
          providerId: 'integration-test',
          externalTransactionId: rejectionExternalId,
          idempotencyKey: rejectionKey,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '11.00', currency: 'BRL' },
        },
        rejectionMessageId,
        consumerName,
      );
      const pending = await wageringService.processQueueTransaction(
        {
          providerId: 'integration-test',
          externalTransactionId: pendingExternalId,
          idempotencyKey: pendingKey,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Refund,
          money: { amount: '1.00', currency: 'BRL' },
          referenceExternalTransactionId: 'missing-reference',
        },
        pendingMessageId,
        consumerName,
      );

      expect(rejected).toMatchObject({
        duplicate: false,
        result: { status: 'REJECTED', balance: { amount: '10.00', currency: 'BRL' } },
      });
      expect(pending).toMatchObject({
        duplicate: false,
        result: { status: 'PENDING_REFERENCE', balance: { amount: '10.00', currency: 'BRL' } },
      });

      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionPendingReference',
        'WagerTransactionProcessed',
        'WagerTransactionRejected',
        'WalletBalanceChanged',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = ANY($2::varchar[])', [
        consumerName,
        [rejectionMessageId, pendingMessageId],
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [[rejectionKey, pendingKey]]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('reprocesses a pending refund when its bet reference arrives', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const betMessageId = randomUUID();
    const refundMessageId = randomUUID();
    const betExternalId = randomUUID();
    const refundExternalId = randomUUID();
    const betKey = randomUUID();
    const refundKey = randomUUID();
    const betRequest = {
      providerId: 'integration-test',
      externalTransactionId: betExternalId,
      idempotencyKey: betKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };
    const refundRequest = {
      ...betRequest,
      externalTransactionId: refundExternalId,
      idempotencyKey: refundKey,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: betExternalId,
    };

    try {
      const pending = await wageringService.processQueueTransaction(
        refundRequest,
        refundMessageId,
        consumerName,
      );
      expect(pending).toMatchObject({
        duplicate: false,
        result: {
          status: WagerTransactionStatus.PendingReference,
          balance: { amount: '10.00', currency: 'BRL' },
        },
      });

      const pendingRow = await pool.query(
        'SELECT id, reference_attempts FROM wager_transactions WHERE external_transaction_id = $1',
        [refundExternalId],
      );
      expect(pendingRow.rows[0].reference_attempts).toBe(1);

      const bet = await wageringService.processQueueTransaction(betRequest, betMessageId, consumerName);
      expect(bet).toMatchObject({
        duplicate: false,
        result: { status: 'PROCESSED', balance: { amount: '7.00', currency: 'BRL' } },
      });

      await pool.query(
        'UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1',
        [pendingRow.rows[0].id],
      );
      const concurrentResults = await Promise.all([
        pendingReferenceWorker.runOnce(),
        pendingReferenceWorker.runOnce(),
      ]);
      expect(concurrentResults.filter(Boolean)).toHaveLength(1);

      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      const refundRow = await pool.query(
        'SELECT status, reference_transaction_id FROM wager_transactions WHERE external_transaction_id = $1',
        [refundExternalId],
      );
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_sequence',
        [wallet.id],
      );

      expect(String(walletRow.rows[0].balance)).toBe('10.00');
      expect(refundRow.rows[0].status).toBe(WagerTransactionStatus.Processed);
      expect(refundRow.rows[0].reference_transaction_id).toBe(
        (await pool.query('SELECT id FROM wager_transactions WHERE external_transaction_id = $1', [betExternalId]))
          .rows[0].id,
      );
      expect(ledgerRows.rowCount).toBe(3);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
        'WagerTransactionPendingReference',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = ANY($2::varchar[])', [
        consumerName,
        [betMessageId, refundMessageId],
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [[betKey, refundKey]]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('allows only one pending reversal to process for a reference', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const betMessageId = randomUUID();
    const betExternalId = randomUUID();
    const betKey = randomUUID();
    const reversals = [0, 1].map(() => ({
      messageId: randomUUID(),
      externalTransactionId: randomUUID(),
      idempotencyKey: randomUUID(),
    }));
    const betRequest = {
      providerId: 'integration-test',
      externalTransactionId: betExternalId,
      idempotencyKey: betKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: { amount: '3.00', currency: 'BRL' },
    };
    const refundRequests = reversals.map((reversal) => ({
      ...betRequest,
      externalTransactionId: reversal.externalTransactionId,
      idempotencyKey: reversal.idempotencyKey,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: betExternalId,
    }));

    try {
      for (let index = 0; index < refundRequests.length; index += 1) {
        const result = await wageringService.processQueueTransaction(
          refundRequests[index],
          reversals[index].messageId,
          consumerName,
        );
        expect(result).toMatchObject({ result: { status: WagerTransactionStatus.PendingReference } });
      }
      await wageringService.processQueueTransaction(betRequest, betMessageId, consumerName);

      const pending = await pool.query(
        `SELECT id, external_transaction_id FROM wager_transactions
         WHERE external_transaction_id = ANY($1::varchar[])`,
        [reversals.map((reversal) => reversal.externalTransactionId)],
      );
      const firstReversal = pending.rows.find(
        (row) => row.external_transaction_id === reversals[0].externalTransactionId,
      );
      const secondReversal = pending.rows.find(
        (row) => row.external_transaction_id === reversals[1].externalTransactionId,
      );
      await pool.query(
        `UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1`,
        [firstReversal.id],
      );
      await pool.query(
        `UPDATE wager_transactions
         SET reference_next_attempt_at = CURRENT_TIMESTAMP + INTERVAL '1 hour'
         WHERE id = $1`,
        [secondReversal.id],
      );

      expect(await pendingReferenceWorker.runOnce()).toBe(true);
      await pool.query(
        'UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1',
        [secondReversal.id],
      );
      expect(await pendingReferenceWorker.runOnce()).toBe(true);

      const result = await pool.query(
        `SELECT external_transaction_id, status, failure_code
         FROM wager_transactions WHERE id = ANY($1::varchar[])`,
        [[firstReversal.id, secondReversal.id]],
      );
      const walletRow = await pool.query('SELECT balance FROM wallets WHERE id = $1', [wallet.id]);
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      expect(result.rows.find((row) => row.external_transaction_id === reversals[0].externalTransactionId)).toMatchObject({
        status: WagerTransactionStatus.Processed,
        failure_code: null,
      });
      expect(result.rows.find((row) => row.external_transaction_id === reversals[1].externalTransactionId)).toMatchObject({
        status: WagerTransactionStatus.Rejected,
        failure_code: 'REFERENCE_ALREADY_REVERSED',
      });
      expect(String(walletRow.rows[0].balance)).toBe('10.00');
      expect(ledgerRows.rowCount).toBe(3);
    } finally {
      await pool.query(
        'DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = ANY($2::varchar[])',
        [consumerName, [betMessageId, ...reversals.map((reversal) => reversal.messageId)]],
      );
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = ANY($1::varchar[])', [
        [betKey, ...reversals.map((reversal) => reversal.idempotencyKey)],
      ]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rejects a pending reversal after the configured attempts are exhausted', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const idempotencyKey = randomUUID();
    const request = {
      providerId: 'integration-test',
      externalTransactionId: randomUUID(),
      idempotencyKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Refund,
      money: { amount: '1.00', currency: 'BRL' },
      referenceExternalTransactionId: randomUUID(),
    };

    try {
      await wageringService.processQueueTransaction(request, messageId, consumerName);
      const row = await pool.query(
        'SELECT id FROM wager_transactions WHERE external_transaction_id = $1',
        [request.externalTransactionId],
      );
      await pool.query(
        'UPDATE wager_transactions SET reference_next_attempt_at = CURRENT_TIMESTAMP WHERE id = $1',
        [row.rows[0].id],
      );
      expect(await pendingReferenceWorker.runOnce()).toBe(true);
      const rescheduled = await pool.query(
        'SELECT reference_attempts, reference_next_attempt_at FROM wager_transactions WHERE id = $1',
        [row.rows[0].id],
      );
      expect(rescheduled.rows[0].reference_attempts).toBe(2);
      expect(new Date(rescheduled.rows[0].reference_next_attempt_at).getTime()).toBeGreaterThan(Date.now());

      await pool.query(
        `UPDATE wager_transactions
         SET reference_attempts = 10, reference_next_attempt_at = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [row.rows[0].id],
      );
      expect(await pendingReferenceWorker.runOnce()).toBe(true);

      const transaction = await pool.query(
        'SELECT status, failure_code FROM wager_transactions WHERE id = $1',
        [row.rows[0].id],
      );
      const event = await pool.query(
        `SELECT payload->'data'->>'failureCode' AS failure_code
         FROM outbox_events
         WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'`,
        [wallet.id],
      );
      expect(transaction.rows[0]).toMatchObject({
        status: WagerTransactionStatus.Rejected,
        failure_code: 'REFERENCE_NOT_FOUND',
      });
      expect(event.rows[0].failure_code).toBe('REFERENCE_NOT_FOUND');
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });

  test('rejects a pending reversal after its reference TTL expires', async () => {
    const wallet = await walletsService.createWallet({
      playerId: `player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const idempotencyKey = randomUUID();
    const externalTransactionId = randomUUID();
    const request = {
      providerId: 'integration-test',
      externalTransactionId,
      idempotencyKey,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Refund,
      money: { amount: '1.00', currency: 'BRL' },
      referenceExternalTransactionId: randomUUID(),
    };

    try {
      await wageringService.processQueueTransaction(request, messageId, consumerName);
      const pending = await pool.query(
        'SELECT id FROM wager_transactions WHERE external_transaction_id = $1',
        [externalTransactionId],
      );
      await pool.query(
        `UPDATE wager_transactions
         SET reference_next_attempt_at = CURRENT_TIMESTAMP,
             reference_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
         WHERE id = $1`,
        [pending.rows[0].id],
      );

      expect(await pendingReferenceWorker.runOnce()).toBe(true);

      const transaction = await pool.query(
        'SELECT status, failure_code FROM wager_transactions WHERE id = $1',
        [pending.rows[0].id],
      );
      expect(transaction.rows[0]).toMatchObject({
        status: WagerTransactionStatus.Rejected,
        failure_code: 'REFERENCE_NOT_FOUND',
      });
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await deleteLedgerEntriesForTest(wallet.id);
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });
});
