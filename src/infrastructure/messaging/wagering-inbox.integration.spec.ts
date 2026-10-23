import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { WagerTransactionKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { PendingReferenceWorkerService } from './pending-reference-worker.service';
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
      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      expect(String(walletRow.rows[0].balance)).toBe('7.00');
      expect(ledgerRows.rowCount).toBe(1);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
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
      const outboxRows = await pool.query('SELECT event_id FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      expect(inboxRow.rowCount).toBe(0);
      expect(String(walletRow.rows[0].balance)).toBe('10.00');
      expect(outboxRows.rowCount).toBe(0);
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
        result: { status: 'PROCESSED', balance: '10.00' },
      });

      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      const ledgerRows = await pool.query('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual(['WagerTransactionProcessed']);
      expect(ledgerRows.rowCount).toBe(0);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
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
        result: { status: 'REJECTED', balance: '10.00' },
      });
      expect(pending).toMatchObject({
        duplicate: false,
        result: { status: 'PENDING_REFERENCE', balance: '10.00' },
      });

      const outboxRows = await pool.query(
        'SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY event_type',
        [wallet.id],
      );
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
        'WagerTransactionPendingReference',
        'WagerTransactionRejected',
      ]);
    } finally {
      await pool.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = ANY($2::varchar[])', [
        consumerName,
        [rejectionMessageId, pendingMessageId],
      ]);
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
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
        result: { status: WagerTransactionStatus.PendingReference, balance: '10.00' },
      });

      const pendingRow = await pool.query(
        'SELECT id, reference_attempts FROM wager_transactions WHERE external_transaction_id = $1',
        [refundExternalId],
      );
      expect(pendingRow.rows[0].reference_attempts).toBe(1);

      const bet = await wageringService.processQueueTransaction(betRequest, betMessageId, consumerName);
      expect(bet).toMatchObject({ duplicate: false, result: { status: 'PROCESSED', balance: '7.00' } });

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
      expect(ledgerRows.rowCount).toBe(2);
      expect(outboxRows.rows.map((row) => row.event_type)).toEqual([
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
      await pool.query('DELETE FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
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
      expect(ledgerRows.rowCount).toBe(2);
    } finally {
      await pool.query(
        'DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = ANY($2::varchar[])',
        [consumerName, [betMessageId, ...reversals.map((reversal) => reversal.messageId)]],
      );
      await pool.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
      await pool.query('DELETE FROM wallet_ledger_entries WHERE wallet_id = $1', [wallet.id]);
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
      await pool.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
      await pool.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
      await pool.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
    }
  });
});
