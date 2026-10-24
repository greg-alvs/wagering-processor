import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { AddressInfo } from 'node:net';
import { ChildProcess, spawn } from 'node:child_process';
import { createServer, Server, request as httpRequest } from 'node:http';
import { Socket } from 'node:net';
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Pool } from 'pg';

const runId = randomUUID().replaceAll('-', '');
const localStackEndpoint =
  process.env.SQS_ENDPOINT ??
  `http://${process.env.LOCALSTACK_HOST ?? 'localhost'}:${process.env.LOCALSTACK_PORT ?? 4566}`;
const region = process.env.AWS_REGION ?? 'us-east-1';
const credentials = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
};
const sqs = new SQSClient({ endpoint: localStackEndpoint, region, credentials });
const database = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const runtimeDatabase = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres',
  password:
    process.env.POSTGRES_APP_PASSWORD ??
    process.env.POSTGRES_PASSWORD ??
    'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});

interface InterceptedRequest {
  receiptHandle?: string;
}

interface SqsProxy {
  endpoint: string;
  intercepted: Promise<InterceptedRequest>;
  close(): Promise<void>;
}

interface RunningWorker {
  child: ChildProcess;
  output: string;
}

function createSqsProxy(actionToIntercept: string, forwardBeforeIntercept: boolean): Promise<SqsProxy> {
  return new Promise((resolve, reject) => {
    let resolveIntercepted: (request: InterceptedRequest) => void;
    const intercepted = new Promise<InterceptedRequest>((complete) => {
      resolveIntercepted = complete;
    });
    const sockets = new Set<Socket>();
    const upstreamRequests = new Set<ReturnType<typeof httpRequest>>();
    let interceptedOnce = false;
    let closed = false;
    let proxyEndpoint: string;

    const server: Server = createServer((incoming, outgoing) => {
      const chunks: Buffer[] = [];
      incoming.on('data', (chunk: Buffer) => chunks.push(chunk));
      incoming.on('end', () => {
        const body = Buffer.concat(chunks);
        const targetHeader = incoming.headers['x-amz-target'];
        const action = typeof targetHeader === 'string' ? targetHeader.split('.').at(-1) : undefined;
        let requestPayload: Record<string, unknown> = {};
        try {
          requestPayload = JSON.parse(body.toString()) as Record<string, unknown>;
        } catch {
          requestPayload = {};
        }
        const shouldIntercept = action === actionToIntercept && !interceptedOnce;

        if (shouldIntercept && !forwardBeforeIntercept) {
          interceptedOnce = true;
          resolveIntercepted(
            typeof requestPayload.ReceiptHandle === 'string'
              ? { receiptHandle: requestPayload.ReceiptHandle }
              : {},
          );
          return;
        }

        const target = new URL(localStackEndpoint);
        const upstream = httpRequest(
          {
            hostname: target.hostname,
            port: target.port,
            path: incoming.url,
            method: incoming.method,
            headers: { ...incoming.headers, host: target.host },
          },
          (response) => {
            const responseChunks: Buffer[] = [];
            response.on('data', (chunk: Buffer) => responseChunks.push(chunk));
            response.on('end', () => {
              let responseBody = Buffer.concat(responseChunks);
              if (action === 'GetQueueUrl') {
                const queueResponse = JSON.parse(responseBody.toString()) as { QueueUrl?: string };
                if (queueResponse.QueueUrl) {
                  queueResponse.QueueUrl = `${proxyEndpoint}${new URL(queueResponse.QueueUrl).pathname}`;
                  responseBody = Buffer.from(JSON.stringify(queueResponse));
                }
              }
              if (shouldIntercept && response.statusCode === 200) {
                interceptedOnce = true;
                resolveIntercepted({});
                return;
              }
              const headers = { ...response.headers };
              if (action === 'GetQueueUrl') {
                headers['content-length'] = String(responseBody.length);
              }
              outgoing.writeHead(response.statusCode ?? 502, headers);
              outgoing.end(responseBody);
            });
          },
        );
        upstreamRequests.add(upstream);
        upstream.on('close', () => upstreamRequests.delete(upstream));
        upstream.on('error', () => {
          if (!outgoing.destroyed) {
            outgoing.writeHead(502);
            outgoing.end();
          }
        });
        upstream.end(body);
      });
    });

    server.on('connection', (socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });

    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address() as AddressInfo;
      proxyEndpoint = `http://127.0.0.1:${address.port}`;
      resolve({
        endpoint: proxyEndpoint,
        intercepted,
        close: () => {
          if (closed) {
            return Promise.resolve();
          }
          closed = true;
          return new Promise<void>((complete, fail) => {
            for (const upstream of upstreamRequests) {
              upstream.destroy();
            }
            for (const socket of sockets) {
              socket.destroy();
            }
            server.close((error) => (error ? fail(error) : complete()));
          });
        },
      });
    });
  });
}

function workerEnvironment(
  queueName: string,
  endpoint: string,
  consumerName?: string,
  pgOptions?: string,
): NodeJS.ProcessEnv {
  return {
    ...process.env,
    AWS_REGION: region,
    AWS_ACCESS_KEY_ID: credentials.accessKeyId,
    AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
    SQS_ENDPOINT: endpoint,
    SQS_MAIN_QUEUE_NAME: queueName,
    SQS_DLQ_QUEUE_NAME: `${queueName.replace('.fifo', '-dlq.fifo')}`,
    SQS_EVENTS_QUEUE_NAME: queueName,
    SQS_CONSUMER_NAME: consumerName ?? 'crash-recovery-consumer',
    SQS_WAIT_TIME_SECONDS: '1',
    ...(pgOptions ? { PGOPTIONS: pgOptions } : {}),
  };
}

const consumerWorkerScript = `
import { Pool } from 'pg';
import { WageringService } from './src/wagering/wagering.service.ts';
import { SqsConsumerService } from './src/infrastructure/messaging/sqs-consumer.service.ts';
const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_APP_PASSWORD ?? process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const values = process.env;
const config = { get: (key, fallback) => values[key] ?? fallback };
const consumer = new SqsConsumerService(config, new WageringService(pool));
await consumer.onModuleInit();
console.log('crash-worker-ready');
process.on('SIGTERM', async () => {
  await consumer.onModuleDestroy();
  await pool.end();
  process.exit(0);
});
`;

const publisherWorkerScript = `
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { OutboxPublisherService } from './src/infrastructure/outbox/outbox-publisher.service.ts';
const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_APP_PASSWORD ?? process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const values = process.env;
const config = { get: (key, fallback) => values[key] ?? fallback };
const publisher = new OutboxPublisherService(config, pool);
await publisher.onModuleInit();
console.log('crash-worker-ready');
process.on('SIGTERM', async () => {
  await publisher.onModuleDestroy();
  await pool.end();
  process.exit(0);
});
`;

function startWorker(script: string, env: NodeJS.ProcessEnv): RunningWorker {
  const child = spawn(process.execPath, ['-e', script], {
    cwd: process.cwd(),
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const worker: RunningWorker = { child, output: '' };
  child.stdout?.on('data', (chunk: Buffer) => (worker.output += chunk.toString()));
  child.stderr?.on('data', (chunk: Buffer) => (worker.output += chunk.toString()));
  return worker;
}

async function waitForWorkerReady(worker: RunningWorker): Promise<void> {
  await waitUntil(
    'worker process to initialize',
    async () => {
      if (worker.output.includes('crash-worker-ready')) {
        return true;
      }
      if (worker.child.exitCode !== null) {
        throw new Error(`Worker exited before readiness: ${worker.output}`);
      }
      return false;
    },
  );
}

async function crashWorker(worker: RunningWorker): Promise<void> {
  if (worker.child.exitCode !== null) {
    return;
  }
  const exited = new Promise<void>((resolve, reject) => {
    worker.child.once('exit', (code, signal) => {
      if (signal === null && code === 0) {
        reject(new Error(`Worker did not crash as expected (code=${code}, signal=${signal}).`));
      } else {
        resolve();
      }
    });
  });
  worker.child.kill('SIGKILL');
  await exited;
}

async function stopWorker(worker: RunningWorker): Promise<void> {
  if (worker.child.exitCode !== null) {
    return;
  }
  const exited = new Promise<void>((resolve, reject) => {
    worker.child.once('exit', (code, signal) => {
      if (code !== 0 && signal !== 'SIGTERM') {
        reject(new Error(`Worker failed to stop cleanly (code=${code}): ${worker.output}`));
      } else {
        resolve();
      }
    });
  });
  worker.child.kill('SIGTERM');
  await exited;
}

async function deleteLedgerEntriesForTest(walletId: string): Promise<void> {
  const client = await database.connect();
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

async function waitUntil(
  description: string,
  condition: () => Promise<boolean>,
  timeoutMilliseconds = 30000,
): Promise<void> {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline) {
    if (await condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ${description}.`);
}

async function createQueue(name: string, visibilityTimeout?: number): Promise<string> {
  const attributes: Record<string, string> = {
    FifoQueue: 'true',
    ContentBasedDeduplication: 'false',
  };
  if (visibilityTimeout !== undefined) {
    attributes.VisibilityTimeout = String(visibilityTimeout);
  }
  const result = await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }));
  if (!result.QueueUrl) {
    throw new Error(`LocalStack did not return a queue URL for ${name}.`);
  }
  return result.QueueUrl;
}

async function createIsolatedOutboxSchema(schema: string): Promise<void> {
  const runtimeRole = process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres';
  await database.query(`CREATE SCHEMA ${schema}`);
  await database.query(`
    CREATE TABLE ${schema}.outbox_events (
      event_id varchar(36) PRIMARY KEY,
      aggregate_id varchar(128) NOT NULL,
      event_type varchar(128) NOT NULL,
      payload jsonb NOT NULL,
      occurred_at timestamptz NOT NULL,
      created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
      published_at timestamptz,
      attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      next_attempt_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
      event_sequence bigint GENERATED BY DEFAULT AS IDENTITY
    )
  `);
  await database.query(`GRANT USAGE ON SCHEMA ${schema} TO ${runtimeRole}`);
  await database.query(`GRANT SELECT, UPDATE ON ${schema}.outbox_events TO ${runtimeRole}`);
}

async function insertOutboxEvent(schema: string, eventId: string, aggregateId: string): Promise<void> {
  const payload = {
    eventId,
    eventType: 'WorkerCrashRecoveryIntegrationTest',
    aggregateId,
    occurredAt: new Date().toISOString(),
    data: { testRunId: runId },
  };
  await database.query(
    `INSERT INTO ${schema}.outbox_events (event_id, aggregate_id, event_type, payload, occurred_at)
     VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP)`,
    [eventId, aggregateId, 'WorkerCrashRecoveryIntegrationTest', JSON.stringify(payload)],
  );
}

const describeLocalStack = process.env.RUN_LOCALSTACK_INTEGRATION ? describe : describe.skip;

describeLocalStack('Worker crash recovery with LocalStack and PostgreSQL', () => {
  afterAll(async () => {
    sqs.destroy();
    await runtimeDatabase.end();
    await database.end();
  });

  test(
    'redelivers after a real consumer process crash between database commit and SQS ACK',
    async () => {
      const queueName = `wager-crash-consumer-${runId}.fifo`;
      const dlqName = queueName.replace('.fifo', '-dlq.fifo');
      const queueUrl = await createQueue(queueName, 30);
      const dlqUrl = await createQueue(dlqName);
      const walletId = randomUUID();
      const playerId = `crash-consumer-${runId}`;
      const consumerName = `crash-recovery-${runId}`;
      const messageId = randomUUID();
      const externalTransactionId = randomUUID();
      const idempotencyKey = randomUUID();
      const proxy = await createSqsProxy('DeleteMessage', false);
      let worker: RunningWorker | undefined;
      let recoveryWorker: RunningWorker | undefined;
      let walletCreated = false;
      let queueMessageId: string | undefined;

      try {
        const wallet = await runtimeDatabase.query(
          `INSERT INTO wallets (id, player_id, currency, balance, opening_balance, version)
           VALUES ($1, $2, 'BRL', 10.00, 10.00, 1)
           RETURNING id`,
          [walletId, playerId],
        );
        walletCreated = wallet.rowCount === 1;

        worker = startWorker(consumerWorkerScript, workerEnvironment(queueName, proxy.endpoint, consumerName));
        await waitForWorkerReady(worker);

        const envelope = {
          messageId,
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            providerId: 'crash-recovery-test',
            externalTransactionId,
            idempotencyKey,
            walletId,
            playerId,
            roundId: 'crash-recovery-round',
            gameId: 'crash-recovery-game',
            kind: 'BET',
            money: { amount: '3.00', currency: 'BRL' },
          },
        };
        const sent = await sqs.send(
          new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(envelope),
            MessageGroupId: walletId,
            MessageDeduplicationId: messageId,
          }),
        );
        queueMessageId = sent.MessageId;
        if (!queueMessageId) {
          throw new Error('LocalStack did not return the SQS message ID.');
        }

        const intercepted = await Promise.race([
          proxy.intercepted,
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`Consumer did not reach ACK: ${worker?.output}`)), 30000),
          ),
        ]);
        const committed = await database.query(
          `SELECT
             (SELECT balance FROM wallets WHERE id = $1) AS balance,
             (SELECT count(*) FROM message_inbox WHERE consumer_name = $2 AND message_id = $3) AS inbox_count,
             (SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $4) AS transaction_count,
             (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1) AS ledger_count`,
          [walletId, consumerName, queueMessageId, externalTransactionId],
        );
        expect(committed.rows[0]).toEqual({
          balance: '7.00',
          inbox_count: '1',
          transaction_count: '1',
          ledger_count: '1',
        });
        expect(intercepted.receiptHandle).toBeDefined();

        await crashWorker(worker);
        worker = undefined;
        await sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: intercepted.receiptHandle,
            VisibilityTimeout: 0,
          }),
        );
        await proxy.close();

        recoveryWorker = startWorker(consumerWorkerScript, workerEnvironment(queueName, localStackEndpoint, consumerName));
        await waitForWorkerReady(recoveryWorker);
        await waitUntil('redelivered message to be acknowledged after inbox deduplication', async () => {
          const counts = await sqs.send(
            new GetQueueAttributesCommand({
              QueueUrl: queueUrl,
              AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
            }),
          );
          return (
            Number(counts.Attributes?.ApproximateNumberOfMessages ?? 0) === 0 &&
            Number(counts.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0) === 0
          );
        });
        const afterRecovery = await database.query(
          `SELECT
             (SELECT balance FROM wallets WHERE id = $1) AS balance,
             (SELECT count(*) FROM message_inbox WHERE consumer_name = $2 AND message_id = $3) AS inbox_count,
             (SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $4) AS transaction_count,
             (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1) AS ledger_count`,
          [walletId, consumerName, queueMessageId, externalTransactionId],
        );
        expect(afterRecovery.rows[0]).toEqual({
          balance: '7.00',
          inbox_count: '1',
          transaction_count: '1',
          ledger_count: '1',
        });
      } finally {
        if (worker) {
          await crashWorker(worker);
        }
        if (recoveryWorker) {
          await stopWorker(recoveryWorker);
        }
        await proxy.close();
        if (walletCreated) {
          await database.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
            consumerName,
            queueMessageId,
          ]);
          await database.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [walletId]);
          await deleteLedgerEntriesForTest(walletId);
          await database.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [walletId]);
          await database.query('DELETE FROM idempotency_keys WHERE key = $1', [idempotencyKey]);
          await database.query('DELETE FROM wallets WHERE id = $1', [walletId]);
        }
        await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
        await sqs.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
      }
    },
    90000,
  );

  test(
    'retries an outbox event after a real publisher process crash following SQS acceptance',
    async () => {
      const queueName = `wager-crash-outbox-${runId}.fifo`;
      const queueUrl = await createQueue(queueName);
      const eventId = randomUUID();
      const aggregateId = `crash-outbox-${randomUUID()}`;
      const schema = `crash_outbox_${runId}`;
      const proxy = await createSqsProxy('SendMessage', true);
      let worker: RunningWorker | undefined;
      let recoveryWorker: RunningWorker | undefined;
      let schemaCreated = false;

      try {
        await createIsolatedOutboxSchema(schema);
        schemaCreated = true;
        await insertOutboxEvent(schema, eventId, aggregateId);
        const pgOptions = `-c search_path=${schema}`;
        worker = startWorker(
          publisherWorkerScript,
          workerEnvironment(queueName, proxy.endpoint, undefined, pgOptions),
        );
        await waitForWorkerReady(worker);

        await Promise.race([
          proxy.intercepted,
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`Publisher did not send the event: ${worker?.output}`)), 30000),
          ),
        ]);
        const unpublished = await database.query(
          `SELECT published_at, attempts FROM ${schema}.outbox_events WHERE event_id = $1`,
          [eventId],
        );
        expect(unpublished.rows[0]).toEqual({ published_at: null, attempts: 0 });

        await crashWorker(worker);
        worker = undefined;
        await proxy.close();
        await waitUntil('crashed publisher transaction to roll back', async () => {
          const result = await database.query(
            `SELECT published_at, attempts FROM ${schema}.outbox_events WHERE event_id = $1`,
            [eventId],
          );
          return result.rowCount === 1 && result.rows[0].published_at === null;
        });

        recoveryWorker = startWorker(
          publisherWorkerScript,
          workerEnvironment(queueName, localStackEndpoint, undefined, pgOptions),
        );
        await waitForWorkerReady(recoveryWorker);
        await waitUntil('restarted publisher to commit published_at', async () => {
          const result = await database.query(
            `SELECT published_at, attempts FROM ${schema}.outbox_events WHERE event_id = $1`,
            [eventId],
          );
          return (
            result.rowCount === 1 &&
            result.rows[0].published_at !== null &&
            result.rows[0].attempts === 1
          );
        });

        const delivered = await sqs.send(
          new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 2 }),
        );
        expect(delivered.Messages).toHaveLength(1);
        expect(JSON.parse(delivered.Messages?.[0].Body ?? '{}')).toMatchObject({ eventId });
        if (delivered.Messages?.[0].ReceiptHandle) {
          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: delivered.Messages[0].ReceiptHandle,
            }),
          );
        }
        const afterReceive = await sqs.send(
          new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 2 }),
        );
        expect(afterReceive.Messages ?? []).toHaveLength(0);
      } finally {
        if (worker) {
          await crashWorker(worker);
        }
        if (recoveryWorker) {
          await stopWorker(recoveryWorker);
        }
        await proxy.close();
        if (schemaCreated) {
          await database.query(`DROP SCHEMA ${schema} CASCADE`);
        }
        await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
      }
    },
    90000,
  );
});
