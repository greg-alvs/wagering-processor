import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';
import { ApplicationMetrics } from '../observability/application-metrics';
import { WageringService } from '../../wagering/wagering.service';
import { WalletsService } from '../../wallets/wallets.service';
import { SqsConsumerService } from './sqs-consumer.service';

const runId = randomUUID().replaceAll('-', '');
const queueNames = [0, 1, 2].map((index) => `wager-it-${runId}-${index}.fifo`);
const dlqName = `wager-it-dlq-${runId}.fifo`;
const consumerNames = [0, 1, 2].map((index) => `wager-it-${runId}-${index}`);
const endpoint =
  process.env.SQS_ENDPOINT ??
  `http://${process.env.LOCALSTACK_HOST ?? 'localhost'}:${process.env.LOCALSTACK_PORT ?? 4566}`;
const region = process.env.AWS_REGION ?? 'us-east-1';
const credentials = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
};
const sqs = new SQSClient({ endpoint, region, credentials });
const database = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const applicationDatabase = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres',
  password:
    process.env.POSTGRES_APP_PASSWORD ??
    process.env.POSTGRES_PASSWORD ??
    'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const walletsService = new WalletsService(applicationDatabase);
const queueUrls: string[] = [];
let dlqUrl: string;
let consumers: SqsConsumerService[] = [];

function metricValue(name: string): number {
  const line = ApplicationMetrics.render()
    .split('\n')
    .find((sample) => sample.startsWith(`${name}{component="sqs_consumer"} `));
  return line ? Number(line.split(' ').at(-1)) : 0;
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

async function getQueueCounts(url: string): Promise<{ visible: number; inFlight: number }> {
  const result = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return {
    visible: Number(result.Attributes?.ApproximateNumberOfMessages ?? 0),
    inFlight: Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0),
  };
}

function createConsumerConfig(consumerName: string, queueName: string): ConfigService {
  const values: Record<string, string> = {
    AWS_REGION: region,
    AWS_ACCESS_KEY_ID: credentials.accessKeyId,
    AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
    SQS_ENDPOINT: endpoint,
    SQS_MAIN_QUEUE_NAME: queueName,
    SQS_DLQ_QUEUE_NAME: dlqName,
    SQS_CONSUMER_NAME: consumerName,
    SQS_WAIT_TIME_SECONDS: '1',
  };
  return {
    get: (key: string, defaultValue?: string) => values[key] ?? defaultValue,
  } as unknown as ConfigService;
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

const describeLocalStack = process.env.RUN_LOCALSTACK_INTEGRATION ? describe : describe.skip;

describeLocalStack('SQS consumer with LocalStack', () => {
  beforeAll(async () => {
    const dlq = await sqs.send(
      new CreateQueueCommand({
        QueueName: dlqName,
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
          VisibilityTimeout: '1',
        },
      }),
    );
    if (!dlq.QueueUrl) {
      throw new Error('LocalStack did not return the test DLQ URL.');
    }
    dlqUrl = dlq.QueueUrl;

    const dlqAttributes = await sqs.send(
      new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }),
    );
    const dlqArn = dlqAttributes.Attributes?.QueueArn;
    if (!dlqArn) {
      throw new Error('LocalStack did not return the test DLQ ARN.');
    }

    for (const name of queueNames) {
      const queue = await sqs.send(
        new CreateQueueCommand({
          QueueName: name,
          Attributes: {
            FifoQueue: 'true',
            ContentBasedDeduplication: 'false',
            VisibilityTimeout: '1',
            RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: '2' }),
          },
        }),
      );
      if (!queue.QueueUrl) {
        throw new Error(`LocalStack did not return the test transaction queue URL for ${name}.`);
      }
      queueUrls.push(queue.QueueUrl);
    }

    consumers = consumerNames.map(
      (consumerName, index) =>
        new SqsConsumerService(
          createConsumerConfig(consumerName, queueNames[index]),
          new WageringService(applicationDatabase),
        ),
    );
    await Promise.all(consumers.map((consumer) => consumer.onModuleInit()));
  }, 30000);

  afterAll(async () => {
    await Promise.all(consumers.map((consumer) => consumer.onModuleDestroy()));
    for (const url of queueUrls) {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: url }));
    }
    if (dlqUrl) {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
    }
    sqs.destroy();
    await applicationDatabase.end();
    await database.end();
  }, 30000);

  test(
    'retries transient database failures until commit and redrives poison messages to the DLQ',
    async () => {
      const poisonBody = '{invalid-json';
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: queueUrls[0],
          MessageBody: poisonBody,
          MessageGroupId: 'poison-message',
          MessageDeduplicationId: randomUUID(),
        }),
      );

      let deadLetter;
      await waitUntil(
        'poison message to reach the DLQ',
        async () => {
          const response = await sqs.send(
            new ReceiveMessageCommand({
              QueueUrl: dlqUrl,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds: 1,
            }),
          );
          deadLetter = response.Messages?.[0];
          return Boolean(deadLetter);
        },
      );
      expect(deadLetter?.Body).toBe(poisonBody);
      if (deadLetter?.ReceiptHandle) {
        await sqs.send(
          new DeleteMessageCommand({ QueueUrl: dlqUrl, ReceiptHandle: deadLetter.ReceiptHandle }),
        );
      }

      const wallet = await walletsService.createWallet({
        playerId: `sqs-localstack-${randomUUID()}`,
        initialBalance: { amount: '10.00', currency: 'BRL' },
      });
      const idempotencyKey = randomUUID();
      const messageId = randomUUID();
      const externalTransactionId = randomUUID();
      let queueMessageId: string | undefined;
      const functionName = `sqs_retry_failure_${runId}`;
      const triggerName = `sqs_retry_failure_trigger_${runId}`;
      let triggerInstalled = false;

      try {
        await database.query(`
          CREATE FUNCTION public.${functionName}() RETURNS trigger
          LANGUAGE plpgsql AS $body$
          BEGIN
            IF NEW.wallet_id = '${wallet.id}' THEN
              RAISE EXCEPTION 'injected transient ledger failure' USING ERRCODE = '40001';
            END IF;
            RETURN NEW;
          END;
          $body$;
          CREATE TRIGGER ${triggerName}
          BEFORE INSERT ON wallet_ledger_entries
          FOR EACH ROW EXECUTE FUNCTION public.${functionName}();
        `);
        triggerInstalled = true;

        const retryBaseline = metricValue('wager_retries_total');
        const sentMessage = await sqs.send(
          new SendMessageCommand({
            QueueUrl: queueUrls[1],
            MessageBody: JSON.stringify({
              messageId,
              type: 'WagerTransactionRequested',
              occurredAt: new Date().toISOString(),
              data: {
                providerId: 'localstack-integration',
                externalTransactionId,
                idempotencyKey,
                walletId: wallet.id,
                playerId: wallet.playerId,
                roundId: 'localstack-round',
                gameId: 'localstack-game',
                kind: WagerTransactionKind.Bet,
                money: { amount: '3.00', currency: 'BRL' },
              },
            }),
            MessageGroupId: wallet.id,
            MessageDeduplicationId: messageId,
          }),
        );
        queueMessageId = sentMessage.MessageId;
        if (!queueMessageId) {
          throw new Error('LocalStack did not return the transaction message ID.');
        }

        await waitUntil('the consumer to encounter the injected database failure', async () => {
          if (metricValue('wager_retries_total') <= retryBaseline) {
            return false;
          }
          const result = await applicationDatabase.query(
            `SELECT
               (SELECT balance FROM wallets WHERE id = $1) AS balance,
               (SELECT count(*) FROM message_inbox WHERE message_id = $2) AS inbox_count`,
            [wallet.id, queueMessageId],
          );
          return result.rows[0].balance === '10.00' && result.rows[0].inbox_count === '0';
        });

        await database.query(`DROP TRIGGER ${triggerName} ON wallet_ledger_entries`);
        await database.query(`DROP FUNCTION public.${functionName}()`);
        triggerInstalled = false;

        let lastRetryState: unknown;
        try {
          await waitUntil(
            'the retried transaction to commit and its SQS message to be acknowledged',
            async () => {
              const result = await applicationDatabase.query(
                `SELECT
                   (SELECT balance FROM wallets WHERE id = $1) AS balance,
                   (SELECT count(*) FROM message_inbox WHERE message_id = $2) AS inbox_count,
                   (SELECT count(*) FROM wager_transactions WHERE external_transaction_id = $3) AS transaction_count,
                   (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1) AS ledger_count`,
                [wallet.id, queueMessageId, externalTransactionId],
              );
              const queueCounts = await getQueueCounts(queueUrls[1]);
              lastRetryState = { database: result.rows[0], queue: queueCounts };
              return (
                result.rows[0].balance === '7.00' &&
                result.rows[0].inbox_count === '1' &&
                result.rows[0].transaction_count === '1' &&
                result.rows[0].ledger_count === '2' &&
                queueCounts.visible === 0 &&
                queueCounts.inFlight === 0
              );
            },
          );
        } catch (error) {
          throw new Error(
            `Retry did not reach the expected committed/acknowledged state: ${JSON.stringify(lastRetryState)}`,
            { cause: error },
          );
        }

        const walletCount = await applicationDatabase.query(
          'SELECT balance FROM wallets WHERE id = $1',
          [wallet.id],
        );
        expect(walletCount.rows[0].balance).toBe('7.00');

        for (const index of [0, 2]) {
          const followupId = randomUUID();
          const followupExternalId = randomUUID();
          const followupKey = randomUUID();
          const followup = await sqs.send(
            new SendMessageCommand({
              QueueUrl: queueUrls[index],
              MessageBody: JSON.stringify({
                messageId: followupId,
                type: 'WagerTransactionRequested',
                occurredAt: new Date().toISOString(),
                data: {
                  providerId: 'localstack-integration',
                  externalTransactionId: followupExternalId,
                  idempotencyKey: followupKey,
                  walletId: wallet.id,
                  playerId: wallet.playerId,
                  roundId: 'localstack-round',
                  gameId: 'localstack-game',
                  kind: WagerTransactionKind.Bet,
                  money: { amount: '1.00', currency: 'BRL' },
                },
              }),
              MessageGroupId: wallet.id,
              MessageDeduplicationId: followupId,
            }),
          );
          const followupQueueMessageId = followup.MessageId;
          if (!followupQueueMessageId) {
            throw new Error('LocalStack did not return a follow-up message ID.');
          }

          const expectedBalance = index === 0 ? '6.00' : '5.00';
          await waitUntil(
            `consumer ${consumerNames[index]} to process and acknowledge its message`,
            async () => {
              const state = await applicationDatabase.query(
                `SELECT
                   (SELECT balance FROM wallets WHERE id = $1) AS balance,
                   (SELECT count(*) FROM message_inbox
                    WHERE consumer_name = $2 AND message_id = $3) AS inbox_count`,
                [wallet.id, consumerNames[index], followupQueueMessageId],
              );
              const counts = await getQueueCounts(queueUrls[index]);
              return (
                state.rows[0].balance === expectedBalance &&
                state.rows[0].inbox_count === '1' &&
                counts.visible === 0 &&
                counts.inFlight === 0
              );
            },
          );
        }
      } finally {
        if (triggerInstalled) {
          await database.query(`DROP TRIGGER IF EXISTS ${triggerName} ON wallet_ledger_entries`);
          await database.query(`DROP FUNCTION IF EXISTS public.${functionName}()`);
        }
        await database.query(
          'DELETE FROM message_inbox WHERE consumer_name = ANY($1::varchar[]) AND message_id = $2',
          [consumerNames, queueMessageId],
        );
        await database.query('DELETE FROM outbox_events WHERE aggregate_id = $1', [wallet.id]);
        await deleteLedgerEntriesForTest(wallet.id);
        await database.query('DELETE FROM wager_transactions WHERE wallet_id = $1', [wallet.id]);
        await database.query('DELETE FROM wallets WHERE id = $1', [wallet.id]);
      }
    },
    45000,
  );
});
