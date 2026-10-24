import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { OutboxPublisherService } from './outbox-publisher.service';

const runId = randomUUID().replaceAll('-', '');
const queueName = `wager-outbox-it-${runId}.fifo`;
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
const publisherDatabase = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_APP_USER ?? process.env.POSTGRES_USER ?? 'postgres',
  password:
    process.env.POSTGRES_APP_PASSWORD ??
    process.env.POSTGRES_PASSWORD ??
    'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
let queueUrl: string;
let publishers: OutboxPublisherService[] = [];

function createPublisher(): OutboxPublisherService {
  const values: Record<string, string> = {
    AWS_REGION: region,
    AWS_ACCESS_KEY_ID: credentials.accessKeyId,
    AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
    SQS_ENDPOINT: endpoint,
    SQS_EVENTS_QUEUE_NAME: queueName,
  };
  const config = {
    get: (key: string, defaultValue?: string) => values[key] ?? defaultValue,
  } as unknown as ConfigService;
  return new OutboxPublisherService(config, publisherDatabase);
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

async function insertEvents(events: Array<{ eventId: string; aggregateId: string }>): Promise<void> {
  const client = await database.connect();
  try {
    await client.query('BEGIN');
    for (const event of events) {
      const payload = {
        eventId: event.eventId,
        eventType: 'OutboxPublisherIntegrationTest',
        aggregateId: event.aggregateId,
        occurredAt: new Date().toISOString(),
        data: { testRunId: runId },
      };
      await client.query(
        `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at)
         VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP)`,
        [event.eventId, event.aggregateId, 'OutboxPublisherIntegrationTest', JSON.stringify(payload)],
      );
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function receiveUntil(
  expectedEventIds: Set<string>,
  timeoutMilliseconds = 30000,
): Promise<string[]> {
  const receivedIds: string[] = [];
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline && receivedIds.length < expectedEventIds.size) {
    const response = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }),
    );
    for (const message of response.Messages ?? []) {
      if (!message.Body || !message.ReceiptHandle) {
        throw new Error('Received an incomplete outbox message from LocalStack.');
      }
      const payload = JSON.parse(message.Body) as { eventId?: unknown };
      if (typeof payload.eventId !== 'string') {
        throw new Error('Outbox message payload has no eventId.');
      }
      receivedIds.push(payload.eventId);
      await sqs.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }),
      );
    }
  }
  return receivedIds;
}

const describeLocalStack = process.env.RUN_LOCALSTACK_INTEGRATION ? describe : describe.skip;

describeLocalStack('Outbox publisher with LocalStack', () => {
  beforeAll(async () => {
    const queue = await sqs.send(
      new CreateQueueCommand({
        QueueName: queueName,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );
    if (!queue.QueueUrl) {
      throw new Error('LocalStack did not return the outbox test queue URL.');
    }
    queueUrl = queue.QueueUrl;
  }, 30000);

  afterAll(async () => {
    await Promise.all(publishers.map((publisher) => publisher.onModuleDestroy()));
    if (queueUrl) {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    }
    sqs.destroy();
    await publisherDatabase.end();
    await database.end();
  }, 30000);

  test(
    'publishes pending events once across concurrent publishers',
    async () => {
      const events = Array.from({ length: 8 }, () => ({
        eventId: randomUUID(),
        aggregateId: `outbox-aggregate-${randomUUID()}`,
      }));
      const eventIds = new Set(events.map((event) => event.eventId));
      await insertEvents(events);

      publishers = [createPublisher(), createPublisher()];
      await Promise.all(publishers.map((publisher) => publisher.onModuleInit()));

      await waitUntil('both publishers to mark all test events published', async () => {
        const result = await database.query(
          `SELECT count(*) AS count
           FROM outbox_events
           WHERE event_id = ANY($1::varchar[]) AND published_at IS NOT NULL`,
          [[...eventIds]],
        );
        return result.rows[0].count === String(events.length);
      });

      const delivered = await receiveUntil(eventIds);
      expect(delivered).toHaveLength(events.length);
      expect(new Set(delivered)).toEqual(eventIds);

      const queueAttributes = await sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
        }),
      );
      expect(Number(queueAttributes.Attributes?.ApproximateNumberOfMessages ?? 0)).toBe(0);
      expect(Number(queueAttributes.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0)).toBe(0);

      await Promise.all(publishers.map((publisher) => publisher.onModuleDestroy()));
      publishers = [];
      await database.query('DELETE FROM outbox_events WHERE event_id = ANY($1::varchar[])', [[...eventIds]]);
    },
    45000,
  );

  test(
    'recovers a send-before-mark failure after publisher restart without losing the event',
    async () => {
      const event = { eventId: randomUUID(), aggregateId: `outbox-crash-${randomUUID()}` };
      const functionName = `outbox_crash_${runId}`;
      const triggerName = `outbox_crash_trigger_${runId}`;
      let triggerInstalled = false;
      let interruptedPublisher: OutboxPublisherService | undefined;
      let recoveredPublisher: OutboxPublisherService | undefined;
      await insertEvents([event]);

      try {
        await database.query(`
          CREATE FUNCTION public.${functionName}() RETURNS trigger
          LANGUAGE plpgsql AS $body$
          BEGIN
            IF NEW.event_id = '${event.eventId}' AND NEW.published_at IS NOT NULL THEN
              RAISE EXCEPTION 'injected crash after SQS send' USING ERRCODE = '40001';
            END IF;
            RETURN NEW;
          END;
          $body$;
          CREATE TRIGGER ${triggerName}
          BEFORE UPDATE ON outbox_events
          FOR EACH ROW EXECUTE FUNCTION public.${functionName}();
        `);
        triggerInstalled = true;
        interruptedPublisher = createPublisher();
        await interruptedPublisher.onModuleInit();

        const firstAttempt = await receiveUntil(new Set([event.eventId]), 15000);
        expect(firstAttempt).toEqual([event.eventId]);
        const unpublished = await database.query(
          'SELECT published_at, attempts FROM outbox_events WHERE event_id = $1',
          [event.eventId],
        );
        expect(unpublished.rows[0]).toEqual({ published_at: null, attempts: 0 });

        await interruptedPublisher.onModuleDestroy();
        publishers = publishers.filter((publisher) => publisher !== interruptedPublisher);
        interruptedPublisher = undefined;
        await database.query(`DROP TRIGGER ${triggerName} ON outbox_events`);
        await database.query(`DROP FUNCTION public.${functionName}()`);
        triggerInstalled = false;

        recoveredPublisher = createPublisher();
        publishers.push(recoveredPublisher);
        await recoveredPublisher.onModuleInit();
        await waitUntil('restarted publisher to mark the event published', async () => {
          const result = await database.query(
            'SELECT published_at FROM outbox_events WHERE event_id = $1',
            [event.eventId],
          );
          return result.rowCount === 1 && result.rows[0].published_at !== null;
        });

        const afterRestart = await sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 2,
          }),
        );
        expect(afterRestart.Messages ?? []).toHaveLength(0);
      } finally {
        if (interruptedPublisher) {
          await interruptedPublisher.onModuleDestroy();
          publishers = publishers.filter((publisher) => publisher !== interruptedPublisher);
        }
        if (recoveredPublisher) {
          await recoveredPublisher.onModuleDestroy();
          publishers = publishers.filter((publisher) => publisher !== recoveredPublisher);
        }
        if (triggerInstalled) {
          await database.query(`DROP TRIGGER IF EXISTS ${triggerName} ON outbox_events`);
          await database.query(`DROP FUNCTION IF EXISTS public.${functionName}()`);
        }
        await database.query('DELETE FROM outbox_events WHERE event_id = $1', [event.eventId]);
      }
    },
    45000,
  );
});
