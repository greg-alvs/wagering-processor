import { describe, expect, test } from 'bun:test';
import { ServiceUnavailableException } from '@nestjs/common';
import { Pool } from 'pg';
import { AppService } from './app.service';
import { SqsConsumerService } from './infrastructure/messaging/sqs-consumer.service';
import { OutboxPublisherService } from './infrastructure/outbox/outbox-publisher.service';

describe('AppService readiness', () => {
  test('reports ready when PostgreSQL and both SQS queues are reachable', async () => {
    const pool = { query: async () => ({ rows: [{ '?column?': 1 }] }) } as unknown as Pool;
    const consumer = { checkReady: async () => undefined } as SqsConsumerService;
    const publisher = { checkReady: async () => undefined } as OutboxPublisherService;
    const service = new AppService(pool, consumer, publisher);

    await expect(service.getReady()).resolves.toMatchObject({
      status: 'ok',
      checks: { postgres: 'ok', transactionQueue: 'ok', eventsQueue: 'ok' },
    });
  });

  test('returns service unavailable when a required dependency fails', async () => {
    const pool = { query: async () => ({ rows: [] }) } as unknown as Pool;
    const consumer = { checkReady: async () => undefined } as SqsConsumerService;
    const publisher = {
      checkReady: async () => {
        throw new Error('SQS unavailable');
      },
    } as OutboxPublisherService;
    const service = new AppService(pool, consumer, publisher);

    await expect(service.getReady()).rejects.toBeInstanceOf(ServiceUnavailableException);
  });
});
