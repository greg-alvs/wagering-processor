import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Pool } from 'pg';
import { ApplicationMetrics } from '../observability/application-metrics';

interface PendingOutboxEvent {
  event_id: string;
  aggregate_id: string;
  payload: unknown;
  attempts: number;
  event_sequence: string;
}

@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private readonly client: SQSClient;
  private readonly queueName: string;
  private queueUrl: string | undefined;
  private stopping = false;
  private publishing: Promise<void> | undefined;

  constructor(
    config: ConfigService,
    @Inject('DATABASE_POOL') private readonly pool: Pool,
  ) {
    this.client = new SQSClient({
      region: config.get<string>('AWS_REGION', 'us-east-1'),
      endpoint:
        config.get<string>('SQS_ENDPOINT') ??
        `http://${config.get<string>('LOCALSTACK_HOST', 'localhost')}:${config.get<string>('LOCALSTACK_PORT', '4566')}`,
      credentials: {
        accessKeyId: config.get<string>('AWS_ACCESS_KEY_ID', 'test'),
        secretAccessKey: config.get<string>('AWS_SECRET_ACCESS_KEY', 'test'),
      },
    });
    this.queueName = config.get<string>('SQS_EVENTS_QUEUE_NAME', 'wager-integration-events.fifo');
  }

  async onModuleInit(): Promise<void> {
    const queue = await this.client.send(new GetQueueUrlCommand({ QueueName: this.queueName }));
    if (!queue.QueueUrl) {
      throw new Error(`SQS did not return a URL for outbox queue ${this.queueName}.`);
    }

    this.queueUrl = queue.QueueUrl;
    this.publishing = this.publishPending();
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    await this.publishing;
    this.client.destroy();
  }

  async checkReady(): Promise<void> {
    if (!this.queueUrl) {
      throw new Error('SQS integration events queue has not been initialized.');
    }

    await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        AttributeNames: ['QueueArn'],
      }),
    );
  }

  private async publishPending(): Promise<void> {
    while (!this.stopping) {
      try {
        const published = await this.publishOne();
        if (!published) {
          await this.delay(1000);
        }
      } catch (error) {
        ApplicationMetrics.increment('wager_retries_total', { component: 'outbox_publisher' });
        this.logger.error(
          JSON.stringify({ event: 'outbox_poll_failed', errorCode: this.errorCode(error) }),
        );
        await this.delay(1000);
      }
    }
  }

  private async publishOne(): Promise<boolean> {
    const databaseClient = await this.pool.connect();
    let transactionStarted = false;

    try {
      await databaseClient.query('BEGIN');
      transactionStarted = true;

      const selected = await databaseClient.query<PendingOutboxEvent>(
        `SELECT event.event_id, event.aggregate_id, event.payload, event.attempts, event.event_sequence
         FROM outbox_events event
         WHERE event.published_at IS NULL
           AND event.next_attempt_at <= CURRENT_TIMESTAMP
           AND NOT EXISTS (
             SELECT 1
             FROM outbox_events earlier
             WHERE earlier.aggregate_id = event.aggregate_id
               AND earlier.published_at IS NULL
               AND earlier.event_sequence < event.event_sequence
           )
         ORDER BY event.event_sequence
         FOR UPDATE SKIP LOCKED
         LIMIT 1`,
      );

      const event = selected.rows[0];
      if (!event) {
        await databaseClient.query('COMMIT');
        transactionStarted = false;
        return false;
      }

      let publicationFailed = false;
      let publicationError: unknown;
      try {
        await this.client.send(
          new SendMessageCommand({
            QueueUrl: this.queueUrl,
            MessageBody: JSON.stringify(event.payload),
            MessageGroupId: event.aggregate_id,
            MessageDeduplicationId: event.event_id,
          }),
        );
      } catch (error) {
        publicationFailed = true;
        publicationError = error;
      }

      if (publicationFailed) {
        const attempts = Number(event.attempts) + 1;
        const delaySeconds = Math.min(2 ** Math.min(attempts - 1, 8), 300);
        const context = this.eventLogContext(event.payload);
        await databaseClient.query(
          `UPDATE outbox_events
           SET attempts = attempts + 1,
               next_attempt_at = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')
           WHERE event_id = $1`,
          [event.event_id, delaySeconds],
        );
        await databaseClient.query('COMMIT');
        transactionStarted = false;
        ApplicationMetrics.increment('wager_retries_total', { component: 'outbox_publisher' });
        this.logger.error(
          JSON.stringify({
            event: 'outbox_publish_retry_scheduled',
            correlationId: context.correlationId,
            eventId: event.event_id,
            aggregateId: event.aggregate_id,
            transactionId: context.transactionId,
            walletId: context.walletId ?? event.aggregate_id,
            providerId: context.providerId,
            retryDelaySeconds: delaySeconds,
            errorCode: this.errorCode(publicationError),
          }),
        );
        return true;
      }

      try {
        await databaseClient.query(
          `UPDATE outbox_events
           SET published_at = CURRENT_TIMESTAMP, attempts = attempts + 1
           WHERE event_id = $1`,
          [event.event_id],
        );
        await databaseClient.query('COMMIT');
        transactionStarted = false;
        const context = this.eventLogContext(event.payload);
        this.logger.log(
          JSON.stringify({
            event: 'outbox_event_published',
            correlationId: context.correlationId,
            eventId: event.event_id,
            aggregateId: event.aggregate_id,
            transactionId: context.transactionId,
            walletId: context.walletId ?? event.aggregate_id,
            providerId: context.providerId,
          }),
        );
        return true;
      } catch (error) {
        throw error;
      }
    } catch (error) {
      if (transactionStarted) {
        await databaseClient.query('ROLLBACK');
      }
      throw error;
    } finally {
      databaseClient.release();
    }
  }

  private errorCode(error: unknown): string | undefined {
    if (typeof error === 'object' && error !== null && 'code' in error) {
      return String(error.code);
    }
    return error instanceof Error ? error.name : undefined;
  }

  private eventLogContext(payload: unknown): {
    correlationId?: string;
    transactionId?: string;
    walletId?: string;
    providerId?: string;
  } {
    if (typeof payload !== 'object' || payload === null) {
      return {};
    }
    const event = payload as {
      correlationId?: unknown;
      data?: { transactionId?: unknown; walletId?: unknown; providerId?: unknown };
    };
    return {
      correlationId: typeof event.correlationId === 'string' ? event.correlationId : undefined,
      transactionId: typeof event.data?.transactionId === 'string' ? event.data.transactionId : undefined,
      walletId: typeof event.data?.walletId === 'string' ? event.data.walletId : undefined,
      providerId: typeof event.data?.providerId === 'string' ? event.data.providerId : undefined,
    };
  }

  private delay(milliseconds: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
  }
}
