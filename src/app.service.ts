import { Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import { Pool } from 'pg';
import { SqsConsumerService } from './infrastructure/messaging/sqs-consumer.service';
import { OutboxPublisherService } from './infrastructure/outbox/outbox-publisher.service';
import { ApplicationMetrics } from './infrastructure/observability/application-metrics';

type DependencyCheck = 'ok' | 'error';

@Injectable()
export class AppService {
  constructor(
    @Inject('DATABASE_POOL') private readonly pool: Pool,
    private readonly sqsConsumer: SqsConsumerService,
    private readonly outboxPublisher: OutboxPublisherService,
  ) {}

  getLive() {
    return { status: 'ok', timestamp: new Date().toISOString() };
  }

  async getReady() {
    const [postgres, transactionQueue, eventsQueue] = await Promise.allSettled([
      this.pool.query('SELECT 1'),
      this.sqsConsumer.checkReady(),
      this.outboxPublisher.checkReady(),
    ]);

    const checks = {
      postgres: this.toCheck(postgres),
      transactionQueue: this.toCheck(transactionQueue),
      eventsQueue: this.toCheck(eventsQueue),
    };
    const response = {
      status: Object.values(checks).every((check) => check === 'ok') ? 'ok' : 'error',
      checks,
      timestamp: new Date().toISOString(),
    };

    if (response.status === 'error') {
      throw new ServiceUnavailableException(response);
    }

    return response;
  }

  async getMetrics(): Promise<string> {
    const [deadLetterMessages, outboxLag] = await Promise.all([
      this.sqsConsumer.getDeadLetterQueueMessageCount(),
      this.pool.query<{ lag_seconds: string | null }>(
        `SELECT coalesce(
           extract(epoch FROM (CURRENT_TIMESTAMP - min(created_at))),
           0
         ) AS lag_seconds
         FROM outbox_events
         WHERE published_at IS NULL`,
      ),
    ]);

    return ApplicationMetrics.render({
      sqs_dlq_messages: deadLetterMessages,
      outbox_lag_seconds: Number(outboxLag.rows[0]?.lag_seconds ?? 0),
    });
  }

  private toCheck<T>(result: PromiseSettledResult<T>): DependencyCheck {
    return result.status === 'fulfilled' ? 'ok' : 'error';
  }
}
