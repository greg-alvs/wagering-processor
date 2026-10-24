import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { WageringService } from '../../wagering/wagering.service';
import { ApplicationMetrics } from '../observability/application-metrics';

const POLL_INTERVAL_MS = 1000;
const MAX_ATTEMPTS_PER_POLL = 100;
const MAX_REFERENCE_ATTEMPTS = 10;

@Injectable()
export class PendingReferenceWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PendingReferenceWorkerService.name);
  private timer?: ReturnType<typeof setTimeout>;
  private activePoll?: Promise<void>;
  private stopping = false;

  constructor(
    @Inject('DATABASE_POOL') private readonly pool: Pool,
    private readonly wageringService: WageringService,
  ) {}

  onModuleInit(): void {
    this.schedule(0);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.timer) {
      clearTimeout(this.timer);
    }
    await this.activePoll;
  }

  private schedule(delay: number): void {
    if (this.stopping) {
      return;
    }
    this.timer = setTimeout(() => {
      this.activePoll = this.poll();
    }, delay);
  }

  private async poll(): Promise<void> {
    try {
      for (let count = 0; count < MAX_ATTEMPTS_PER_POLL; count += 1) {
        if (!(await this.runOnce())) {
          break;
        }
      }
    } catch (error) {
      ApplicationMetrics.increment('wager_retries_total', { component: 'pending_reference' });
      this.logger.error(
        JSON.stringify({
          event: 'pending_reference_worker_failed',
          errorCode: this.errorCode(error),
        }),
      );
    } finally {
      this.schedule(POLL_INTERVAL_MS);
    }
  }

  async runOnce(): Promise<boolean> {
    const startedAt = performance.now();
    const client = await this.pool.connect();
    let transactionStarted = false;

    try {
      await client.query('BEGIN');
      transactionStarted = true;
      const pendingResult = await client.query(
        `SELECT id, provider_id, wallet_id FROM wager_transactions
         WHERE status = $1
           AND (
             reference_next_attempt_at IS NULL
             OR reference_next_attempt_at <= CURRENT_TIMESTAMP
             OR reference_expires_at <= CURRENT_TIMESTAMP
           )
         ORDER BY COALESCE(reference_next_attempt_at, created_at), created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1`,
        [WagerTransactionStatus.PendingReference],
      );
      const pending = pendingResult.rows[0];
      if (!pending) {
        await client.query('COMMIT');
        transactionStarted = false;
        return false;
      }

      const outcome = await this.wageringService.reprocessPendingReference(
        client,
        pending.id,
        MAX_REFERENCE_ATTEMPTS,
      );
      await client.query('COMMIT');
      transactionStarted = false;
      if (outcome === 'rescheduled') {
        ApplicationMetrics.increment('wager_retries_total', { component: 'pending_reference' });
      } else {
        ApplicationMetrics.increment('wager_transactions_total', {
          status: outcome === 'processed' ? 'PROCESSED' : 'REJECTED',
        });
      }
      ApplicationMetrics.observe(
        'wager_processing_duration_seconds',
        (performance.now() - startedAt) / 1000,
        { source: 'pending_reference' },
      );
      this.logger.log(
        JSON.stringify({
          event: 'pending_reference_reprocessed',
          correlationId: pending.id,
          transactionId: pending.id,
          walletId: pending.wallet_id,
          providerId: pending.provider_id,
          outcome,
        }),
      );
      return true;
    } catch (error) {
      if (transactionStarted) {
        await client.query('ROLLBACK');
      }
      throw error;
    } finally {
      client.release();
    }
  }

  private errorCode(error: unknown): string | undefined {
    if (typeof error === 'object' && error !== null && 'code' in error) {
      return String(error.code);
    }
    return error instanceof Error ? error.name : undefined;
  }
}
