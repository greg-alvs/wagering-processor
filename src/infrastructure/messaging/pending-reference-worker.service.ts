import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { WageringService } from '../../wagering/wagering.service';

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
      this.logger.error('Pending reference processing failed.', error);
    } finally {
      this.schedule(POLL_INTERVAL_MS);
    }
  }

  async runOnce(): Promise<boolean> {
    const client = await this.pool.connect();
    let transactionStarted = false;

    try {
      await client.query('BEGIN');
      transactionStarted = true;
      const pendingResult = await client.query(
        `SELECT id FROM wager_transactions
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

      await this.wageringService.reprocessPendingReference(client, pending.id, MAX_REFERENCE_ATTEMPTS);
      await client.query('COMMIT');
      transactionStarted = false;
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
}
