import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000003_AddPendingReferenceRetry extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      alter table "wager_transactions"
        add column "reference_attempts" integer not null default 0,
        add column "reference_next_attempt_at" timestamptz,
        add column "reference_expires_at" timestamptz,
        add constraint "wager_transactions_reference_attempts_non_negative"
          check ("reference_attempts" >= 0);

      update "wager_transactions"
      set "reference_attempts" = 1,
          "reference_next_attempt_at" = CURRENT_TIMESTAMP + INTERVAL '1 second',
          "reference_expires_at" = "created_at" + INTERVAL '24 hours'
      where "status" = 'PENDING_REFERENCE';

      create index "wager_transactions_pending_reference_due_idx"
        on "wager_transactions" ("reference_next_attempt_at", "created_at")
        where "status" = 'PENDING_REFERENCE';
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop index "wager_transactions_pending_reference_due_idx";
      alter table "wager_transactions"
        drop constraint "wager_transactions_reference_attempts_non_negative",
        drop column "reference_attempts",
        drop column "reference_next_attempt_at",
        drop column "reference_expires_at";
    `);
  }
}
