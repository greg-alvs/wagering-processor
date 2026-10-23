import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000001_CreateOutbox extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table "outbox_events" (
        "event_id" varchar(36) not null,
        "aggregate_id" varchar(128) not null,
        "event_type" varchar(128) not null,
        "payload" jsonb not null,
        "occurred_at" timestamptz not null,
        "created_at" timestamptz not null default CURRENT_TIMESTAMP,
        "published_at" timestamptz,
        "attempts" integer not null default 0,
        "next_attempt_at" timestamptz not null default CURRENT_TIMESTAMP,
        constraint "outbox_events_pkey" primary key ("event_id"),
        constraint "outbox_events_attempts_non_negative" check ("attempts" >= 0)
      );

      create index "outbox_events_pending_idx"
        on "outbox_events" ("next_attempt_at", "created_at")
        where "published_at" is null;
    `);
  }

  override async down(): Promise<void> {
    this.addSql('drop table "outbox_events";');
  }
}
