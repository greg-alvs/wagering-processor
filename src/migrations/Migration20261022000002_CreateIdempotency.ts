import { Migration } from '@mikro-orm/migrations';

export class Migration20261022000002_CreateIdempotency extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table if not exists "idempotency_keys" (
        "id" varchar(36) not null,
        "key" varchar(128) not null,
        "payload_hash" varchar(128) not null,
        "response_hash" varchar(128) not null,
        "created_at" timestamptz not null default CURRENT_TIMESTAMP,
        "completed_at" timestamptz,
        constraint "idempotency_keys_pkey" primary key ("id"),
        constraint "idempotency_keys_key_unique" unique ("key")
      );

      create index if not exists "idempotency_keys_key_idx"
        on "idempotency_keys" ("key");
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists "idempotency_keys" cascade;`);
  }
}
