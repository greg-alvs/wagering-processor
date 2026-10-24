import { Migration } from '@mikro-orm/migrations';

export class Migration20261022000001_CreateFinancialTables extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table if not exists "wager_transactions" (
        "id" varchar(36) not null,
        "provider_id" varchar(64) not null,
        "external_transaction_id" varchar(128) not null,
        "idempotency_key" varchar(128) not null,
        "payload_hash" varchar(128) not null,
        "wallet_id" varchar(36) not null,
        "player_id" varchar(64) not null,
        "round_id" varchar(64) not null,
        "game_id" varchar(64) not null,
        "kind" varchar(32) not null,
        "amount" decimal(18,2) not null,
        "currency" varchar(8) not null,
        "reference_external_transaction_id" varchar(128),
        "status" varchar(32) not null,
        "reference_transaction_id" varchar(36),
        "failure_code" varchar(64),
        "created_at" timestamptz not null default CURRENT_TIMESTAMP,
        "processed_at" timestamptz,
        constraint "wager_transactions_pkey" primary key ("id"),
        constraint "wager_transactions_provider_ext_unique" unique ("provider_id", "external_transaction_id")
      );

      create index if not exists "wager_transactions_wallet_id_idx"
        on "wager_transactions" ("wallet_id");

      create index if not exists "wager_transactions_player_id_idx"
        on "wager_transactions" ("player_id");

      create index if not exists "wager_transactions_status_idx"
        on "wager_transactions" ("status");

      create index if not exists "wager_transactions_idempotency_key_idx"
        on "wager_transactions" ("idempotency_key");

      create index if not exists "wager_transactions_provider_reference_idx"
        on "wager_transactions" ("provider_id", "reference_external_transaction_id");

      create table if not exists "wallet_ledger_entries" (
        "id" varchar(36) not null,
        "wallet_id" varchar(36) not null,
        "transaction_id" varchar(36) not null,
        "direction" varchar(16) not null,
        "amount" decimal(18,2) not null,
        "currency" varchar(8) not null,
        "balance_before" decimal(18,2) not null,
        "balance_after" decimal(18,2) not null,
        "created_at" timestamptz not null default CURRENT_TIMESTAMP,
        constraint "wallet_ledger_entries_pkey" primary key ("id")
      );

      create index if not exists "wallet_ledger_entries_wallet_id_idx"
        on "wallet_ledger_entries" ("wallet_id");

      create index if not exists "wallet_ledger_entries_transaction_id_idx"
        on "wallet_ledger_entries" ("transaction_id");
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop table if exists "wallet_ledger_entries" cascade;
      drop table if exists "wager_transactions" cascade;
    `);
  }
}
