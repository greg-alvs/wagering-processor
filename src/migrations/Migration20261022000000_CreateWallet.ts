import { Migration } from '@mikro-orm/migrations';

export class Migration20261022000000_CreateWallet extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table if not exists "wallets" (
        "id" varchar(36) not null,
        "player_id" varchar(64) not null,
        "currency" varchar(8) not null,
        "balance" decimal(18,2) not null default 0,
        "version" int not null default 1,
        "created_at" timestamptz not null default CURRENT_TIMESTAMP,
        "updated_at" timestamptz not null default CURRENT_TIMESTAMP,
        constraint "wallets_pkey" primary key ("id"),
        constraint "wallets_player_currency_unique" unique ("player_id", "currency"),
        constraint "wallets_balance_non_negative" check ("balance" >= 0)
      );

      create index if not exists "wallets_player_id_idx"
        on "wallets" ("player_id");
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists "wallets" cascade;`);
  }
}
