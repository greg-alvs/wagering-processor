import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000005_AddWalletOpeningBalance extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      alter table "wallets" add column "opening_balance" numeric(18,2);

      update "wallets" wallet
      set "opening_balance" = wallet."balance" - coalesce(
        (
          select sum(
            case when entry."direction" = 'CREDIT' then entry."amount" else -entry."amount" end
          )
          from "wallet_ledger_entries" entry
          where entry."wallet_id" = wallet."id"
        ),
        0
      );

      alter table "wallets"
        alter column "opening_balance" set not null,
        add constraint "wallets_opening_balance_non_negative" check ("opening_balance" >= 0);
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      alter table "wallets"
        drop constraint "wallets_opening_balance_non_negative",
        drop column "opening_balance";
    `);
  }
}
