import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000007_ProtectWalletLedgerImmutability extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create or replace function "reject_wallet_ledger_mutation"()
      returns trigger
      language plpgsql
      as $$
      begin
        raise exception 'wallet_ledger_entries is immutable'
          using errcode = '55000';
      end;
      $$;

      create trigger "wallet_ledger_entries_immutable_row"
      before update or delete on "wallet_ledger_entries"
      for each row execute function "reject_wallet_ledger_mutation"();

      create trigger "wallet_ledger_entries_immutable_truncate"
      before truncate on "wallet_ledger_entries"
      for each statement execute function "reject_wallet_ledger_mutation"();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop trigger if exists "wallet_ledger_entries_immutable_truncate" on "wallet_ledger_entries";
      drop trigger if exists "wallet_ledger_entries_immutable_row" on "wallet_ledger_entries";
      drop function if exists "reject_wallet_ledger_mutation"();
    `);
  }
}
