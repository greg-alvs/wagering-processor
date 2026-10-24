import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000008_EnforceFinancialConstraints extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      alter table "wallets"
        add constraint "wallets_id_player_unique"
        unique ("id", "player_id"),
        add constraint "wallets_currency_valid"
        check ("currency" ~ '^[A-Z]{3}$') not valid;

      alter table "wager_transactions"
        add constraint "wager_transactions_id_wallet_unique"
          unique ("id", "wallet_id"),
        add constraint "wager_transactions_id_wallet_amount_currency_unique"
          unique ("id", "wallet_id", "amount", "currency"),
        add constraint "wager_transactions_amount_positive"
          check ("amount" > 0) not valid,
        add constraint "wager_transactions_kind_valid"
          check ("kind" in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')) not valid,
        add constraint "wager_transactions_status_valid"
          check ("status" in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')) not valid,
        add constraint "wager_transactions_reference_required"
          check ("kind" not in ('REFUND', 'ROLLBACK') or "reference_external_transaction_id" is not null) not valid,
        add constraint "wager_transactions_currency_valid"
          check ("currency" ~ '^[A-Z]{3}$') not valid,
        add constraint "wager_transactions_wallet_owner_fk"
          foreign key ("wallet_id", "player_id")
          references "wallets" ("id", "player_id") not valid,
        add constraint "wager_transactions_reference_fk"
          foreign key ("reference_transaction_id")
          references "wager_transactions" ("id") not valid;

      alter table "wallet_ledger_entries"
        add constraint "wallet_ledger_entries_transaction_unique"
          unique ("transaction_id"),
        add constraint "wallet_ledger_entries_direction_valid"
          check ("direction" in ('CREDIT', 'DEBIT')) not valid,
        add constraint "wallet_ledger_entries_currency_valid"
          check ("currency" ~ '^[A-Z]{3}$') not valid,
        add constraint "wallet_ledger_entries_positive_amount"
          check ("amount" > 0) not valid,
        add constraint "wallet_ledger_entries_non_negative_balances"
          check ("balance_before" >= 0 and "balance_after" >= 0) not valid,
        add constraint "wallet_ledger_entries_balance_transition"
          check (
            ("direction" = 'CREDIT' and "balance_after" = "balance_before" + "amount")
            or ("direction" = 'DEBIT' and "balance_before" >= "amount"
              and "balance_after" = "balance_before" - "amount")
          ) not valid,
        add constraint "wallet_ledger_entries_transaction_integrity_fk"
          foreign key ("transaction_id", "wallet_id", "amount", "currency")
          references "wager_transactions" ("id", "wallet_id", "amount", "currency") not valid;

      create or replace function "assert_wallet_ledger_balance"()
      returns trigger
      language plpgsql
      as $$
      declare
        target_wallet_id varchar(36);
        stored_balance numeric(18,2);
        baseline_balance numeric(18,2);
        ledger_balance numeric(18,2);
      begin
        if tg_table_name = 'wallets' then
          target_wallet_id := new.id;
        elsif tg_op = 'DELETE' then
          target_wallet_id := old.wallet_id;
        else
          target_wallet_id := new.wallet_id;
        end if;

        select balance, opening_balance
          into stored_balance, baseline_balance
          from wallets
          where id = target_wallet_id;
        if not found then
          return null;
        end if;

        select coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0)
          into ledger_balance
          from wallet_ledger_entries
          where wallet_id = target_wallet_id;

        if stored_balance <> baseline_balance + ledger_balance then
          raise exception 'wallet balance does not match its ledger'
            using errcode = '23514';
        end if;
        return null;
      end;
      $$;

      create constraint trigger "wallets_balance_matches_ledger"
        after insert or update on "wallets"
        deferrable initially deferred
        for each row execute function "assert_wallet_ledger_balance"();

      create constraint trigger "ledger_balance_matches_wallet"
        after insert or update or delete on "wallet_ledger_entries"
        deferrable initially deferred
        for each row execute function "assert_wallet_ledger_balance"();

      create or replace function "assert_transaction_ledger_count"()
      returns trigger
      language plpgsql
      as $$
      declare
        target_transaction_id varchar(36);
        transaction_kind varchar(32);
        transaction_status varchar(32);
        entry_count bigint;
      begin
        if tg_table_name = 'wager_transactions' then
          target_transaction_id := new.id;
        elsif tg_op = 'DELETE' then
          target_transaction_id := old.transaction_id;
        else
          target_transaction_id := new.transaction_id;
        end if;

        select kind, status
          into transaction_kind, transaction_status
          from wager_transactions
          where id = target_transaction_id;
        if not found then
          return null;
        end if;

        select count(*)
          into entry_count
          from wallet_ledger_entries
          where transaction_id = target_transaction_id;

        if transaction_status = 'PROCESSED' and transaction_kind <> 'LOSS' then
          if entry_count <> 1 then
            raise exception 'processed balance-changing transaction must have one ledger entry'
              using errcode = '23514';
          end if;
        elsif entry_count <> 0 then
          raise exception 'transaction without balance impact cannot have a ledger entry'
            using errcode = '23514';
        end if;
        return null;
      end;
      $$;

      create constraint trigger "transactions_ledger_entry_count"
        after insert or update on "wager_transactions"
        deferrable initially deferred
        for each row execute function "assert_transaction_ledger_count"();

      create constraint trigger "ledger_transaction_entry_count"
        after insert or update or delete on "wallet_ledger_entries"
        deferrable initially deferred
        for each row execute function "assert_transaction_ledger_count"();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop trigger if exists "ledger_transaction_entry_count" on "wallet_ledger_entries";
      drop trigger if exists "transactions_ledger_entry_count" on "wager_transactions";
      drop trigger if exists "ledger_balance_matches_wallet" on "wallet_ledger_entries";
      drop trigger if exists "wallets_balance_matches_ledger" on "wallets";
      drop function if exists "assert_transaction_ledger_count"();
      drop function if exists "assert_wallet_ledger_balance"();

      alter table "wallet_ledger_entries"
        drop constraint "wallet_ledger_entries_transaction_integrity_fk",
        drop constraint "wallet_ledger_entries_balance_transition",
        drop constraint "wallet_ledger_entries_non_negative_balances",
        drop constraint "wallet_ledger_entries_positive_amount",
        drop constraint "wallet_ledger_entries_currency_valid",
        drop constraint "wallet_ledger_entries_direction_valid",
        drop constraint "wallet_ledger_entries_transaction_unique";

      alter table "wager_transactions"
        drop constraint "wager_transactions_reference_fk",
        drop constraint "wager_transactions_wallet_owner_fk",
        drop constraint "wager_transactions_currency_valid",
        drop constraint "wager_transactions_reference_required",
        drop constraint "wager_transactions_status_valid",
        drop constraint "wager_transactions_kind_valid",
        drop constraint "wager_transactions_amount_positive",
        drop constraint "wager_transactions_id_wallet_amount_currency_unique",
        drop constraint "wager_transactions_id_wallet_unique";

      alter table "wallets"
        drop constraint "wallets_currency_valid",
        drop constraint "wallets_id_player_unique";
    `);
  }
}
