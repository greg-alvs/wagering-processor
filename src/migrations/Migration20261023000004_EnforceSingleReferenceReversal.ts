import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000004_EnforceSingleReferenceReversal extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create unique index "wager_transactions_reference_reversal_unique"
        on "wager_transactions" ("reference_transaction_id")
        where "status" = 'PROCESSED'
          and "kind" in ('REFUND', 'ROLLBACK')
          and "reference_transaction_id" is not null;
    `);
  }

  override async down(): Promise<void> {
    this.addSql('drop index "wager_transactions_reference_reversal_unique";');
  }
}
