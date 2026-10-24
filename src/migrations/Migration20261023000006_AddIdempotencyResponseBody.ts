import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000006_AddIdempotencyResponseBody extends Migration {
  override async up(): Promise<void> {
    this.addSql('alter table "idempotency_keys" add column "response_body" jsonb;');
  }

  override async down(): Promise<void> {
    this.addSql('alter table "idempotency_keys" drop column "response_body";');
  }
}
