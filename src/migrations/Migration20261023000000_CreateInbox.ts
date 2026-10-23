import { Migration } from '@mikro-orm/migrations';

export class Migration20261023000000_CreateInbox extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table "message_inbox" (
        "consumer_name" varchar(128) not null,
        "message_id" varchar(128) not null,
        "processed_at" timestamptz not null default CURRENT_TIMESTAMP,
        constraint "message_inbox_pkey" primary key ("consumer_name", "message_id"),
        constraint "message_inbox_consumer_name_not_empty" check (length("consumer_name") > 0),
        constraint "message_inbox_message_id_not_empty" check (length("message_id") > 0)
      );
    `);
  }

  override async down(): Promise<void> {
    this.addSql('drop table "message_inbox";');
  }
}
