import { PoolClient } from 'pg';

export class InboxRepository {
  async tryInsert(client: PoolClient, consumerName: string, messageId: string): Promise<boolean> {
    const result = await client.query(
      `INSERT INTO message_inbox (consumer_name, message_id)
       VALUES ($1, $2)
       ON CONFLICT (consumer_name, message_id) DO NOTHING
       RETURNING message_id`,
      [consumerName, messageId],
    );

    return result.rowCount === 1;
  }
}
