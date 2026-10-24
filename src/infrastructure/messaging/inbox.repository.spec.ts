import { afterAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { InboxRepository } from './inbox.repository';

const pool = new Pool({
  host: process.env.POSTGRES_HOST ?? 'localhost',
  port: Number(process.env.POSTGRES_PORT ?? 5432),
  user: process.env.POSTGRES_USER ?? 'postgres',
  password: process.env.POSTGRES_PASSWORD ?? 'postgres',
  database: process.env.POSTGRES_DB ?? 'wagering',
});
const inbox = new InboxRepository();

afterAll(async () => {
  await pool.end();
});

describe('InboxRepository', () => {
  test('records a consumer message only once', async () => {
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      expect(await inbox.tryInsert(client, consumerName, messageId)).toBe(true);
      await client.query('COMMIT');

      await client.query('BEGIN');
      expect(await inbox.tryInsert(client, consumerName, messageId)).toBe(false);
      await client.query('COMMIT');
    } finally {
      await client.query('DELETE FROM message_inbox WHERE consumer_name = $1 AND message_id = $2', [
        consumerName,
        messageId,
      ]);
      client.release();
    }
  });

  test('rolls back an inbox insert with its transaction', async () => {
    const consumerName = `test-${randomUUID()}`;
    const messageId = randomUUID();
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      expect(await inbox.tryInsert(client, consumerName, messageId)).toBe(true);
      await client.query('ROLLBACK');

      await client.query('BEGIN');
      expect(await inbox.tryInsert(client, consumerName, messageId)).toBe(true);
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
  });
});
