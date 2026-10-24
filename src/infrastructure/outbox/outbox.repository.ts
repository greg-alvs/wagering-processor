import { PoolClient } from 'pg';
import { IntegrationEvent } from './integration-event';

export class OutboxRepository {
  async append(client: PoolClient, event: IntegrationEvent<unknown>): Promise<void> {
    const envelope = event.toJSON();
    await client.query(
      `INSERT INTO outbox_events (
         event_id, aggregate_id, event_type, payload, occurred_at
       ) VALUES ($1, $2, $3, $4::jsonb, $5)`,
      [event.eventId, event.aggregateId, event.eventType, JSON.stringify(envelope), event.occurredAt],
    );
  }
}
