import { defineEntity } from '@mikro-orm/core';

export const IdempotencyKeyEntity = defineEntity({
  name: 'IdempotencyKey',
  tableName: 'idempotency_keys',
  uniques: [{ name: 'idempotency_keys_key_unique', properties: ['key'] }],
  properties: {
    id: { type: 'varchar', length: 36, primary: true },
    key: { type: 'varchar', length: 128 },
    payloadHash: { type: 'varchar', length: 128, fieldName: 'payload_hash' },
    responseHash: { type: 'varchar', length: 128, fieldName: 'response_hash' },
    createdAt: { type: 'Date', fieldName: 'created_at', defaultRaw: 'CURRENT_TIMESTAMP' },
    completedAt: { type: 'Date', nullable: true, fieldName: 'completed_at' },
  },
  indexes: [{ name: 'idempotency_keys_key_idx', properties: ['key'] }],
});
