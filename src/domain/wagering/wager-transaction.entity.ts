import { defineEntity } from '@mikro-orm/core';

export const WagerTransactionEntity = defineEntity({
  name: 'WagerTransaction',
  tableName: 'wager_transactions',
  uniques: [{ name: 'wager_transactions_provider_ext_unique', properties: ['providerId', 'externalTransactionId'] }],
  properties: {
    id: { type: 'varchar', length: 36, primary: true },
    providerId: { type: 'varchar', length: 64, fieldName: 'provider_id' },
    externalTransactionId: { type: 'varchar', length: 128, fieldName: 'external_transaction_id' },
    idempotencyKey: { type: 'varchar', length: 128, fieldName: 'idempotency_key' },
    payloadHash: { type: 'varchar', length: 128, fieldName: 'payload_hash' },
    walletId: { type: 'varchar', length: 36, fieldName: 'wallet_id' },
    playerId: { type: 'varchar', length: 64, fieldName: 'player_id' },
    roundId: { type: 'varchar', length: 64, fieldName: 'round_id' },
    gameId: { type: 'varchar', length: 64, fieldName: 'game_id' },
    kind: { type: 'varchar', length: 32 },
    amount: { type: 'decimal', precision: 18, scale: 2 },
    currency: { type: 'varchar', length: 8 },
    referenceExternalTransactionId: { type: 'varchar', length: 128, nullable: true, fieldName: 'reference_external_transaction_id' },
    status: { type: 'varchar', length: 32 },
    referenceTransactionId: { type: 'varchar', length: 36, nullable: true, fieldName: 'reference_transaction_id' },
    failureCode: { type: 'varchar', length: 64, nullable: true, fieldName: 'failure_code' },
    createdAt: { type: 'Date', fieldName: 'created_at', defaultRaw: 'CURRENT_TIMESTAMP' },
    processedAt: { type: 'Date', nullable: true, fieldName: 'processed_at' },
  },
  indexes: [
    { name: 'wager_transactions_wallet_id_idx', properties: ['walletId'] },
    { name: 'wager_transactions_player_id_idx', properties: ['playerId'] },
    { name: 'wager_transactions_status_idx', properties: ['status'] },
    { name: 'wager_transactions_idempotency_key_idx', properties: ['idempotencyKey'] },
    { name: 'wager_transactions_provider_reference_idx', properties: ['providerId', 'referenceExternalTransactionId'] },
  ],
});
