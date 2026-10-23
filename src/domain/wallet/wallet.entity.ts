import { defineEntity } from '@mikro-orm/core';

export const Wallet = defineEntity({
  name: 'Wallet',
  tableName: 'wallets',
  uniques: [{ name: 'wallets_player_currency_unique', properties: ['playerId', 'currency'] }],
  properties: {
    id: { type: 'varchar', length: 36, primary: true },
    playerId: { type: 'varchar', length: 64, fieldName: 'player_id' },
    currency: { type: 'varchar', length: 8 },
    balance: { type: 'decimal', precision: 18, scale: 2, default: '0' },
    version: { type: 'integer', default: 1 },
    createdAt: { type: 'Date', fieldName: 'created_at', defaultRaw: 'CURRENT_TIMESTAMP' },
    updatedAt: {
      type: 'Date',
      fieldName: 'updated_at',
      defaultRaw: 'CURRENT_TIMESTAMP',
      onUpdate: () => new Date(),
    },
  },
});
