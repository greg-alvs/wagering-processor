import { defineEntity } from '@mikro-orm/core';

export const WalletLedgerEntryEntity = defineEntity({
  name: 'WalletLedgerEntry',
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'varchar', length: 36, primary: true },
    walletId: { type: 'varchar', length: 36, fieldName: 'wallet_id' },
    transactionId: { type: 'varchar', length: 36, fieldName: 'transaction_id' },
    direction: { type: 'varchar', length: 16 },
    amount: { type: 'decimal', precision: 18, scale: 2 },
    currency: { type: 'varchar', length: 8 },
    balanceBefore: { type: 'decimal', precision: 18, scale: 2, fieldName: 'balance_before' },
    balanceAfter: { type: 'decimal', precision: 18, scale: 2, fieldName: 'balance_after' },
    createdAt: { type: 'Date', fieldName: 'created_at', defaultRaw: 'CURRENT_TIMESTAMP' },
  },
  indexes: [
    { name: 'wallet_ledger_entries_wallet_id_idx', properties: ['walletId'] },
    { name: 'wallet_ledger_entries_transaction_id_idx', properties: ['transactionId'] },
  ],
});
