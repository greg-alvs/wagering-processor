import { describe, expect, test } from 'bun:test';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';
import { PermanentMessageError, parseWagerTransactionMessage } from './wager-transaction-message';

describe('parseWagerTransactionMessage', () => {
  test('parses a valid transaction envelope', async () => {
    const dto = await parseWagerTransactionMessage(
      JSON.stringify({
        messageId: 'message-1',
        type: 'WagerTransactionRequested',
        occurredAt: '2026-10-23T12:00:00.000Z',
        data: {
          providerId: 'provider-a',
          externalTransactionId: 'external-1',
          idempotencyKey: 'provider-a:external-1',
          playerId: 'player-1',
          walletId: 'wallet-1',
          roundId: 'round-1',
          gameId: 'game-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '25.00', currency: 'BRL' },
        },
      }),
    );

    expect(dto.kind).toBe(WagerTransactionKind.Bet);
    expect(dto.money.amount).toBe('25.00');
  });

  test('rejects malformed and unexpected envelopes', async () => {
    await expect(parseWagerTransactionMessage('{')).rejects.toBeInstanceOf(PermanentMessageError);
    await expect(
      parseWagerTransactionMessage(
        JSON.stringify({
          messageId: 'message-1',
          type: 'UnexpectedEvent',
          occurredAt: '2026-10-23T12:00:00.000Z',
          data: {},
        }),
      ),
    ).rejects.toBeInstanceOf(PermanentMessageError);
  });
});
