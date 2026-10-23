import 'reflect-metadata';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { CreateWagerTransactionDto } from '../../wagering/dto/create-transaction.dto';

export class PermanentMessageError extends Error {}

export async function parseWagerTransactionMessage(body: string): Promise<CreateWagerTransactionDto> {
  let envelope: unknown;
  try {
    envelope = JSON.parse(body);
  } catch {
    throw new PermanentMessageError('Message body is not valid JSON.');
  }

  if (
    !envelope ||
    typeof envelope !== 'object' ||
    !('messageId' in envelope) ||
    typeof envelope.messageId !== 'string' ||
    envelope.messageId.length === 0 ||
    !('type' in envelope) ||
    envelope.type !== 'WagerTransactionRequested' ||
    !('occurredAt' in envelope) ||
    typeof envelope.occurredAt !== 'string' ||
    Number.isNaN(Date.parse(envelope.occurredAt)) ||
    !('data' in envelope) ||
    !envelope.data ||
    typeof envelope.data !== 'object'
  ) {
    throw new PermanentMessageError('Message envelope is invalid.');
  }

  const dto = plainToInstance(CreateWagerTransactionDto, envelope.data);
  const errors = await validate(dto, { whitelist: true, forbidNonWhitelisted: true });
  if (errors.length > 0) {
    throw new PermanentMessageError('Message transaction data failed validation.');
  }

  return dto;
}
