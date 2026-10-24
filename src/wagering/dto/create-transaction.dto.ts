import { Type } from 'class-transformer';
import { IsEnum, IsOptional, IsString, Matches, ValidateNested } from 'class-validator';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';

export class MoneyDto {
  @IsString()
  @Matches(/^\d+(\.\d{1,2})?$/, { message: 'amount must be a decimal string with up to 2 decimal places' })
  amount: string;

  @IsString()
  @Matches(/^[A-Z]{3}$/, { message: 'currency must be a valid ISO-4217 code' })
  currency: string;
}

export class CreateWagerTransactionPayloadDto {
  @IsString()
  providerId: string;

  @IsString()
  externalTransactionId: string;

  @IsString()
  walletId: string;

  @IsString()
  playerId: string;

  @IsString()
  roundId: string;

  @IsString()
  gameId: string;

  @IsEnum(WagerTransactionKind)
  kind: WagerTransactionKind;

  @ValidateNested()
  @Type(() => MoneyDto)
  money: MoneyDto;

  @IsOptional()
  @IsString()
  referenceExternalTransactionId?: string;
}

export class CreateWagerTransactionDto extends CreateWagerTransactionPayloadDto {
  @IsString()
  idempotencyKey: string;
}
