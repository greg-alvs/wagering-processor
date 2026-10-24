import { Type } from 'class-transformer';
import { IsEnum, IsOptional, IsString, Length, Matches, ValidateNested } from 'class-validator';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';

export class MoneyDto {
  @IsString()
  @Matches(/^\d{1,16}(\.\d{1,2})?$/, { message: 'amount must fit numeric(18,2)' })
  amount: string;

  @IsString()
  @Matches(/^[A-Z]{3}$/, { message: 'currency must be a valid ISO-4217 code' })
  currency: string;
}

export class CreateWagerTransactionPayloadDto {
  @IsString()
  @Length(1, 64)
  providerId: string;

  @IsString()
  @Length(1, 128)
  externalTransactionId: string;

  @IsString()
  @Length(1, 36)
  walletId: string;

  @IsString()
  @Length(1, 64)
  playerId: string;

  @IsString()
  @Length(1, 64)
  roundId: string;

  @IsString()
  @Length(1, 64)
  gameId: string;

  @IsEnum(WagerTransactionKind)
  kind: WagerTransactionKind;

  @ValidateNested()
  @Type(() => MoneyDto)
  money: MoneyDto;

  @IsOptional()
  @IsString()
  @Length(1, 128)
  referenceExternalTransactionId?: string;
}

export class CreateWagerTransactionDto extends CreateWagerTransactionPayloadDto {
  @IsString()
  @Length(1, 128)
  idempotencyKey: string;
}
