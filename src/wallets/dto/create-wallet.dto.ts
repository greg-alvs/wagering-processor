import { Type } from 'class-transformer';
import { IsDefined, IsString, Length, Matches, ValidateNested } from 'class-validator';

export class MoneyDto {
  @IsString()
  @Matches(/^\d{1,16}(\.\d{1,2})?$/, { message: 'amount must fit numeric(18,2)' })
  amount: string;

  @IsString()
  @Matches(/^[A-Z]{3}$/, { message: 'currency must be a valid ISO-4217 code' })
  currency: string;
}

export class CreateWalletDto {
  @IsString()
  @IsDefined()
  @Length(1, 64)
  playerId: string;

  @ValidateNested()
  @Type(() => MoneyDto)
  initialBalance: MoneyDto;
}
