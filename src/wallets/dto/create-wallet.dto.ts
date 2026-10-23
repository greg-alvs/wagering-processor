import { Type } from 'class-transformer';
import { IsDefined, IsString, Matches, ValidateNested } from 'class-validator';

export class MoneyDto {
  @IsString()
  @Matches(/^\d+(\.\d{1,2})?$/, { message: 'amount must be a decimal string with up to 2 decimal places' })
  amount: string;

  @IsString()
  @Matches(/^[A-Z]{3}$/, { message: 'currency must be a valid ISO-4217 code' })
  currency: string;
}

export class CreateWalletDto {
  @IsString()
  @IsDefined()
  playerId: string;

  @ValidateNested()
  @Type(() => MoneyDto)
  initialBalance: MoneyDto;
}
