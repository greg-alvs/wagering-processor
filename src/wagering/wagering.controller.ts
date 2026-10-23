import { Body, Controller, Get, Param, Post, ValidationPipe } from '@nestjs/common';
import { CreateWagerTransactionDto } from './dto/create-transaction.dto';
import { WageringService } from './wagering.service';

@Controller()
export class WageringController {
  constructor(private readonly wageringService: WageringService) {}

  @Post('wagering/transactions')
  createTransaction(@Body(new ValidationPipe({ whitelist: true })) dto: CreateWagerTransactionDto) {
    return this.wageringService.createTransaction(dto);
  }

  @Get('wagering/transactions/:transactionId')
  getTransaction(@Param('transactionId') transactionId: string) {
    return this.wageringService.getTransaction(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  getProviderTransaction(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    return this.wageringService.getTransactionByProvider(providerId, externalTransactionId);
  }
}
