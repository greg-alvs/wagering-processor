import { BadRequestException, Body, Controller, Get, Headers, Param, Post, ValidationPipe } from '@nestjs/common';
import { CreateWagerTransactionPayloadDto } from './dto/create-transaction.dto';
import { WageringService } from './wagering.service';

@Controller()
export class WageringController {
  constructor(private readonly wageringService: WageringService) {}

  @Post('wagering/transactions')
  createTransaction(
    @Body(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true })) dto: CreateWagerTransactionPayloadDto,
    @Headers('idempotency-key') idempotencyKey?: string,
  ) {
    if (!idempotencyKey?.trim() || idempotencyKey.length > 128) {
      throw new BadRequestException('A valid Idempotency-Key header is required.');
    }

    return this.wageringService.createTransaction(dto, idempotencyKey);
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
