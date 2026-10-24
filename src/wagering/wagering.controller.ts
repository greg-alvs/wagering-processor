import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Param,
  Post,
  Res,
  ValidationPipe,
} from '@nestjs/common';
import { CreateWagerTransactionPayloadDto } from './dto/create-transaction.dto';
import { WageringService } from './wagering.service';

interface HttpStatusResponse {
  status(code: number): unknown;
}

@Controller()
export class WageringController {
  constructor(private readonly wageringService: WageringService) {}

  @Post('wagering/transactions')
  createTransaction(
    @Body(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true })) dto: CreateWagerTransactionPayloadDto,
    @Headers('idempotency-key') idempotencyKey?: string,
    @Res({ passthrough: true }) response?: HttpStatusResponse,
  ) {
    if (!idempotencyKey?.trim() || idempotencyKey.length > 128) {
      throw new BadRequestException('A valid Idempotency-Key header is required.');
    }

    return this.wageringService.createTransaction(dto, idempotencyKey).then((result) => {
      if ('idempotentReplay' in result && result.idempotentReplay) {
        response?.status(HttpStatus.OK);
      } else if (
        'status' in result &&
        (result.status === 'PENDING' || result.status === 'PENDING_REFERENCE')
      ) {
        response?.status(HttpStatus.ACCEPTED);
      } else if ('status' in result && result.status === 'REJECTED') {
        response?.status(HttpStatus.UNPROCESSABLE_ENTITY);
      } else {
        response?.status(HttpStatus.CREATED);
      }
      return result;
    });
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
