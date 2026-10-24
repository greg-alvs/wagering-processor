import { Body, Controller, Get, Param, Post, Query, ValidationPipe } from '@nestjs/common';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { WalletsService } from './wallets.service';

@Controller('wallets')
export class WalletsController {
  constructor(private readonly walletsService: WalletsService) {}

  @Post()
  create(@Body(new ValidationPipe({ whitelist: true })) dto: CreateWalletDto) {
    return this.walletsService.createWallet(dto);
  }

  @Get(':walletId')
  getById(@Param('walletId') walletId: string) {
    return this.walletsService.getWallet(walletId);
  }

  @Get(':walletId/ledger')
  getLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    return this.walletsService.getLedger(walletId, cursor, limit);
  }

  @Post(':walletId/reconciliation')
  reconcile(@Param('walletId') walletId: string) {
    return this.walletsService.reconcile(walletId);
  }

}
