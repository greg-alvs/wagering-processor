import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { DatabaseModule } from './database/database.module';
import { WalletsController } from './wallets/wallets.controller';
import { WalletsService } from './wallets/wallets.service';
import { WageringController } from './wagering/wagering.controller';
import { WageringService } from './wagering/wagering.service';
import { SqsConsumerService } from './infrastructure/messaging/sqs-consumer.service';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env', '.env.example'],
    }),
    DatabaseModule,
  ],
  controllers: [AppController, WalletsController, WageringController],
  providers: [AppService, WalletsService, WageringService, SqsConsumerService],
})
export class AppModule {}
