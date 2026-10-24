import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Pool } from 'pg';

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        allowGlobalContext: true,
        driver: PostgreSqlDriver,
        entities: ['dist/**/*.entity.js'],
        entitiesTs: ['src/**/*.entity.ts'],
        dbName: config.get<string>('POSTGRES_DB', 'wagering'),
        user: config.getOrThrow<string>('POSTGRES_APP_USER'),
        password: config.getOrThrow<string>('POSTGRES_APP_PASSWORD'),
        host: config.get<string>('POSTGRES_HOST', 'localhost'),
        port: Number(config.get<number>('POSTGRES_PORT', 5432)),
        debug: config.get<string>('NODE_ENV') !== 'production',
        migrations: {
          path: 'dist/migrations',
          pathTs: 'src/migrations',
          transactional: true,
          disableForeignKeys: false,
          snapshot: true,
        },
      }),
    }),
  ],
  providers: [
    {
      provide: 'DATABASE_POOL',
      inject: [ConfigService],
      useFactory: (config: ConfigService) =>
        new Pool({
          host: config.get<string>('POSTGRES_HOST', 'localhost'),
          port: Number(config.get<number>('POSTGRES_PORT', 5432)),
          user: config.getOrThrow<string>('POSTGRES_APP_USER'),
          password: config.getOrThrow<string>('POSTGRES_APP_PASSWORD'),
          database: config.get<string>('POSTGRES_DB', 'wagering'),
          max: 20,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 5000,
        }),
    },
  ],
  exports: ['DATABASE_POOL'],
})
export class DatabaseModule {}
