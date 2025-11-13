// src/cotacao-sync/cotacao-sync.module.ts
import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { CotacaoSyncController } from './cotacao-sync.controller';
import { CotacaoSyncService } from './cotacao-sync.service';
import { PrismaService } from '../../../prisma/prisma.service';
import { OpenQueryModule } from 'src/shared/database/openquery/openquery.module';
import { CotacaoSyncRepository } from './cotacao-sync.repository';
// import { DatabaseModule } from '../../cotacao/openquery/database/database.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    HttpModule.register({
      timeout: 7000,
      maxRedirects: 2,
    }),
    OpenQueryModule
    // DatabaseModule,
  ],
  controllers: [CotacaoSyncController],
  providers: [
    CotacaoSyncService,
    PrismaService,
    CotacaoSyncRepository,
  ],
})
export class CotacaoSyncModule {}
