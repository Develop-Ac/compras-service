// src/compras/cotacao.module.ts
import { Module } from '@nestjs/common';
import { CotacaoController } from './cotacao.controller';
import { CotacaoService } from './cotacao.service';
import { PrismaService } from '../../prisma/prisma.service';
import { CotacaoRepository } from './cotacao.repository';

@Module({
  controllers: [CotacaoController],
  providers: [CotacaoService, CotacaoRepository, PrismaService],
  exports: [CotacaoService, CotacaoRepository],
})
export class CotacaoModule {}
