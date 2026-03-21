import { Module } from '@nestjs/common';
import { CotacaoController } from './cotacao.controller';
import { CotacaoService } from './cotacao.service';
import { PrismaService } from '../../prisma/prisma.service';
import { CotacaoRepository } from './cotacao.repository';
import { OpenQueryModule } from 'src/shared/database/openquery/openquery.module';

@Module({
  imports: [OpenQueryModule],
  controllers: [CotacaoController],
  providers: [CotacaoService, CotacaoRepository, PrismaService],
  exports: [CotacaoService, CotacaoRepository],
})
export class CotacaoModule {}
