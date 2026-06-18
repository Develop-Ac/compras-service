import { Module } from '@nestjs/common';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryHttpModule } from '../cotacao/openquery/openquery.module';
import { VinculacaoNfeController } from './vinculacao-nfe.controller';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';
import { AutoVinculoService } from './auto-vinculo.service';
import { NotaFiscalRepository } from '../nota fiscal/nota fiscal/notaFiscal.repository';

@Module({
  // OpenQueryModule -> OpenQueryService (Firebird via MSSQL); OpenQueryHttpModule
  // exporta ConsultaOpenqueryRepository (fornecedor por for_codigo); PrismaModule -> Prisma.
  imports: [OpenQueryModule, OpenQueryHttpModule, PrismaModule],
  controllers: [VinculacaoNfeController],
  providers: [
    VinculacaoNfeService,
    VinculacaoNfeRepository,
    AutoVinculoService,
    // NotaFiscalRepository só depende de OpenQueryService (já disponível em OpenQueryModule).
    NotaFiscalRepository,
  ],
  exports: [VinculacaoNfeService, AutoVinculoService],
})
export class VinculacaoNfeModule {}
