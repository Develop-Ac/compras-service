import { Module } from '@nestjs/common';
import { NotaFiscalController } from './notaFiscal.controller';
import { NotaFiscalService } from './notaFiscal.service';
import { NotaFiscalRepository } from './notaFiscal.repository';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';
import { PrismaService } from '../../../prisma/prisma.service';
import { FornecedorGrupoModule } from '../../fornecedor-grupo/fornecedor-grupo.module';

@Module({
  // FornecedorGrupoModule -> FornecedorGrupoService (CNPJs do grupo p/ filtrar NF-e disponíveis)
  imports: [FornecedorGrupoModule],
  controllers: [NotaFiscalController],
  providers: [NotaFiscalService, NotaFiscalRepository, OpenQueryService, PrismaService],
})
export class NotaFiscalModule {}