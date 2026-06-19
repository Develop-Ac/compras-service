import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { FornecedorGrupoController } from './fornecedor-grupo.controller';
import { FornecedorGrupoService } from './fornecedor-grupo.service';
import { FornecedorGrupoRepository } from './fornecedor-grupo.repository';

@Module({
  // PrismaModule -> PrismaService; OpenQueryModule -> OpenQueryService (SQL Server BI / Stage_Fornecedores)
  imports: [PrismaModule, OpenQueryModule],
  controllers: [FornecedorGrupoController],
  providers: [FornecedorGrupoService, FornecedorGrupoRepository],
  exports: [FornecedorGrupoService],
})
export class FornecedorGrupoModule {}
