import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { FornecedorGrupoModule } from '../fornecedor-grupo/fornecedor-grupo.module';
import { GarantiaController } from './garantia.controller';
import { GarantiaService } from './garantia.service';
import { GarantiaRepository } from './garantia.repository';

@Module({
  // OpenQueryModule -> SQL Server BI (Stage_*) + OPENQUERY([CONSULTA]) ao Firebird;
  // FornecedorGrupoModule -> expandGrupo (matriz/filiais compartilham CNPJs).
  imports: [PrismaModule, OpenQueryModule, FornecedorGrupoModule],
  controllers: [GarantiaController],
  providers: [GarantiaService, GarantiaRepository],
  exports: [GarantiaService],
})
export class GarantiaModule {}
