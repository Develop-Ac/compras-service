import { Module } from '@nestjs/common';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { PrismaModule } from '../../prisma/prisma.module';
import { OpenQueryHttpModule } from '../cotacao/openquery/openquery.module';
import { VinculacaoNfeController } from './vinculacao-nfe.controller';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';
import { AutoVinculoService } from './auto-vinculo.service';
import { PedidoTransporteCron } from './pedido-transporte.cron';
import { NotaFiscalRepository } from '../nota fiscal/nota fiscal/notaFiscal.repository';
import { FornecedorGrupoModule } from '../fornecedor-grupo/fornecedor-grupo.module';
import { GarantiaModule } from '../garantia/garantia.module';
import { AvisosClientService } from '../avisos/avisos-client.service';

@Module({
  // OpenQueryModule -> OpenQueryService (Firebird via MSSQL); OpenQueryHttpModule
  // exporta ConsultaOpenqueryRepository (fornecedor por for_codigo); PrismaModule -> Prisma.
  // FornecedorGrupoModule -> compartilhamento de referências entre fornecedores relacionados.
  // GarantiaModule -> recalcula o resumo de garantia do pedido ao salvar a NF.
  imports: [OpenQueryModule, OpenQueryHttpModule, PrismaModule, FornecedorGrupoModule, GarantiaModule],
  controllers: [VinculacaoNfeController],
  providers: [
    VinculacaoNfeService,
    VinculacaoNfeRepository,
    AutoVinculoService,
    PedidoTransporteCron,
    // NotaFiscalRepository só depende de OpenQueryService (já disponível em OpenQueryModule).
    NotaFiscalRepository,
    AvisosClientService,
  ],
  exports: [VinculacaoNfeService, AutoVinculoService],
})
export class VinculacaoNfeModule {}
