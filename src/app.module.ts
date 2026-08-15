import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { RouterModule } from '@nestjs/core';
import { ScheduleModule } from '@nestjs/schedule';
<<<<<<< HEAD
import { ErpApiModule } from './shared/erp-api/erp-api.module';
=======
import { PrometheusModule } from '@willsoto/nestjs-prometheus';

>>>>>>> 8a5c4fec92569d8a3e867045becd0399f8f364c2
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { OpenQueryHttpModule } from './compras/cotacao/openquery/openquery.module';
import { CotacaoModule } from './compras/cotacao/cotacao.module';
import { FornecedorModule } from './compras/cotacao/fornecedor/fornecedor.module';
import { CotacaoSyncModule } from './compras/cotacao/cotacao-sync/cotacao-sync.module';
import { PedidoModule } from './compras/cotacao/pedido/pedido.module';
import { S3Module } from './storage/s3.module';
import { NotaFiscalModule } from './compras/nota fiscal/nota fiscal/notaFiscal.module';
import { kanbanModule } from './compras/kanban/kanban.module';
import { ItensModule } from './compras/itens/intes.module';
import { RabbitMqModule } from './compras/cotacao/job/rabbitmq.module';
import { PedidosLogsModule } from './compras/logs/pedidos/pedidos.module';
import { VinculacaoNfeModule } from './compras/vicunlacao-nfe/vinculacao-nfe.module';
import { FornecedorGrupoModule } from './compras/fornecedor-grupo/fornecedor-grupo.module';
import { GarantiaModule } from './compras/garantia/garantia.module';
import { CompraCasadaModule } from './compras/compra-casada/compra-casada.module';

@Module({
imports: [
    // Global: o ErpApiService lê ERP_API_URL/TOKEN e não deve depender de qual
    // outro módulo chamou forRoot antes dele.
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    // Leitura do ERP sem passar pelo SQL Server (global, como o OpenQuery).
    ErpApiModule,
    OpenQueryHttpModule,
    CotacaoModule,
    FornecedorModule,
    CotacaoSyncModule,
    PedidoModule,
    S3Module,
    NotaFiscalModule,
    kanbanModule,
    ItensModule,
    PedidosLogsModule,
    RabbitMqModule,
    VinculacaoNfeModule,
    FornecedorGrupoModule,
    GarantiaModule,
    CompraCasadaModule,

    PrometheusModule.register({
      defaultMetrics: { enabled: true }, // CPU, memória, event loop, GC
    }),

    // ⬇️ Prefixa *somente* esses módulos com /compras
    RouterModule.register([
      { path: 'compras', module: OpenQueryHttpModule }, 
      { path: 'compras', module: CotacaoModule }, 
      { path: 'compras', module: FornecedorModule },
      { path: 'compras', module: CotacaoSyncModule },
      { path: 'compras', module: PedidoModule },
      { path: 'compras', module: NotaFiscalModule },
      { path: 'compras', module: kanbanModule },
      { path: 'compras', module: ItensModule },
      { path: 'compras', module: PedidosLogsModule },
      { path: 'compras', module: VinculacaoNfeModule },
      { path: 'compras', module: FornecedorGrupoModule },
      { path: 'compras', module: GarantiaModule },
      { path: 'compras', module: CompraCasadaModule }
    ]),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
