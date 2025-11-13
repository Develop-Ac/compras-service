import { Module } from '@nestjs/common';
import { RouterModule } from '@nestjs/core';
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

@Module({
imports: [
    PrismaModule,
    OpenQueryHttpModule,
    CotacaoModule,
    FornecedorModule,
    CotacaoSyncModule,
    PedidoModule,
    S3Module,
    NotaFiscalModule,
    kanbanModule,

    // ⬇️ Prefixa *somente* esses módulos com /compras
    RouterModule.register([
      { path: 'compras', module: OpenQueryHttpModule }, 
      { path: 'compras', module: CotacaoModule }, 
      { path: 'compras', module: FornecedorModule },
      { path: 'compras', module: CotacaoSyncModule },
      { path: 'compras', module: PedidoModule },
      { path: 'compras', module: NotaFiscalModule },
      { path: 'compras', module: kanbanModule }
    ]),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
