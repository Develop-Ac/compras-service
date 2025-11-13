import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { PedidoService } from './pedido.service';
import { PedidoRepository } from './pedido.repository';

// Se você já tiver um PrismaModule que exporta PrismaService, importe-o.
// Caso NÃO tenha, descomente a importação direta do PrismaService e adicione-o em providers.
import { PrismaService } from '../../../prisma/prisma.service';

// Módulo que criamos acima para expor o OpenQueryService
import { OpenQueryModule } from '../../../shared/database/openquery/openquery.module';
import { PedidoController } from './pedido.controller';

@Module({
  imports: [
    ConfigModule,       // garante ConfigService disponível (usado pelo OpenQueryService)
    OpenQueryModule,    // <-- essencial: disponibiliza OpenQueryService para este módulo
    // PrismaModule,    // (opcional) se existir um PrismaModule que exporta PrismaService
  ],
  controllers: [PedidoController],
  providers: [
    PedidoService,
    PedidoRepository,
    PrismaService, // se você usa PrismaModule que já exporta PrismaService, remova esta linha
  ],
  exports: [PedidoService],
})
export class PedidoModule {}
