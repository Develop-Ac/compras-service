import { Module } from '@nestjs/common';
import { PedidosLogsController } from './pedidos.controller';
import { PedidosLogsService } from './pedidos.service';
import { PedidosLogsRepository } from './pedidos.repository';
import { PrismaService } from 'src/prisma/prisma.service';

@Module({
  controllers: [PedidosLogsController],
  providers: [PedidosLogsService, PedidosLogsRepository, PrismaService],
})
export class PedidosLogsModule {}
