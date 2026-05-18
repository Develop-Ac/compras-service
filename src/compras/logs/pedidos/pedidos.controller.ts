import { Controller, Get, Param } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { PedidosLogsService } from './pedidos.service';

@ApiTags('Compras - Logs')
@Controller('logs/pedidos')
export class PedidosLogsController {
  constructor(private readonly pedidosLogsService: PedidosLogsService) {}

  @Get('gerencial/:pedido')
  async getGerencialLogsByPedido(@Param('pedido') pedido: string) {
    return this.pedidosLogsService.getGerencialLogsByPedido(pedido);
  }
}
