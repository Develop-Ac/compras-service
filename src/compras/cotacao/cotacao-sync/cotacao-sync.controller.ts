// src/cotacao-sync/cotacao-sync.controller.ts
import { Controller, Get, Param, ParseIntPipe } from '@nestjs/common';
import { CotacaoSyncService } from './cotacao-sync.service';
import { ApiOperation, ApiTags } from '@nestjs/swagger';

@ApiTags('Compras - Pedidos de Cotação')
@Controller('cotacao-sync')
export class CotacaoSyncController {
  constructor(private readonly service: CotacaoSyncService) {}

  @Get(':pedido_cotacao')
  @ApiOperation({ summary: 'Sincroniza e lista por pedido_cotacao' })
  async syncAndList(
    @Param('pedido_cotacao', ParseIntPipe) pedido_cotacao: number,
  ) {
    const fornecedores = await this.service.syncByPedido(pedido_cotacao);
    return { ok: true, pedido_cotacao, fornecedores };
  }
}
