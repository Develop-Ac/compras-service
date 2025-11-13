import { BadRequestException, Controller, Get, Param, ParseIntPipe, Query } from '@nestjs/common';
import { ConsultaOpenqueryService } from './openquery.service';
import { ApiOperation, ApiTags } from '@nestjs/swagger';

@ApiTags('Compras - Pedidos de Cotação')
@Controller('openquery')
export class OpenQueryController {
  constructor(private readonly service: ConsultaOpenqueryService) {}

  // GET /openquery/pedido/3957?empresa=3
  @Get('pedido/:pedido')
  @ApiOperation({ summary: 'Busca itens do pedido' })
  async getPedido(
    @Param('pedido', ParseIntPipe) pedido: number,
    @Query('empresa') empresaQ?: string,
  ) {
    const empresa = Number.isFinite(Number(empresaQ)) ? Number(empresaQ) : 3;
    const itens = await this.service.buscarPorEmpresaPedido(empresa, pedido);
    return {
      empresa,
      pedido_cotacao: pedido,
      total_itens: itens.length,
      itens,
    };
  }

  // GET /openquery/fornecedor/123
  @Get('fornecedor/:forCodigo')
  @ApiOperation({ summary: 'Busca fornecedor por codigo' })
  async getFornecedorPorCodigo(@Param('forCodigo', ParseIntPipe) forCodigo: number) {
    if (!Number.isFinite(forCodigo)) {
      throw new BadRequestException('forCodigo inválido.');
    }
    return this.service.buscarFornecedorPorCodigo(forCodigo);
  }
}
