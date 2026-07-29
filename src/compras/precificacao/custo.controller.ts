import { Controller, Get, Param } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { CustoService } from './custo.service';
import { CustoComposicaoDto } from './dto/custo-item.dto';

@ApiTags('Compras - Precificação')
@Controller('precificacao/custo')
export class CustoController {
  constructor(private readonly service: CustoService) {}

  // GET /compras/precificacao/custo/pedido/:pedidoId
  @Get('pedido/:pedidoId')
  @ApiOperation({
    summary: 'Custo composto de todos os itens de um pedido (5 parcelas discriminadas)',
    description:
      'Devolve, para cada item do pedido, o custo unitário composto e as parcelas ' +
      'unitário, ICMS ST, ICMS ST complementar, IPI e frete (apenas quando o CT-e é ' +
      'por nossa conta). Uma única requisição cobre a nota inteira, incluindo notas ' +
      'com centenas de itens.',
  })
  @ApiParam({ name: 'pedidoId', description: 'id (cuid) de com_pedido.' })
  @ApiResponse({ status: 200, type: CustoComposicaoDto })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  async porPedido(@Param('pedidoId') pedidoId: string): Promise<CustoComposicaoDto> {
    return this.service.custoDoPedido(pedidoId);
  }

  // GET /compras/precificacao/custo/nfe/:chaveNfe
  @Get('nfe/:chaveNfe')
  @ApiOperation({
    summary: 'Custo composto dos itens cobertos por uma NF-e',
    description:
      'Resolve os pedidos com vínculo confirmado para a chave e devolve uma composição ' +
      'por pedido (a mesma NF pode ser repartida entre pedidos).',
  })
  @ApiParam({ name: 'chaveNfe', description: 'Chave de acesso da NF-e (44 dígitos).' })
  @ApiResponse({ status: 200, type: [CustoComposicaoDto] })
  @ApiResponse({ status: 404, description: 'Chave inválida ou sem vínculo confirmado.' })
  async porNota(@Param('chaveNfe') chaveNfe: string): Promise<CustoComposicaoDto[]> {
    return this.service.custoDaNota(chaveNfe);
  }
}
