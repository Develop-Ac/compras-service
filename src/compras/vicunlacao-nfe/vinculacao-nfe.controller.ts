import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { ApiBody, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VincularNfeDto } from './dto/vincular-nfe.dto';

@ApiTags('Compras - Vinculação NF-e')
@Controller('vinculacao-nfe')
export class VinculacaoNfeController {
  constructor(private readonly service: VinculacaoNfeService) {}

  // POST /compras/vinculacao-nfe  (body: { pedido, nfe })
  @Post()
  @HttpCode(200)
  @ApiOperation({
    summary: 'Vincula itens do XML da NF-e com os itens de um pedido de cotação',
    description:
      'Busca o XML da NF-e (chave) e os itens da cotação (Firebird + com_cotacao_itens_for), ' +
      'vincula por referência/código (com fallback de análise semântica) e devolve 3 listas: ' +
      'itens vinculados (com a quantidade do pedido), itens do XML sem vínculo e produtos do ' +
      'pedido (com_pedido_itens) sem vínculo.',
  })
  @ApiBody({ type: VincularNfeDto })
  @ApiResponse({ status: 200, description: 'Vinculação realizada com sucesso.' })
  @ApiResponse({ status: 404, description: 'NF-e ou XML não encontrados para a chave informada.' })
  async vincular(@Body() body: VincularNfeDto) { 
    const pedido = Number(body?.pedido);
    const nfe = String(body?.nfe ?? '').trim();
    return this.service.vincular(pedido, nfe);
  }
}
