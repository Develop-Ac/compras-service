import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { PrecoVigenteDto } from './dto/preco-vigente.dto';
import { TabelasPrecoService } from './tabelas-preco.service';

@ApiTags('Compras - Precificação')
@Controller('precificacao/tabelas-preco')
export class TabelasPrecoController {
  constructor(private readonly service: TabelasPrecoService) {}

  // GET /compras/precificacao/tabelas-preco?codigos=38883,38884
  @Get()
  @ApiOperation({
    summary: 'Três tabelas de preço vigentes de vários produtos (lote)',
    description:
      'Uma ida ao ERP e uma ao espelho para o lote inteiro. Cada produto devolve as ' +
      'três tabelas (varejo, atacado especial e atacado) rotuladas, com a origem de ' +
      'cada valor e a idade do espelhamento. Códigos desconhecidos não voltam na lista.',
  })
  @ApiQuery({
    name: 'codigos',
    description: 'pro_codigo separados por vírgula (máx. 500).',
    example: '38883,38884',
  })
  @ApiResponse({ status: 200, type: [PrecoVigenteDto] })
  @ApiResponse({ status: 400, description: 'Nenhum código válido ou lote acima do limite.' })
  async emLote(@Query('codigos') codigos: string): Promise<PrecoVigenteDto[]> {
    const lista = String(codigos ?? '')
      .split(',')
      .map((c) => c.trim())
      .filter(Boolean);
    return this.service.precosVigentesLote(lista);
  }

  // GET /compras/precificacao/tabelas-preco/:proCodigo
  @Get(':proCodigo')
  @ApiOperation({
    summary: 'Três tabelas de preço vigentes de um produto',
    description:
      'Devolve varejo (produtos.preco_venda), atacado especial (produtos.preco2) e ' +
      'atacado (produtos.preco5) — este último não espelhado em com_fifo_completo e ' +
      'por isso lido direto do ERP, somente leitura. Traz também a data/hora da ' +
      'última atualização do espelhamento e sua idade em horas.',
  })
  @ApiParam({ name: 'proCodigo', description: 'Código do produto no ERP.', example: 38883 })
  @ApiResponse({ status: 200, type: PrecoVigenteDto })
  @ApiResponse({ status: 400, description: 'pro_codigo inválido.' })
  @ApiResponse({ status: 404, description: 'Produto não encontrado no ERP nem no espelho.' })
  async porProduto(@Param('proCodigo') proCodigo: string): Promise<PrecoVigenteDto> {
    return this.service.precosVigentes(proCodigo);
  }
}
