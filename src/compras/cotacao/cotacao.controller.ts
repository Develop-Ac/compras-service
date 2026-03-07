
// src/compras/cotacao.controller.ts
import { Body, Controller, Get, Param, ParseIntPipe, Post, Query } from '@nestjs/common';
import { CotacaoService } from './cotacao.service';
import { CreateCotacaoDto } from './cotacao.dto';
import { 
  ApiOperation, 
  ApiTags, 
  ApiQuery, 
  ApiParam, 
  ApiOkResponse, 
  ApiCreatedResponse,
  ApiBadRequestResponse 
} from '@nestjs/swagger';

@ApiTags('Compras - Pedidos de Cotação')
@Controller('pedidos-cotacao')
export class CotacaoController {
  constructor(private service: CotacaoService) {}

    /**
   * GET /compras/pedidos-cotacao/:pedido/itens?empresa=3
   * Retorna cotação customizada (empresa, pedido_cotacao, total_itens, itens)
   */
  @Get(':pedido/itens')
  @ApiOperation({
    summary: 'Obtém cotação customizada',
    description: 'Retorna cotação no formato customizado (empresa, pedido_cotacao, total_itens, itens)'
  })
  @ApiParam({
    name: 'pedido',
    description: 'Número do pedido',
    example: 4131,
    type: 'number',
  })
  @ApiQuery({
    name: 'empresa',
    description: 'Código da empresa',
    example: 3,
    type: 'number',
    required: true,
  })
  @ApiOkResponse({
    description: 'Cotação customizada retornada com sucesso',
    schema: {
      example: {
        empresa: 3,
        pedido_cotacao: 4131,
        total_itens: 24,
        itens: [
          {
            PEDIDO_COTACAO: 4131,
            EMISSAO: '2026-02-10T00:00:00.000Z',
            PRO_CODIGO: 48293,
            PRO_DESCRICAO: 'ARRUELA CALOTA 13/14/15/16 KIT  (METAL)',
            MAR_DESCRICAO: 'SPORT INOX',
            REFERENCIA: 'ARR-01',
            UNIDADE: 'KT',
            QUANTIDADE: 40,
            DT_ULTIMA_COMPRA: '2025-10-03T00:00:00.000Z',
            emissao: null,
          },
        ],
      },
    },
  })
  async getCotacaoItens(
    @Param('pedido', ParseIntPipe) pedido: number,
    @Query('empresa', ParseIntPipe) empresa: number,
  ) {
    return this.service.getCotacaoItens(empresa, pedido);
  }

  @Get('proximo-indice')
  @ApiOperation({ summary: 'Retorna o próximo índice disponível da tabela com_cotacao' })
  @ApiOkResponse({ description: 'Próximo índice retornado com sucesso', schema: { example: { proximoIndice: 123 } } })
  async getProximoIndice() {
    const proximoIndice = await this.service.getNextIndice();
    return { proximoIndice };
  }

  // POST /compras/pedidos-cotacao
  @Post()
  @ApiOperation({ 
    summary: 'Cria ou atualiza cotação',
    description: 'Cria uma nova cotação ou atualiza uma existente baseada no pedido e empresa. Inclui suporte ao campo DT_ULTIMA_COMPRA para cada item.'
  })
  @ApiCreatedResponse({
    description: 'Cotação criada/atualizada com sucesso'
  })
  @ApiBadRequestResponse({
    description: 'Dados inválidos fornecidos',
    example: { statusCode: 400, message: 'Validation failed', error: 'Bad Request' }
  })
  async create(@Body() dto: CreateCotacaoDto) {
    console.log('cheguei')
    console.log(dto)
    return this.service.upsertCotacao(dto);
  }

  @Get()
  @ApiOperation({ 
    summary: 'Lista cotações com paginação',
    description: 'Lista todas as cotações com opções de filtro e paginação'
  })
  @ApiQuery({
    name: 'empresa',
    description: 'Código da empresa para filtrar',
    required: false,
    example: '3'
  })
  @ApiQuery({
    name: 'page',
    description: 'Página atual (inicia em 1)',
    required: false,
    example: '1'
  })
  @ApiQuery({
    name: 'pageSize',
    description: 'Número de itens por página',
    required: false,
    example: '20'
  })
  @ApiQuery({
    name: 'includeItems',
    description: 'Incluir itens das cotações na resposta',
    required: false,
    example: 'true'
  })
  @ApiOkResponse({
    description: 'Lista de cotações retornada com sucesso'
  })
  async getAll(
    @Query('empresa') empresaQ?: string,
    @Query('page') pageQ?: string,
    @Query('pageSize') pageSizeQ?: string,
    @Query('includeItems') includeItemsQ?: string,
  ) {
    const toNum = (v?: string) => (v != null && v !== '' ? Number(v) : NaN);

    const page = Number.isFinite(toNum(pageQ)) && toNum(pageQ)! > 0 ? toNum(pageQ)! : 1;
    const pageSize = Number.isFinite(toNum(pageSizeQ)) && toNum(pageSizeQ)! > 0 ? toNum(pageSizeQ)! : 20;
    const empresa = Number.isFinite(toNum(empresaQ)) ? toNum(empresaQ)! : undefined;
    const includeItems = (includeItemsQ ?? '').toLowerCase() === 'true';

    return this.service.listAll({ empresa, page, pageSize, includeItems });
  }

  // GET /compras/pedidos-cotacao/:pedido?empresa=3
  @Get(':pedido')
  @ApiOperation({ 
    summary: 'Obtém cotação por pedido e empresa',
    description: 'Busca uma cotação específica pelo número do pedido e empresa'
  })
  @ApiParam({
    name: 'pedido',
    description: 'Número do pedido',
    example: 123,
    type: 'number'
  })
  @ApiQuery({
    name: 'empresa',
    description: 'Código da empresa',
    example: 3,
    type: 'number',
    required: true
  })
  @ApiOkResponse({
    description: 'Cotação encontrada com sucesso'
  })
  @ApiBadRequestResponse({
    description: 'Pedido ou empresa inválidos',
    example: { statusCode: 400, message: 'Validation failed (numeric string is expected)', error: 'Bad Request' }
  })
  async getOne(
    @Param('pedido', ParseIntPipe) pedido: number,
    @Query('empresa', ParseIntPipe) empresa: number,
  ) {
    return this.service.getCotacao(empresa, pedido);
  }
}
