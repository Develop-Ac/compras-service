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
