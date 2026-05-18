import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { Delete, Param } from '@nestjs/common';
import { FornecedorService } from './fornecedor.service';
import { CreateFornecedorDto } from './fornecedor.dto';
import { 
  ApiOperation, 
  ApiTags, 
  ApiQuery,
  ApiParam,
  ApiOkResponse, 
  ApiCreatedResponse,
  ApiBadRequestResponse 
} from '@nestjs/swagger';

@ApiTags('Compras - Fornecedores')
@Controller('fornecedor')
export class FornecedorController {
  constructor(private readonly service: FornecedorService) {}

  @Post()
  @ApiOperation({ 
    summary: 'Cria ou atualiza fornecedor',
    description: 'Cria um novo fornecedor ou atualiza um existente, sincronizando com o sistema Next.js'
  })
  @ApiCreatedResponse({
    description: 'Fornecedor criado/atualizado com sucesso'
  })
  @ApiBadRequestResponse({
    description: 'Dados inválidos fornecidos',
    example: { statusCode: 400, message: 'Validation failed', error: 'Bad Request' }
  })
  async create(@Body() dto: CreateFornecedorDto) {
    // executa upsert local **e** envia ao Next
    return this.service.upsertLocalEEnviarParaNext(dto);
  }

  // GET /fornecedor?pedido_cotacao=3957
  @Get()
  @ApiOperation({ 
    summary: 'Lista fornecedores por pedido de cotação',
    description: 'Retorna todos os fornecedores associados a um pedido de cotação específico'
  })
  @ApiQuery({
    name: 'pedido_cotacao',
    description: 'ID do pedido de cotação',
    example: '3957',
    required: true,
    type: 'string'
  })
  @ApiOkResponse({
    description: 'Lista de fornecedores retornada com sucesso',
    example: {
      data: [
        { id: 1, nome: 'Fornecedor A', cnpj: '12.345.678/0001-90' },
        { id: 2, nome: 'Fornecedor B', cnpj: '98.765.432/0001-10' }
      ],
      total: 2
    }
  })
  @ApiBadRequestResponse({
    description: 'ID do pedido de cotação inválido'
  })
  async list(@Query('pedido_cotacao') pedido: string) {
    const n = Number(pedido);
    if (!Number.isFinite(n)) return { data: [], total: 0 };
    const data = await this.service.listarFornecedoresPorPedido(n);
    return { data, total: data.length };
  }

  // GET /fornecedor/:pedido_cotacao/itens -> lista itens da cotação
  @Get(':pedido_cotacao/itens')
  @ApiOperation({
    summary: 'Lista itens da cotação por pedido',
    description: 'Retorna todos os itens da tabela com_cotacao_itens filtrados pelo pedido_cotacao',
  })
  @ApiParam({
    name: 'pedido_cotacao',
    description: 'ID do pedido de cotação',
    example: '3957',
    type: 'string',
  })
  @ApiOkResponse({
    description: 'Lista de itens retornada com sucesso',
  })
  async listarItens(@Param('pedido_cotacao') pedido_cotacao: string) {
    const n = Number(pedido_cotacao);
    if (!Number.isFinite(n)) return [];
    return this.service.listarCotacaoItens(n);
  }

  /**
   * DELETE /fornecedor/:pedido_cotacao/:for_codigo
   * Altera trash de 0 para 1 para o fornecedor do pedido de cotação
   */
  @Delete(':pedido_cotacao/:for_codigo')
  async deleteFornecedor(
    @Param('pedido_cotacao') pedido_cotacao: number,
    @Param('for_codigo') for_codigo: number
  ) {
    await this.service.trashFornecedor(pedido_cotacao, for_codigo);
    return { success: true };
  }
}
