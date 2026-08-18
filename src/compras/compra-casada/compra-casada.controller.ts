import {
  Controller,
  Get,
  Post,
  Param,
  ParseIntPipe,
  UploadedFile,
  UseInterceptors,
  Body,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiConsumes,
  ApiBody,
  ApiParam,
} from '@nestjs/swagger';

import { CompraCasadaFornecedoresService } from './compra-casada.service';
import { FornecedoresPorProdutoDto } from './dto/fornecedor-subgrupo.dto';

@ApiTags('Compra - Casada')
@Controller('compra-casada')
export class CompraCasadaController {
  constructor(
    private readonly fornecedoresService: CompraCasadaFornecedoresService,
  ) {}

  @Get('fornecedores/:pro_codigo')
  @ApiOperation({
    summary: 'Lista os fornecedores do subgrupo de um produto',
    description:
      'A partir do PRO_CODIGO, resolve o subgrupo do produto (Stage_Produtos -> ' +
      'Stage_ProdutosSubgrupos) e retorna, da staging do BI ' +
      '(Stage_FornecedorSubgrupos), todos os fornecedores que já venderam produtos ' +
      'desse subgrupo, com contato (telefone, celular, observações), UF e a data da ' +
      'última compra. Cada fornecedor traz também, ao vivo do ERP, a data e o valor ' +
      'unitário da última compra DESTE produto (ultima_compra_produto / ' +
      'unitario_produto, null quando nunca comprou). ' +
      'Ordenado pela última compra do subgrupo (mais recente primeiro).',
  })
  @ApiParam({
    name: 'pro_codigo',
    type: Number,
    description: 'Código do produto (PRO_CODIGO)',
    example: 47386,
  })
  @ApiResponse({
    status: 200,
    description: 'Lista de fornecedores do subgrupo',
    type: FornecedoresPorProdutoDto,
  })
  @ApiResponse({
    status: 404,
    description: 'Produto não encontrado ou sem subgrupo cadastrado',
  })
  fornecedoresPorProduto(
    @Param('pro_codigo', ParseIntPipe) proCodigo: number,
  ): Promise<FornecedoresPorProdutoDto> {
    return this.fornecedoresService.porProduto(proCodigo);
  }

}