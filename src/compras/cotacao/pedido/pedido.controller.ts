
// ...mantém apenas a definição única do controller e imports necessários...
// src/pedido/pedido.controller.ts
import { Body, Controller, Get, Param, Post, Put, Query, Res } from '@nestjs/common';
import { PedidoService } from './pedido.service';
import { CreatePedidoDto } from './dto/create-pedido.dto';
import { AutorizacaoItemDto } from './dto/autorizacao-item.dto';
import express from 'express';
import type { Response as ExpressResponse } from 'express';
import { 
  ApiOperation, 
  ApiTags, 
  ApiParam, 
  ApiQuery, 
  ApiOkResponse, 
  ApiCreatedResponse,
  ApiBadRequestResponse,
  ApiProduces
} from '@nestjs/swagger';

@ApiTags('Compras - Pedidos de Cotação')
@Controller('pedido')
export class PedidoController {
  constructor(private readonly service: PedidoService) {}

    // GET /pedido  -> listagem leve
  @Get()
  @ApiOperation({ 
    summary: 'Lista pedidos',
    description: 'Retorna uma listagem resumida de todos os pedidos'
  })
  @ApiOkResponse({
    description: 'Lista de pedidos retornada com sucesso'
  })
  async listagem() {
    return this.service.listagem();
  } 

  // GET /pedido/sincronizacao/:id  -> retorna pedido e itens completos
  @Get('sincronizacao/:id')
  @ApiOperation({
    summary: 'Busca pedido completo para sincronização',
    description: 'Retorna o pedido e todos os itens pelo id'
  })
  @ApiParam({
    name: 'id',
    description: 'ID do pedido',
    example: '123'
  })
  async getPedidoSincronizacao(@Param('id') id: string) {
    return this.service.buscarPedidoSincronizacao(id);
  }

  // GET /pedido/gerencial/:id  -> retorna pedido e itens para gerencial
  @Get('gerencial/:id')
  @ApiOperation({
    summary: 'Busca pedido completo para área gerencial',
    description: 'Retorna o pedido completo com todos os dados do com_pedido e com_pedido_itens'
  })
  @ApiParam({
    name: 'id',
    description: 'ID do pedido',
    example: 'cm123abc'
  })
  @ApiOkResponse({
    description: 'Pedido retornado com sucesso'
  })
  async getPedidoGerencial(@Param('id') id: string) {
    return this.service.buscarPedidoGerencial(id);
  }

  // GET /pedido/:id  -> PDF
  @Get(':id')
  @ApiOperation({ 
    summary: 'Gera PDF do pedido',
    description: 'Gera e retorna um arquivo PDF com os detalhes do pedido'
  })
  @ApiParam({
    name: 'id',
    description: 'ID do pedido',
    example: '123'
  })
  @ApiQuery({
    name: 'marca',
    description: 'Incluir marca no PDF (true/false)',
    required: false,
    example: 'true'
  })
  @ApiProduces('application/pdf')
  @ApiOkResponse({
    description: 'PDF gerado com sucesso',
    content: {
      'application/pdf': {
        schema: {
          type: 'string',
          format: 'binary'
        }
      }
    }
  })
  @ApiBadRequestResponse({
    description: 'ID do pedido inválido'
  })
  async pdf(
    @Param('id') id: string,
    @Query('marca') marca: string | undefined,
    @Res() res: ExpressResponse, // << mesmo tipo que o service espera
  ) {
    const showMarca =
      marca == null ? true : /^(true|1|on|yes)$/i.test(String(marca).trim());

    // O service já dá pipe(res) e finaliza com doc.end()
    await this.service.gerarPdfPedidoExpress(res, id, { marca: showMarca });
  }

  // PUT /pedido/autorizacao/:id -> atualiza autorização de item
  @Put('autorizacao/:id')
  @ApiOperation({
    summary: 'Atualiza autorização de item do pedido',
    description: 'Atualiza o estado de autorização (carlos/renato) de um item específico do pedido'
  })
  @ApiParam({
    name: 'id',
    description: 'ID do pedido',
    example: 'cm123abc'
  })
  @ApiOkResponse({
    description: 'Autorização atualizada com sucesso'
  })
  @ApiBadRequestResponse({
    description: 'Dados inválidos fornecidos'
  })
  async updateAutorizacao(
    @Param('id') pedidoId: string,
    @Body() body: AutorizacaoItemDto
  ) {
    return this.service.atualizarAutorizacaoItem(
      pedidoId, 
      body.id, 
      body.coluna, 
      body.check
    );
  }


  @Post()
  @ApiOperation({ 
    summary: 'Cria ou atualiza pedido',
    description: 'Cria um novo pedido ou atualiza um existente'
  })
  @ApiCreatedResponse({
    description: 'Pedido criado/atualizado com sucesso'
  })
  @ApiBadRequestResponse({
    description: 'Dados inválidos fornecidos'
  })
  async create(@Body() body: CreatePedidoDto) {
    return this.service.createOrReplace(body);
  }
}
