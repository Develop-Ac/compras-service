// src/pedido/pedido.controller.ts
import { Body, Controller, Get, Param, Post, Query, Res } from '@nestjs/common';
import { PedidoService } from './pedido.service';
import { CreatePedidoDto } from './dto/create-pedido.dto';
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
