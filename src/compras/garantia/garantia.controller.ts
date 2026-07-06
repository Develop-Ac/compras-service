import { Controller, Get, Param } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { GarantiaService } from './garantia.service';

@ApiTags('Compras - Garantia')
@Controller('garantia')
export class GarantiaController {
  constructor(private readonly service: GarantiaService) {}

  // GET /compras/garantia/pedido/:pedidoId
  @Get('pedido/:pedidoId')
  @ApiOperation({
    summary: 'Garantias pendentes do fornecedor do pedido',
    description:
      'Para o fornecedor do pedido (e seu grupo), casa o CNPJ com o cadastro de ' +
      'CLIENTES e lista os títulos de contas a receber IC (Itens para Compra) e RET ' +
      '(Retorno) em aberto (STATUS=0, sem DATA_PAGTO), com os produtos de cada NFS. ' +
      'Cadastro/financeiro vêm do SQL Server (Stage_*); os itens da NFS vêm do ERP.',
  })
  @ApiParam({ name: 'pedidoId', description: 'com_pedido.id' })
  @ApiResponse({ status: 200, description: 'Garantias listadas (tem_garantia indica se há pendências).' })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  async garantiasDoPedido(@Param('pedidoId') pedidoId: string) {
    return this.service.garantiasDoPedido(pedidoId);
  }
}
