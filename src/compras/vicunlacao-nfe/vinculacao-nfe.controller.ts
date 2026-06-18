import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  Post,
} from '@nestjs/common';
import { ApiBody, ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VincularNfeDto } from './dto/vincular-nfe.dto';
import { SalvarVinculoDto } from './dto/salvar-vinculo.dto';

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

  // POST /compras/vinculacao-nfe/salvar
  @Post('salvar')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Salva (upsert) a conferência NF-e × Pedido (cabeçalho + itens tipados)',
    description:
      'Grava o snapshot da conferência. Faz upsert do cabeçalho pelo par ' +
      '(pedido_id, chave_nfe) e substitui os itens (deleteMany + createMany) em transação. ' +
      'Retorna o cabeçalho salvo com a contagem de itens por tipo.',
  })
  @ApiBody({ type: SalvarVinculoDto })
  @ApiResponse({ status: 200, description: 'Vínculo salvo com sucesso.' })
  async salvar(@Body() body: SalvarVinculoDto) {
    return this.service.salvarVinculo(body);
  }

  // GET /compras/vinculacao-nfe/pedido/:pedidoId
  @Get('pedido/:pedidoId')
  @ApiOperation({
    summary: 'Lista as NF-e já vinculadas de um pedido',
    description:
      'Retorna os cabeçalhos (sem itens) das conferências salvas do pedido, com totais por tipo.',
  })
  @ApiParam({ name: 'pedidoId', description: 'com_pedido.id' })
  async listarPorPedido(@Param('pedidoId') pedidoId: string) {
    return this.service.listarVinculosDoPedido(pedidoId);
  }

  // GET /compras/vinculacao-nfe/:vinculoId
  @Get(':vinculoId')
  @ApiOperation({
    summary: 'Carrega uma conferência salva',
    description:
      'Lê cabeçalho + itens e remonta o mesmo shape da vinculação calculada ' +
      '(totais + vinculados / xml_sem_vinculo / pedido_sem_vinculo).',
  })
  @ApiParam({ name: 'vinculoId', description: 'com_pedido_nfe_vinculo.id' })
  @ApiResponse({ status: 404, description: 'Vínculo não encontrado.' })
  async carregar(@Param('vinculoId') vinculoId: string) {
    return this.service.carregarVinculo(vinculoId);
  }

  // DELETE /compras/vinculacao-nfe/:vinculoId
  @Delete(':vinculoId')
  @ApiOperation({
    summary: 'Remove uma conferência salva',
    description: 'Apaga o cabeçalho do vínculo; o cascade remove os itens.',
  })
  @ApiParam({ name: 'vinculoId', description: 'com_pedido_nfe_vinculo.id' })
  @ApiResponse({ status: 404, description: 'Vínculo não encontrado.' })
  async remover(@Param('vinculoId') vinculoId: string) {
    return this.service.removerVinculo(vinculoId);
  }
}
