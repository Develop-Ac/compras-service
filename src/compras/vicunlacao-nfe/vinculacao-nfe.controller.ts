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
import { AutoVinculoService } from './auto-vinculo.service';
import { VincularNfeDto } from './dto/vincular-nfe.dto';
import { SalvarVinculoDto } from './dto/salvar-vinculo.dto';
import { NfLancadaDto } from './dto/nf-lancada.dto';
import { VincularItemDto } from './dto/vincular-item.dto';

@ApiTags('Compras - Vinculação NF-e')
@Controller('vinculacao-nfe')
export class VinculacaoNfeController {
  constructor(
    private readonly service: VinculacaoNfeService,
    private readonly autoVinculo: AutoVinculoService,
  ) {}

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

  // POST /compras/vinculacao-nfe/nf-lancada
  // IMPORTANTE: rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('nf-lancada')
  @HttpCode(200)
  @ApiOperation({
    summary: 'NF lançada no ERP → marca pedidos vinculados como Entregue',
    description:
      'Recebe um lote de NF-e que viraram LANCADA no ERP (chave + dt_entrada). ' +
      'Para cada chave acha os vínculos confirmados e marca os pedidos vinculados ' +
      "como 'Entregue' (sem rebaixar 'Cancelado'), gravando data_recebimento " +
      '(= dt_entrada, ou now() se ausente). Idempotente. Retorna { atualizados, pedidos }.',
  })
  @ApiBody({ type: NfLancadaDto })
  @ApiResponse({ status: 200, description: 'Pedidos atualizados para Entregue.' })
  async nfLancada(@Body() body: NfLancadaDto) {
    return this.service.nfLancada(body?.lancadas ?? []);
  }

  // POST /compras/vinculacao-nfe/item/:itemId/vincular
  // Rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('item/:itemId/vincular')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Vincula manualmente, pela conferência, um item da NF (sem pedido) a um produto do pedido',
  })
  @ApiParam({ name: 'itemId', description: 'com_pedido_nfe_vinculo_item.id (item tipo xml_sem_vinculo)' })
  @ApiBody({ type: VincularItemDto })
  @ApiResponse({ status: 200, description: 'Item vinculado e status do pedido recalculado.' })
  async vincularItemConferencia(
    @Param('itemId') itemId: string,
    @Body() body: VincularItemDto,
  ) {
    return this.service.vincularItemConferencia(itemId, body);
  }

  // POST /compras/vinculacao-nfe/item/:itemId/desvincular
  // Rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('item/:itemId/desvincular')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Desfaz o vínculo de um item na conferência (volta para XML sem vínculo)',
  })
  @ApiParam({ name: 'itemId', description: 'com_pedido_nfe_vinculo_item.id (item tipo vinculado)' })
  @ApiResponse({ status: 200, description: 'Item desvinculado e status do pedido recalculado.' })
  async desvincularItemConferencia(@Param('itemId') itemId: string) {
    return this.service.desvincularItemConferencia(itemId);
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

  // GET /compras/vinculacao-nfe/conferencia/:pedidoId
  // IMPORTANTE: declarado ANTES de /:vinculoId para não ser sombreado.
  @Get('conferencia/:pedidoId')
  @ApiOperation({
    summary: 'Conferência de fechamento de um pedido (pedido × faturado)',
    description:
      'Devolve uma linha por item do pedido comparando o que foi pedido com o que foi ' +
      'faturado (somando todas as NF-e vinculadas confirmadas), com totais, situação por ' +
      'item e os itens das NF-e que não estão no pedido. Somente leitura.',
  })
  @ApiParam({ name: 'pedidoId', description: 'com_pedido.id' })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  async conferencia(@Param('pedidoId') pedidoId: string) {
    return this.service.conferenciaPorItem(pedidoId);
  }

  // POST /compras/vinculacao-nfe/auto/varredura
  // IMPORTANTE: rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('auto/varredura')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Dispara manualmente a varredura de auto-vínculo (sugestões)',
    description:
      'Executa a mesma rotina do job periódico: varre pedidos abertos, busca NF-e ' +
      'candidatas (fornecedor + data), roda o casamento e, com cobertura >= 30%, cria ' +
      "sugestões (confirmado=false, origem='auto') marcando o pedido como 'Vínculo sugerido'. " +
      'Retorna um resumo { pedidos_varridos, sugestoes_criadas, truncado, limite }.',
  })
  @ApiResponse({ status: 200, description: 'Varredura executada.' })
  async varreduraManual() {
    return this.autoVinculo.executarVarredura();
  }

  // POST /compras/vinculacao-nfe/pedido/:pedidoId/ipi-no-valor  (body: { ipi_no_valor })
  // IMPORTANTE: rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('pedido/:pedidoId/ipi-no-valor')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Liga/desliga "IPI incluso no valor unitário" do pedido',
    description:
      'Quando ligado, a Conferência Pedido × Faturado soma o IPI por unidade da NF ' +
      'ao valor faturado antes de comparar com o valor do pedido (que já inclui IPI).',
  })
  @ApiParam({ name: 'pedidoId', description: 'com_pedido.id' })
  @ApiResponse({ status: 200, description: 'Flag atualizado.' })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  async setIpiNoValor(
    @Param('pedidoId') pedidoId: string,
    @Body() body: { ipi_no_valor?: boolean },
  ) {
    return this.service.setIpiNoValor(pedidoId, Boolean(body?.ipi_no_valor));
  }

  // POST /compras/vinculacao-nfe/resumo  (body: { chaves: string[] })
  // IMPORTANTE: rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Post('resumo')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Resumo da vinculação (confirmada) por chave de NF — para a listagem',
    description:
      'Recebe um lote de chaves de NF-e e devolve, por chave, o total de itens do ' +
      'XML, quantos foram vinculados a um pedido e o percentual de conclusão ' +
      '(somente vínculos confirmados). Retorna um mapa { [chave]: { total_itens, ' +
      'vinculados, percentual, pedidos } }.',
  })
  @ApiResponse({ status: 200, description: 'Resumo calculado.' })
  async resumoPorChaves(@Body() body: { chaves?: string[] }) {
    return this.service.resumoVinculacaoPorChaves(body?.chaves ?? []);
  }

  // GET /compras/vinculacao-nfe/por-chave/:chave
  // IMPORTANTE: rota específica declarada ANTES de /:vinculoId para não ser sombreada.
  @Get('por-chave/:chave')
  @ApiOperation({
    summary: 'Vinculação (confirmada) de uma NF, item a item, com o pedido de cada item',
    description:
      'Para a tela de detalhe da NF: devolve, para cada item do XML que foi vinculado, ' +
      'a qual pedido (pedido_id + pedido_cotacao) ele pertence, além do resumo de conclusão. ' +
      'Somente vínculos confirmados.',
  })
  @ApiParam({ name: 'chave', description: 'Chave de acesso da NF-e (44 dígitos)' })
  async vinculacaoPorChave(@Param('chave') chave: string) {
    return this.service.vinculacaoPorChave(chave);
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

  // POST /compras/vinculacao-nfe/:vinculoId/confirmar
  @Post(':vinculoId/confirmar')
  @HttpCode(200)
  @ApiOperation({
    summary: 'Confirma um vínculo (sugestão) e recalcula o status do pedido',
    description:
      'Marca o vínculo como confirmado (confirmado=true) e dispara o recálculo ' +
      'do status do pedido (Faturado / Faturado parcialmente). Retorna o status novo.',
  })
  @ApiParam({ name: 'vinculoId', description: 'com_pedido_nfe_vinculo.id' })
  @ApiResponse({ status: 200, description: 'Vínculo confirmado e status recalculado.' })
  @ApiResponse({ status: 404, description: 'Vínculo não encontrado.' })
  async confirmar(@Param('vinculoId') vinculoId: string) {
    return this.service.confirmarVinculo(vinculoId);
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
