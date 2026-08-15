import { Controller, Get, Param, Query } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import { SerieTriagemDto, TriagemNotaDto } from './dto/triagem-nota.dto';
import { TriagemService } from './triagem.service';

/**
 * Triagem da nota entre automáticos e exceções (US-043 / T-030).
 *
 * **Sem guard, por convenção do container** (CODING_STANDARDS#convencoes-backend-compras-service).
 *
 * A nota inteira sai em **uma** requisição, no mesmo padrão da T-020/T-026. A rota consome a
 * proposta pelo contrato e a classifica — item por item, com o motivo do conjunto fechado de
 * sete — e devolve a **taxa de exceção da execução**, comparável ao alvo de M5 (≤ 33%).
 *
 * **Herda o 503 da régua** pela proposta que consome (DDL das faixas da T-024 pendente). Não
 * herda dependência da própria DDL da série: `GET /execucoes` responde **200 com
 * `disponivel: false`** enquanto `sql/2026-07-30_precificacao_triagem_execucao.sql` não for
 * aplicada, e a triagem em si nunca depende dela.
 */
@ApiTags('Compras - Precificação')
@Controller('precificacao/triagem')
export class TriagemController {
  constructor(private readonly service: TriagemService) {}

  @Get('execucoes')
  @ApiOperation({
    summary: 'Série histórica das taxas de exceção, da mais recente para a mais antiga.',
    description:
      'É o que torna M5 acompanhável ao longo do tempo. Responde 200 mesmo sem a DDL ' +
      'aplicada — nesse caso `disponivel: false` e a lista vazia, com o motivo declarado.',
  })
  @ApiQuery({ name: 'limite', required: false, description: 'Máx. de execuções (padrão 200).' })
  @ApiResponse({ status: 200, type: SerieTriagemDto })
  async execucoes(@Query('limite') limite?: string): Promise<SerieTriagemDto> {
    return this.service.serie(limite == null ? undefined : Number(limite));
  }

  @Get('nfe/:chaveNfe')
  @ApiOperation({
    summary: 'Tria todos os itens das notas cobertas por uma chave de NF-e.',
    description:
      'Cada item sai `automatico` ou `excecao`; toda exceção traz motivo do conjunto fechado ' +
      'de sete. Só `custo nao confiavel` suprime a proposta — nos outros seis motivos o item ' +
      'vem COM preço nas três tabelas, para o comprador confirmar ou corrigir.',
  })
  @ApiParam({ name: 'chaveNfe', description: 'Chave de acesso da NF-e (44 dígitos).' })
  @ApiResponse({ status: 200, type: [TriagemNotaDto] })
  @ApiResponse({ status: 404, description: 'Chave inválida ou sem vínculo confirmado.' })
  @ApiResponse({
    status: 503,
    description: 'Régua de margem indisponível (DDL das faixas não aplicada) — herdado.',
  })
  async porNota(@Param('chaveNfe') chaveNfe: string): Promise<TriagemNotaDto[]> {
    return this.service.porNota(chaveNfe);
  }

  @Get('pedido/:pedidoId')
  @ApiOperation({
    summary: 'Tria todos os itens de um pedido.',
    description:
      'Mesmo conjunto de motivos. É o caminho em que aparece o item de pedido sem vínculo de ' +
      'XML, que sai por `custo nao confiavel` — o único motivo sem proposta.',
  })
  @ApiParam({ name: 'pedidoId', description: 'id (cuid) de com_pedido.' })
  @ApiResponse({ status: 200, type: TriagemNotaDto })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  @ApiResponse({
    status: 503,
    description: 'Régua de margem indisponível (DDL das faixas não aplicada) — herdado.',
  })
  async porPedido(@Param('pedidoId') pedidoId: string): Promise<TriagemNotaDto> {
    return this.service.porPedido(pedidoId);
  }
}
