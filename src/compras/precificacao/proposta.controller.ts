import { Controller, Get, Param } from '@nestjs/common';
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { PropostaNotaDto } from './dto/proposta-preco.dto';
import { PropostaService } from './proposta.service';

/**
 * Proposta de preço das compras (US-041 / T-026).
 *
 * **Sem guard, por convenção do container** (CODING_STANDARDS#convencoes-backend-compras-service):
 * o `compras-service` não tem `AuthGuard` nem `pode()` — os controllers são públicos e
 * documentados por Swagger. Introduzir guard aqui seria mudança de arquitetura, não convenção.
 *
 * A nota inteira sai em **uma** requisição, no mesmo padrão da T-020 (`custo.controller.ts`):
 * `GET .../nfe/:chaveNfe` devolve uma composição por pedido coberto pela chave.
 *
 * **Depende da aplicação manual da DDL das faixas** (`sql/2026-07-30_precificacao_faixas.sql`,
 * entregue pela T-024 e ainda não aplicada): sem a tabela `com_precificacao_faixa` a régua
 * responde **503**, e a proposta — que consome o corredor e a trava de piso dela — propaga o
 * 503. Não há fallback em constante de código de propósito: régua em código contrariaria o AC
 * da T-024 ("mudar a faixa sem rebuild nem restart").
 */
@ApiTags('Compras - Precificação')
@Controller('precificacao/proposta')
export class PropostaController {
  constructor(private readonly service: PropostaService) {}

  @Get('nfe/:chaveNfe')
  @ApiOperation({
    summary: 'Propõe preço para as três tabelas em todos os itens de uma NF-e conferida.',
    description:
      'Aplica a árvore de decisão custo ↑↓ × estoque × performance ancorada no markup ' +
      'anterior. Item sem preço anterior recebe proposta derivada da faixa, posicionada pelo ' +
      'sinal de performance e marcada "precisa de confirmação de mercado". Item inelegível ' +
      '(custo não confiável) é o único caso sem preço. Nenhuma proposta fura o piso — a ' +
      'validação é da trava da régua (US-040).',
  })
  @ApiParam({ name: 'chaveNfe', description: 'Chave de acesso da NF-e (44 dígitos).' })
  @ApiResponse({ status: 200, type: [PropostaNotaDto] })
  @ApiResponse({ status: 404, description: 'Chave inválida ou sem vínculo confirmado.' })
  @ApiResponse({
    status: 503,
    description: 'Régua de margem indisponível (DDL das faixas não aplicada).',
  })
  async porNota(@Param('chaveNfe') chaveNfe: string): Promise<PropostaNotaDto[]> {
    return this.service.porNota(chaveNfe);
  }

  @Get('pedido/:pedidoId')
  @ApiOperation({
    summary: 'Propõe preço para todos os itens de um pedido.',
    description:
      'Mesma árvore de decisão do endpoint por chave de NF-e. É o caminho em que aparece o ' +
      'item de pedido sem vínculo de XML (`chaves_nfe: []`), inelegível pela trava da nota.',
  })
  @ApiParam({ name: 'pedidoId', description: 'id (cuid) de com_pedido.' })
  @ApiResponse({ status: 200, type: PropostaNotaDto })
  @ApiResponse({ status: 404, description: 'Pedido não encontrado.' })
  @ApiResponse({
    status: 503,
    description: 'Régua de margem indisponível (DDL das faixas não aplicada).',
  })
  async porPedido(@Param('pedidoId') pedidoId: string): Promise<PropostaNotaDto> {
    return this.service.porPedido(pedidoId);
  }
}
