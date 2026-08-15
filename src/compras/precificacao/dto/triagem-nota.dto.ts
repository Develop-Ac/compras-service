import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { MOTIVO_CUSTO_NAO_CONFIAVEL } from './elegibilidade.dto';
import type { TabelaPreco } from './preco-vigente.dto';
import { MARCACAO_CONFIRMACAO_MERCADO } from './proposta-preco.dto';

/**
 * Triagem da nota entre automáticos e exceções (US-043 / T-030).
 *
 * **Sem class-validator**, pelo mesmo motivo declarado em `proposta-preco.dto.ts`: o
 * `compras-service` não tem `ValidationPipe` global (medido). A validação de entrada vive nas
 * fatias consumidas (custo e proposta), que já recusam chave e pedido malformados.
 *
 * ---
 * ## O que a triagem separa (e o que ela NÃO separa)
 *
 * Ela separa **o que o motor decidiu sozinho** do **que precisa de aval** — não *o que tem
 * preço* de *o que não tem*. Dos sete motivos, **um só** suprime a proposta
 * (`custo nao confiavel`); nos outros seis o item chega ao comprador **com preço nas três
 * tabelas**, e ele confirma ou corrige (US-043#excecao-com-proposta).
 */

/** Cada item da nota sai de um lado ou do outro — não há terceira classificação. */
export type ClassificacaoTriagem = 'automatico' | 'excecao';

/**
 * # O conjunto fechado de SETE motivos
 *
 * Fechado **no sistema de tipos**, não por convenção documentada: `MotivoExcecao` é a união
 * dos valores deste mapa `as const`, então motivo fora do conjunto é **erro de compilação**.
 * Não há motivo genérico do tipo "revisar" — a US-043 o proíbe explicitamente.
 *
 * Dois dos sete são **reaproveitados verbatim** das fatias que já os declaram, nunca
 * redigitados: `custo nao confiavel` vem de `MOTIVO_CUSTO_NAO_CONFIAVEL` (T-025) e
 * `precisa de confirmação de mercado` vem de `MARCACAO_CONFIRMACAO_MERCADO` (T-026). Copiar
 * as strings criaria uma segunda verdade sobre o vocabulário do extrato de decisão.
 *
 * | chave | motivo | proposta? |
 * |---|---|---|
 * | `custo_nao_confiavel` | `custo nao confiavel` | **não** |
 * | `preco_anterior_ilegivel` | `preço anterior ilegível` | sim |
 * | `preco_anterior_inconsistente` | `preço anterior inconsistente` | sim |
 * | `markup_anterior_fora_do_corredor` | `markup anterior fora do corredor` | sim |
 * | `custo_divergente_do_pedido` | `custo divergente do pedido` | sim |
 * | `sinal_giro_ausente_ou_velho` | `sinal de giro ausente ou velho` | sim |
 * | `confirmacao_de_mercado` | `precisa de confirmação de mercado` | sim |
 */
export const MOTIVO_EXCECAO = {
  custo_nao_confiavel: MOTIVO_CUSTO_NAO_CONFIAVEL,
  preco_anterior_ilegivel: 'preço anterior ilegível',
  preco_anterior_inconsistente: 'preço anterior inconsistente',
  markup_anterior_fora_do_corredor: 'markup anterior fora do corredor',
  custo_divergente_do_pedido: 'custo divergente do pedido',
  sinal_giro_ausente_ou_velho: 'sinal de giro ausente ou velho',
  confirmacao_de_mercado: MARCACAO_CONFIRMACAO_MERCADO,
} as const;

/** Chave estável do motivo — é ela que o extrato de decisão (EP-014) agrupa. */
export type ChaveMotivoExcecao = keyof typeof MOTIVO_EXCECAO;

/** O motivo em prosa. União fechada de exatamente sete literais. */
export type MotivoExcecao = (typeof MOTIVO_EXCECAO)[ChaveMotivoExcecao];

/**
 * Precedência do motivo **principal**, do mais material para o menos.
 *
 * O item pode disparar vários motivos — todos são devolvidos —, mas o extrato precisa de UMA
 * linha, e ela é a do motivo de maior precedência. A ordem não é arbitrária: começa no único
 * que suprime a proposta, desce pelos que invalidam a **âncora** (preço anterior ilegível,
 * inconsistente, markup fora do corredor), passa pelo que questiona o **custo** e termina nos
 * dois que apenas pedem julgamento sobre uma proposta válida.
 */
export const PRECEDENCIA_MOTIVOS: readonly ChaveMotivoExcecao[] = [
  'custo_nao_confiavel',
  'preco_anterior_ilegivel',
  'preco_anterior_inconsistente',
  'markup_anterior_fora_do_corredor',
  'custo_divergente_do_pedido',
  'sinal_giro_ausente_ou_velho',
  'confirmacao_de_mercado',
] as const;

/**
 * O **único** motivo que suprime a proposta (US-043, declaração do responsável de 2026-07-29).
 *
 * Declarado como conjunto — e não como `if` espalhado no serviço — porque a invariante
 * "exceção com proposta" é medida contra ele: item cujo motivo principal não está aqui e que
 * chega sem preço nas três tabelas é **defeito**, e o contador `excecoes_sem_proposta` existe
 * para provar que isso não acontece.
 */
export const MOTIVOS_QUE_SUPRIMEM_PROPOSTA: readonly ChaveMotivoExcecao[] = [
  'custo_nao_confiavel',
] as const;

/**
 * Alvo de M5: **≤ 33%** dos itens em exceção (US-043, PRD).
 *
 * Declarado no retorno para o consumidor não replicar o número — mesma disciplina de
 * `JANELA_FRESCURA_HORAS` (US-039) e `CORTE_IDADE_SALDO_DIAS` (T-025).
 */
export const META_TAXA_EXCECAO_PCT = 33;

/** Uma exceção disparada, com o que concretamente a disparou e onde. */
export class ExcecaoTriadaDto {
  @ApiProperty({
    description: 'Chave estável do motivo — o que o extrato de decisão agrupa.',
    enum: Object.keys(MOTIVO_EXCECAO),
    example: 'sinal_giro_ausente_ou_velho',
  })
  chave!: ChaveMotivoExcecao;

  @ApiProperty({
    description: 'Motivo em prosa, do conjunto fechado de sete.',
    enum: Object.values(MOTIVO_EXCECAO),
    example: MOTIVO_EXCECAO.sinal_giro_ausente_ou_velho,
  })
  motivo!: MotivoExcecao;

  @ApiProperty({
    description: 'Qual valor concreto disparou a exceção — nunca prosa genérica.',
    example: 'giro sem materialização em com_fifo_completo (frescor "sem-materializacao")',
  })
  detalhe!: string;

  @ApiProperty({
    description:
      'Tabelas de preço afetadas. Motivo que é do produto (custo, giro) afeta as três; ' +
      'motivo que é da tabela (preço anterior, markup) afeta só as que dispararam.',
    example: ['varejo', 'atacado_especial', 'atacado'],
  })
  tabelas!: TabelaPreco[];
}

/** UM item da nota, já triado. */
export class ItemTriadoDto {
  @ApiProperty({ example: 33090 })
  pro_codigo!: number;

  @ApiProperty({ example: 'PASTILHA DE FREIO DIANTEIRA' })
  pro_descricao!: string;

  @ApiProperty({
    description: 'De que lado da triagem o item caiu.',
    enum: ['automatico', 'excecao'],
    example: 'automatico',
  })
  classificacao!: ClassificacaoTriagem;

  @ApiPropertyOptional({
    description:
      'Motivo de maior precedência (`PRECEDENCIA_MOTIVOS`) — a UMA linha do extrato. null ' +
      'quando o item é automático. **Nunca vazio quando a classificação é `excecao`.**',
    example: null,
    nullable: true,
  })
  motivo_principal!: MotivoExcecao | null;

  @ApiProperty({
    description: 'Todos os motivos disparados, na ordem de precedência. Vazio no automático.',
    type: [String],
    example: [],
  })
  motivos!: MotivoExcecao[];

  @ApiProperty({
    description: 'Cada motivo com o valor concreto que o disparou e as tabelas afetadas.',
    type: [ExcecaoTriadaDto],
  })
  excecoes!: ExcecaoTriadaDto[];

  @ApiProperty({
    description:
      'true quando as TRÊS tabelas trazem preço proposto. Só `custo nao confiavel` produz ' +
      'false — exceção sem preço com qualquer outro motivo é defeito (US-043).',
    example: true,
  })
  com_proposta!: boolean;
}

/** Quantos itens cada motivo produziu — a decomposição da taxa. */
export class ContagemMotivoDto {
  @ApiProperty({ enum: Object.keys(MOTIVO_EXCECAO), example: 'confirmacao_de_mercado' })
  chave!: ChaveMotivoExcecao;

  @ApiProperty({ example: MOTIVO_EXCECAO.confirmacao_de_mercado })
  motivo!: MotivoExcecao;

  @ApiProperty({ description: 'Itens em que ESTE motivo disparou.', example: 9 })
  itens!: number;

  @ApiProperty({
    description: 'Itens em que este motivo foi o principal (o do extrato).',
    example: 7,
  })
  itens_como_principal!: number;
}

/** A taxa de exceção da execução — o insumo de M5. */
export class TaxaExcecaoDto {
  @ApiProperty({ example: 789 })
  itens!: number;

  @ApiProperty({ example: 640 })
  automaticos!: number;

  @ApiProperty({ example: 149 })
  excecoes!: number;

  @ApiProperty({
    description: 'excecoes ÷ itens × 100, em %. 0 quando a nota não tem item.',
    example: 18.8847,
  })
  taxa_excecao_pct!: number;

  @ApiProperty({ description: 'Alvo de M5, declarado no retorno.', example: META_TAXA_EXCECAO_PCT })
  meta_pct!: number;

  @ApiProperty({ description: 'true quando `taxa_excecao_pct <= meta_pct`.', example: true })
  dentro_da_meta!: boolean;

  @ApiProperty({
    description:
      'Exceções cujo motivo principal NÃO suprime a proposta e que mesmo assim chegaram sem ' +
      'preço nas três tabelas. **Invariante da US-043: sempre 0.**',
    example: 0,
  })
  excecoes_sem_proposta!: number;

  @ApiProperty({
    description:
      'Decomposição por motivo. A soma de `itens` pode exceder `excecoes` — um item dispara ' +
      'mais de um motivo, e todos são devolvidos; `itens_como_principal` é que soma exato.',
    type: [ContagemMotivoDto],
  })
  por_motivo!: ContagemMotivoDto[];
}

/**
 * Resultado do **registro** da taxa na série histórica.
 *
 * A triagem nunca depende deste registro para responder (ver `TriagemService`): a
 * classificação e a taxa saem na resposta com ou sem a tabela aplicada. Este objeto declara,
 * sem esconder, se a linha da série foi de fato gravada.
 */
export class RegistroTriagemDto {
  @ApiProperty({
    description: 'true quando a execução entrou na série histórica.',
    example: false,
  })
  persistido!: boolean;

  @ApiPropertyOptional({
    description: 'id da linha gravada em `com_precificacao_triagem_execucao`; null sem registro.',
    example: 1,
    nullable: true,
  })
  execucao_id!: number | null;

  @ApiPropertyOptional({
    description:
      'Por que a série não recebeu a execução — tipicamente a DDL manual ainda não aplicada. ' +
      'null quando gravou.',
    example: 'DDL de sql/2026-07-30_precificacao_triagem_execucao.sql ainda não aplicada.',
    nullable: true,
  })
  indisponivel_motivo!: string | null;
}

/** Resposta do endpoint: a nota inteira triada em UMA requisição (padrão da T-020). */
export class TriagemNotaDto {
  @ApiProperty({ example: 'clx0k9v2h0000abcd1234efgh' })
  pedido_id!: string;

  @ApiPropertyOptional({ nullable: true })
  chave_nfe!: string | null;

  @ApiPropertyOptional({ example: 1234, nullable: true })
  for_codigo!: number | null;

  @ApiProperty({ type: TaxaExcecaoDto })
  taxa!: TaxaExcecaoDto;

  @ApiProperty({ type: [ItemTriadoDto] })
  itens!: ItemTriadoDto[];

  @ApiProperty({ type: RegistroTriagemDto })
  registro!: RegistroTriagemDto;

  @ApiProperty({ example: '2026-07-30T12:00:00.000Z' })
  triado_em!: string;
}

/** Uma execução passada, lida da série histórica. */
export class ExecucaoTriagemDto {
  @ApiProperty({ example: 1 })
  id!: number;

  @ApiProperty({ example: 'clx0k9v2h0000abcd1234efgh' })
  pedido_id!: string;

  @ApiPropertyOptional({ nullable: true })
  chave_nfe!: string | null;

  @ApiPropertyOptional({ example: 1234, nullable: true })
  for_codigo!: number | null;

  @ApiProperty({ example: 789 })
  itens!: number;

  @ApiProperty({ example: 149 })
  excecoes!: number;

  @ApiProperty({ example: 18.8847 })
  taxa_excecao_pct!: number;

  @ApiProperty({ example: META_TAXA_EXCECAO_PCT })
  meta_pct!: number;

  @ApiProperty({ example: true })
  dentro_da_meta!: boolean;

  @ApiProperty({ example: '2026-07-30T12:00:00.000Z' })
  triado_em!: string;
}

/** A série histórica das taxas — o que torna M5 acompanhável ao longo do tempo. */
export class SerieTriagemDto {
  @ApiProperty({
    description:
      'false quando a DDL manual ainda não foi aplicada. A rota responde 200 mesmo assim, ' +
      'com a lista vazia e o motivo — a triagem não fica meio-desligada por causa da série.',
    example: false,
  })
  disponivel!: boolean;

  @ApiPropertyOptional({
    description: 'Por que a série está indisponível; null quando disponível.',
    nullable: true,
  })
  indisponivel_motivo!: string | null;

  @ApiProperty({ type: [ExecucaoTriagemDto] })
  execucoes!: ExecucaoTriagemDto[];

  @ApiProperty({ description: 'Instante da leitura.', example: '2026-07-30T12:00:00.000Z' })
  lido_em!: string;
}
