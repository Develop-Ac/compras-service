import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Escopo sobre o qual o número de giro foi de fato calculado.
 * - `grupo`   — agregado do grupo de itens iguais (o caso desejado);
 * - `produto` — o produto isolado (produto sem grupo, ou grupo ainda não materializado).
 */
export type EscopoGiro = 'grupo' | 'produto';

/** Qual métrica de giro a linha carrega. */
export type MetricaGiro = 'giro_real' | 'giro_esperado';

/**
 * Frescura da materialização que originou os números:
 * - `fresco`             — idade < `JANELA_FRESCURA_HORAS`;
 * - `obsoleto`           — idade ≥ `JANELA_FRESCURA_HORAS` (o motor trata como exceção);
 * - `sem-materializacao` — o produto não tem linha em `com_fifo_completo`.
 */
export type FrescorGiro = 'fresco' | 'obsoleto' | 'sem-materializacao';

/**
 * **Janela de frescura aceitável da materialização de `com_fifo_completo`: 72 horas.**
 *
 * O ETL do `analise-estoque-service` materializa a tabela por processamento
 * (`data_processamento`). A US-039 escreveu "mais de 1 dia é dado velho" e a T-022 traduziu
 * isso em 24h — número que a **cadência real medida** desmentiu: os runs de 2026-07-26 14:06
 * e 2026-07-29 15:25 estão a **~3 dias** um do outro
 * (DATA_MODEL#idade-do-saldo-em-estoque-sinal-de-performance-validado-em-2026-07-29). Com 24h,
 * **todo** produto ficaria permanentemente `obsoleto` entre dois ciclos do ETL — um alerta que
 * dispara sempre não é alerta, e o aprovador aprenderia a ignorá-lo. Corrigido para **72h**
 * (retro-04): a janela passa a cobrir um ciclo do ETL, e `obsoleto` volta a significar "o ETL
 * atrasou", que é a informação útil.
 *
 * Limite **inclusivo**: `idade ≥ 72h` já é obsoleto. Atingir a janela cheia significa que o
 * ciclo esperado do ETL fechou sem dado novo — exatamente o que se quer sinalizar.
 *
 * Acima da janela o sinal continua sendo devolvido (nunca se inventa número nem se esconde o
 * dado), marcado `frescor: 'obsoleto'` e `dado_desatualizado: true`, com o valor da janela
 * **declarado no retorno** (`janela_frescura_horas`) — nunca implícito no código do consumidor.
 */
export const JANELA_FRESCURA_HORAS = 72;

/**
 * Mapa único das duas métricas de giro e das colunas que as sustentam em cada escopo —
 * mesma convenção de rastreabilidade de `preco-vigente.dto.ts` (`campo_erp`/`campo_espelho`).
 *
 * | métrica       | escopo grupo                | escopo produto             |
 * |---------------|-----------------------------|----------------------------|
 * | giro real     | `grp_demanda_media_dia`     | `demanda_media_dia`        |
 * | giro esperado | `grupo_demanda_dia`         | `demanda_planejamento_dia` |
 *
 * O par real/esperado é o mesmo que o ETL usa para dimensionar estoque: a demanda média
 * **observada** (real) contra a demanda de **planejamento** (esperada). Nenhuma agregação
 * nova é feita aqui — as colunas `grp_*`/`grupo_*` já vêm agregadas pelo ETL, dono da
 * escrita; recalcular no `compras-service` criaria uma segunda fórmula de giro.
 */
export const METRICAS_GIRO: ReadonlyArray<{
  metrica: MetricaGiro;
  rotulo: string;
  campo_grupo: string;
  campo_produto: string;
}> = [
  {
    metrica: 'giro_real',
    rotulo: 'Giro real',
    campo_grupo: 'com_fifo_completo.grp_demanda_media_dia',
    campo_produto: 'com_fifo_completo.demanda_media_dia',
  },
  {
    metrica: 'giro_esperado',
    rotulo: 'Giro esperado',
    campo_grupo: 'com_fifo_completo.grupo_demanda_dia',
    campo_produto: 'com_fifo_completo.demanda_planejamento_dia',
  },
];

/** Uma métrica de giro, com o escopo real de cálculo e a coluna de origem. */
export class MetricaGiroDto {
  @ApiProperty({
    description: 'Identificador da métrica.',
    enum: ['giro_real', 'giro_esperado'],
    example: 'giro_real',
  })
  metrica!: MetricaGiro;

  @ApiProperty({ description: 'Rótulo de negócio para exibição.', example: 'Giro real' })
  rotulo!: string;

  @ApiPropertyOptional({
    description: 'Valor em unidades por dia; null quando a coluna de origem está vazia.',
    example: 3.42,
    nullable: true,
  })
  valor!: number | null;

  @ApiProperty({ description: 'Unidade do valor.', example: 'un/dia' })
  unidade!: string;

  @ApiProperty({
    description:
      'Escopo sobre o qual ESTE número foi calculado — `grupo` quando o agregado do grupo ' +
      'está materializado, `produto` no caso contrário.',
    enum: ['grupo', 'produto'],
    example: 'grupo',
  })
  escopo!: EscopoGiro;

  @ApiProperty({
    description: 'Coluna de `com_fifo_completo` que sustenta o valor (rastreabilidade).',
    example: 'com_fifo_completo.grp_demanda_media_dia',
  })
  campo_origem!: string;
}

/**
 * ## `TendenciaGiroDto` foi REMOVIDO do contrato (T-025, retro-04)
 *
 * O objeto `tendencia` expunha quatro campos; **três deles vieram `null` nos 36 produtos da
 * demo em produção da sprint-04**: `fator_tendencia`, `rotulo` (`tendencia_label`) e
 * `vendas_12m_ant`. As colunas existem em `com_fifo_completo`, mas o ETL do
 * `analise-estoque-service` **não as calcula** — logo o contrato prometia um sinal de tendência
 * que nunca chega. Campo exposto e sempre vazio é pior que campo ausente: o consumidor escreve
 * tratamento de `null` para um caso que é 100% dos casos, e o aprovador lê "sem tendência" como
 * informação quando é só lacuna de ETL.
 *
 * **Decisão: remover os três do contrato.** O único campo do grupo que o ETL de fato preenche,
 * `vendas_ult_12m`, sobe para o próprio `SinalGiroDto` — um total absoluto de vendas não é
 * "tendência", e mantê-lo dentro de um envelope chamado `tendencia` sozinho seria rotular
 * errado o que restou.
 *
 * **Alternativa descartada:** manter os três campos e pedir o cálculo ao
 * `analise-estoque-service` (dono da escrita da tabela). Descartada por três razões: (a) move
 * uma fronteira de serviço para fora do escopo desta sprint, exigindo trabalho no backlog de
 * outro dono, com prazo que a US-041 não controla; (b) mesmo entregue, tendência é sinal
 * **redundante** aqui — a US-041 decide performance pelo par giro real × giro esperado e, na
 * falta de giro, pela idade do saldo, nenhum dos dois derivado de tendência; (c) até a entrega,
 * o contrato continuaria mentindo. Se o ETL passar a calcular as colunas e algum caso de uso
 * pedir tendência, reintroduzir é aditivo e barato — o inverso (remover campo que consumidores
 * já leem) não é.
 */

/** Sinal de performance de UM produto, com a idade da materialização que o originou. */
export class SinalGiroDto {
  @ApiProperty({ example: 38883 })
  pro_codigo!: number;

  @ApiPropertyOptional({ example: 'PASTILHA DE FREIO DIANTEIRA', nullable: true })
  pro_descricao!: string | null;

  @ApiProperty({
    description:
      'Escopo efetivo dos números de giro. `produto` quando o produto não tem grupo OU ' +
      'quando o grupo existe mas ainda não foi materializado pelo ETL.',
    enum: ['grupo', 'produto'],
    example: 'grupo',
  })
  escopo!: EscopoGiro;

  @ApiProperty({
    description:
      'true quando o produto NÃO pertence a nenhum grupo de itens iguais — o motor de preço ' +
      'deve tratar este caso como exceção.',
    example: false,
  })
  sem_grupo!: boolean;

  @ApiPropertyOptional({
    description: 'Grupo de itens iguais (`com_relacionamento_itens.group_id`); null sem grupo.',
    example: 'GRP-000123',
    nullable: true,
  })
  group_id!: string | null;

  @ApiPropertyOptional({
    description: 'Chave de agrupamento materializada (`com_fifo_completo.grupo_chave`).',
    example: 'PASTILHA FREIO DIANT / GOL G5',
    nullable: true,
  })
  grupo_chave!: string | null;

  @ApiPropertyOptional({
    description: 'Quantidade de itens do grupo na materialização.',
    example: 4,
    nullable: true,
  })
  grupo_qtd_itens!: number | null;

  @ApiProperty({
    description:
      'true quando o grupo do produto já está refletido na materialização. false significa ' +
      'que o agrupamento é mais novo que o último processamento do ETL — os números caem ' +
      'para o escopo `produto` até o próximo ciclo.',
    example: true,
  })
  agrupamento_materializado!: boolean;

  @ApiProperty({
    description: 'Sempre 2 entradas: giro real e giro esperado — nesta ordem.',
    type: [MetricaGiroDto],
  })
  giro!: MetricaGiroDto[];

  @ApiPropertyOptional({
    description:
      'Vendas dos últimos 12 meses no escopo efetivo do sinal (`grp_vendas_ult_12m` no escopo ' +
      'grupo, `vendas_ult_12m` no escopo produto). Único sobrevivente do antigo objeto ' +
      '`tendencia` — os outros três campos foram removidos por vir sempre nulos (ver acima).',
    example: 1240.5,
    nullable: true,
  })
  vendas_ult_12m!: number | null;

  @ApiPropertyOptional({
    description: 'Tempo médio em estoque do produto, em dias (`tempo_medio_estoque`).',
    example: 47.5,
    nullable: true,
  })
  tempo_medio_estoque_dias!: number | null;

  @ApiPropertyOptional({
    description:
      'Data/hora da materialização que originou os números ' +
      '(`com_fifo_completo.data_processamento`).',
    example: '2026-07-29T06:00:00.000Z',
    nullable: true,
  })
  materializado_em!: string | null;

  @ApiPropertyOptional({
    description: 'Idade da materialização em horas no instante da leitura.',
    example: 6.0,
    nullable: true,
  })
  idade_horas!: number | null;

  @ApiPropertyOptional({
    description: 'Idade da materialização em dias no instante da leitura.',
    example: 0.25,
    nullable: true,
  })
  idade_dias!: number | null;

  @ApiProperty({
    description: 'Classificação da idade contra a janela de frescura aceitável.',
    enum: ['fresco', 'obsoleto', 'sem-materializacao'],
    example: 'fresco',
  })
  frescor!: FrescorGiro;

  @ApiProperty({
    description:
      'Janela de frescura aceitável da materialização, em horas — declarada no retorno para o ' +
      'consumidor não replicar o número.',
    example: JANELA_FRESCURA_HORAS,
  })
  janela_frescura_horas!: number;

  @ApiProperty({
    description:
      'Atalho booleano de `frescor !== "fresco"` — dado velho ou ausente, a sinalizar ao ' +
      'aprovador no extrato de decisão.',
    example: false,
  })
  dado_desatualizado!: boolean;

  @ApiProperty({
    description: 'Instante em que esta resposta foi montada.',
    example: '2026-07-29T12:00:00.000Z',
  })
  lido_em!: string;
}
