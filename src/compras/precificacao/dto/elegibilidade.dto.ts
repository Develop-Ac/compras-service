import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Qual trava de custo derrubou a elegibilidade do item.
 *
 * - `custo_estoque_nao_confiavel` — custo do **estoque existente**: nenhuma âncora aceita
 *   (`com_fifo_completo.custo_fonte` ∉ {`camada_viva`} ∪ {`ultima_entrada` com
 *   `estoque_disponivel = 0`}, ou `custo_unitario` NULL);
 * - `custo_nota_sem_xml` — custo da **nota entrando** (US-037) sem vínculo de XML
 *   (`CustoItemDto.chaves_nfe` vazio), logo composto por parcelas parciais.
 *
 * As duas são distintas de propósito — uma olha o que já está no estoque, a outra o que está
 * chegando — mas ambas produzem o **mesmo motivo de negócio** e mandam o item para exceção
 * humana (US-041, decisão do responsável de 2026-07-29).
 */
export type TravaElegibilidade = 'custo_estoque_nao_confiavel' | 'custo_nota_sem_xml';

/**
 * Motivo de negócio, em uma linha, consumível pelo extrato de decisão (EP-014).
 * Texto sem acento de propósito: é chave de negócio declarada na US-041, não prosa de UI.
 */
export const MOTIVO_CUSTO_NAO_CONFIAVEL = 'custo nao confiavel';

/**
 * Degrau de `com_fifo_completo.custo_fonte` que sustenta proposta automática **em qualquer
 * estoque**.
 *
 * A cadeia é `camada_viva → ultima_entrada → cadastro → NULL`
 * (DATA_MODEL#semantica-do-custo-em-com_fifo_completo): `camada_viva` (14.554 produtos, ≈ os
 * que têm estoque) é a média ponderada das camadas FIFO residuais — **camada viva implica
 * estoque**; `cadastro` (500) e ausente (6.975) são suspeitos por construção e seguem
 * barrados sempre.
 */
export const CUSTO_FONTE_CONFIAVEL = 'camada_viva';

/**
 * Degrau que sustenta proposta automática **somente quando `estoque_disponivel = 0`**
 * (US-047 / T-028, decisão do responsável de 2026-07-30 no gate de review da sprint-05).
 *
 * `ultima_entrada` (23.133 produtos) é o **único custo que existe** para item sem saldo, e é
 * um custo real de compra, não estimativa. A trava da sprint-05 exigia `camada_viva`, que
 * implica estoque — logo item sem estoque caía em `ultima_entrada` e virava inelegível,
 * barrando exatamente os cenários 2 e 3 da US-041, que **pressupõem estoque zerado**. Os dois
 * ramos existiam no código e passavam nos testes, mas nenhum item real os alcançava.
 *
 * **A fronteira move um degrau, e só quando o estoque é zero.** Com estoque presente a trava
 * continua valendo onde foi desenhada para valer: o preço tem de sair da camada FIFO viva.
 */
export const CUSTO_FONTE_ANCORA_SEM_ESTOQUE = 'ultima_entrada';

/**
 * De onde veio o custo que ancora a proposta — declarado no retorno para o aprovador saber se
 * está olhando o estoque presente ou uma compra passada (US-047).
 *
 * - `camada_viva`                — média ponderada das camadas FIFO residuais (estoque presente);
 * - `ultima_entrada_sem_estoque` — custo da última compra conhecida, item **sem** estoque;
 * - `null`                       — nenhuma âncora aceita; a trava de custo disparou.
 */
export type OrigemAncoraCusto = 'camada_viva' | 'ultima_entrada_sem_estoque' | null;

/**
 * Texto que a proposta carrega para o aprovador saber **de onde veio o custo** — a US-047 pede
 * que a origem da âncora seja declarada, não inferida do enum.
 */
export const DECLARACAO_ANCORA: Record<Exclude<OrigemAncoraCusto, null>, string> = {
  camada_viva:
    'Âncora: custo médio das camadas FIFO vivas — estoque presente.',
  ultima_entrada_sem_estoque:
    'Âncora: custo da ÚLTIMA ENTRADA — item sem estoque. O custo vem de uma compra passada, ' +
    'não do estoque presente.',
};

/**
 * Corte de negócio da idade do saldo, em **dias**: 365.
 *
 * Vem da regra do responsável (2026-07-29, US-041#regra-da-idade-do-saldo): sobre os 8.930
 * itens com estoque e sem giro, 7.463 têm idade ≥ 365 dias (média 1.893 dias, R$ 1,1 mi
 * parados) e são julgáveis como performance **ruim**; os 1.467 restantes (média 176 dias) não
 * tiveram tempo de mostrar performance e vão para exceção humana.
 *
 * **O corte é sobre a coluna numérica `tempo_medio_saldo_atual`, nunca sobre
 * `categoria_saldo_atual`** — a bucketização pronta (`Rápido` ≤60 · `Médio` 60–120 ·
 * `Lento` 120–240 · `Obsoleto` >240) para em 240 e não tem degrau em 365, então usá-la
 * classificaria como "ruim" item de 250 dias que a regra manda tratar como sinal ausente.
 * Por isso este DTO **não recebe** `categoria_saldo_atual` na entrada: a categoria não é
 * insumo desta decisão.
 */
export const CORTE_IDADE_SALDO_DIAS = 365;

/**
 * Classificação de performance derivada da **idade do saldo** — só se aplica ao item que tem
 * estoque e **não** tem giro.
 *
 * - `ruim`          — idade ≥ `CORTE_IDADE_SALDO_DIAS`; o motor pode propor redução,
 *                     respeitando o piso da faixa;
 * - `ausente`       — idade < `CORTE_IDADE_SALDO_DIAS`, idade não materializada, ou idade
 *                     **anômala** (negativa): sinal ausente ⇒ exceção humana;
 * - `nao-aplicavel` — o item tem giro (a performance vem do par giro real × esperado da
 *                     US-039) ou não tem estoque; a idade do saldo não decide nada aqui.
 */
export type ClassificacaoSaldo = 'ruim' | 'ausente' | 'nao-aplicavel';

/**
 * Insumo da avaliação de elegibilidade de UM item.
 *
 * **Não é body de requisição** — é contrato de entrada de serviço (não há controller nesta
 * fatia; a T-026 é quem orquestra custo + giro + régua e chama esta avaliação). Por isso
 * `interface` sem `class-validator`: a validação de borda HTTP acontece nos DTOs dos
 * endpoints que já existem, e o que aqui pode vir sujo (`pro_codigo`) é normalizado pelo
 * próprio serviço.
 *
 * Todos os campos numéricos aceitam `null` porque **todas** as colunas de origem em
 * `com_fifo_completo` são nullable — e `custo_unitario` NULL significa **ausência de custo no
 * ERP**, nunca "de graça" (6.975 produtos passaram de `0,00` para NULL na correção de
 * 2026-07-29).
 */
export interface AvaliacaoElegibilidadeInput {
  /** `com_fifo_completo.pro_codigo` (a coluna é `VarChar`; aceita number ou string). */
  pro_codigo: number | string;

  /** `com_fifo_completo.custo_unitario` — custo FIFO do estoque vivo. NULL = sem custo. */
  custo_unitario: number | null;

  /** `com_fifo_completo.custo_fonte` — degrau da cadeia de custo. */
  custo_fonte: string | null;

  /**
   * `com_fifo_completo.estoque_disponivel` — define "item com estoque" (`> 0`) e, desde a
   * US-047, **habilita a âncora `ultima_entrada` exatamente em `0`**.
   *
   * NULL **não** é zero: estoque não materializado não destrava a âncora da última entrada
   * (mesma disciplina de `custo_unitario` NULL). Estoque **negativo** — anomalia de ERP —
   * também não destrava: não se abre exceção de preço com base num número impossível.
   */
  estoque_disponivel: number | null;

  /**
   * Giro real no escopo efetivo do sinal (US-039 / `SinalGiroDto.giro[giro_real].valor`).
   * `null` ou `0` = **sem giro** — é o caso que a idade do saldo desempata.
   */
  giro_real: number | null;

  /** `com_fifo_completo.tempo_medio_saldo_atual` — idade do saldo vivo, em dias. */
  tempo_medio_saldo_atual: number | null;

  /**
   * Chaves de NF-e do custo composto da nota entrando (`CustoItemDto.chaves_nfe`).
   * `[]` = item de pedido **sem** vínculo de XML ⇒ custo parcial ⇒ trava.
   * `undefined`/`null` = a avaliação não recebeu nota nenhuma (item só de estoque), e a
   * trava da nota **não** é avaliada — ausência de nota não é nota sem XML.
   */
  chaves_nfe?: string[] | null;
}

/** Uma trava disparada, com a origem do dado que a disparou (rastreabilidade). */
export class TravaElegibilidadeDto {
  @ApiProperty({
    description: 'Qual trava de custo disparou.',
    enum: ['custo_estoque_nao_confiavel', 'custo_nota_sem_xml'],
    example: 'custo_estoque_nao_confiavel',
  })
  trava!: TravaElegibilidade;

  @ApiProperty({
    description: 'Motivo de negócio, em uma linha, para o extrato de decisão.',
    example: MOTIVO_CUSTO_NAO_CONFIAVEL,
  })
  motivo!: string;

  @ApiProperty({
    description: 'Explicação curta de qual valor concreto disparou a trava.',
    example: 'custo_fonte = "ultima_entrada" (esperado "camada_viva")',
  })
  detalhe!: string;

  @ApiProperty({
    description: 'Campo/coluna de origem do dado que disparou a trava.',
    example: 'com_fifo_completo.custo_fonte',
  })
  campo_origem!: string;
}

/**
 * Resposta da avaliação: **"este item pode receber proposta automática, e qual é o sinal de
 * performance dele?"** — insumo da T-026, que propõe o preço. Aqui não se propõe preço.
 */
export class ElegibilidadeDto {
  @ApiProperty({ example: 33090 })
  pro_codigo!: number;

  @ApiProperty({
    description:
      'true quando NENHUMA trava de custo disparou. false suprime a proposta automática — ' +
      'sem custo confiável não há base sobre a qual aplicar a faixa (US-041).',
    example: true,
  })
  elegivel!: boolean;

  @ApiProperty({
    description:
      'Motivos de negócio, deduplicados. Vazio quando o item é elegível. As duas travas de ' +
      'custo compartilham o mesmo motivo, por decisão da US-041.',
    type: [String],
    example: [],
  })
  motivos!: string[];

  @ApiProperty({
    description: 'Travas disparadas, com o valor concreto e a coluna de origem de cada uma.',
    type: [TravaElegibilidadeDto],
  })
  travas!: TravaElegibilidadeDto[];

  @ApiProperty({
    description:
      'true quando o custo do ESTOQUE sustenta a proposta — `custo_unitario` não nulo E ' +
      '(`custo_fonte = "camada_viva"` OU `custo_fonte = "ultima_entrada"` com ' +
      '`estoque_disponivel = 0`, US-047).',
    example: true,
  })
  custo_estoque_confiavel!: boolean;

  @ApiPropertyOptional({
    description:
      'De onde veio o custo que ancora a proposta. `ultima_entrada_sem_estoque` avisa ao ' +
      'aprovador que a âncora é uma COMPRA PASSADA, não o estoque presente (US-047). null ' +
      'quando nenhuma âncora foi aceita.',
    enum: ['camada_viva', 'ultima_entrada_sem_estoque'],
    example: 'camada_viva',
    nullable: true,
  })
  custo_ancora_origem!: OrigemAncoraCusto;

  @ApiPropertyOptional({
    description:
      'Declaração em uma linha da procedência da âncora, para o extrato de decisão (EP-014). ' +
      'null quando nenhuma âncora foi aceita — nesse caso a explicação está em `travas`.',
    example: DECLARACAO_ANCORA.camada_viva,
    nullable: true,
  })
  custo_ancora_declaracao!: string | null;

  @ApiPropertyOptional({
    description:
      'Custo unitário do estoque vivo, exatamente como veio. NULL é devolvido como null — ' +
      '**nunca** coalescido para 0.',
    example: 40.48,
    nullable: true,
  })
  custo_unitario!: number | null;

  @ApiPropertyOptional({
    description: 'Degrau da cadeia de custo que originou o valor.',
    example: CUSTO_FONTE_CONFIAVEL,
    nullable: true,
  })
  custo_fonte!: string | null;

  @ApiPropertyOptional({
    description:
      'true quando a nota entrando tem ao menos uma chave de NF-e; false quando o item de ' +
      'pedido não tem vínculo de XML; null quando a avaliação não recebeu nota alguma.',
    example: true,
    nullable: true,
  })
  custo_nota_vinculado_xml!: boolean | null;

  @ApiProperty({
    description: 'true quando `estoque_disponivel > 0`.',
    example: true,
  })
  com_estoque!: boolean;

  @ApiProperty({
    description: 'true quando o giro real no escopo efetivo é maior que zero.',
    example: false,
  })
  com_giro!: boolean;

  @ApiProperty({
    description:
      'Performance derivada da idade do saldo — só decide para item COM estoque e SEM giro.',
    enum: ['ruim', 'ausente', 'nao-aplicavel'],
    example: 'ruim',
  })
  classificacao_saldo!: ClassificacaoSaldo;

  @ApiPropertyOptional({
    description:
      'Idade do saldo vivo em dias (`tempo_medio_saldo_atual`), como veio da materialização ' +
      '— inclusive negativa, quando anômala.',
    example: 1893,
    nullable: true,
  })
  idade_saldo_dias!: number | null;

  @ApiProperty({
    description:
      'true quando a idade do saldo é NEGATIVA (anomalia conhecida da base, mínimo medido ' +
      '−150 dias: `data_compra` no futuro ou erro de sinal no ETL). O item NÃO é classificado ' +
      'como performance ruim com base num número impossível — cai em `ausente` (exceção ' +
      'humana) com esta bandeira levantada, para o extrato mostrar ao aprovador e a correção ' +
      'ser endereçada ao `analise-estoque-service`.',
    example: false,
  })
  idade_saldo_anomala!: boolean;

  @ApiProperty({
    description:
      'Corte de negócio da idade do saldo, em dias — declarado no retorno, não implícito.',
    example: CORTE_IDADE_SALDO_DIAS,
  })
  corte_idade_saldo_dias!: number;

  @ApiProperty({
    description: 'Instante em que esta avaliação foi montada.',
    example: '2026-07-29T12:00:00.000Z',
  })
  avaliado_em!: string;
}
