import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
// `import type` nos tipos que aparecem em assinatura decorada: o projeto compila com
// isolatedModules + emitDecoratorMetadata (TS1272 se for import de valor).
import type { ClassificacaoSaldo } from './elegibilidade.dto';
import { ElegibilidadeDto } from './elegibilidade.dto';
import { AvaliacaoPrecoDto, CorredorMarkupDto } from './faixa-markup.dto';
import type { OrigemPreco, TabelaPreco } from './preco-vigente.dto';
import type { EscopoGiro, FrescorGiro } from './sinal-giro.dto';

/**
 * Proposta de preço ancorada no markup anterior, com fallback pela faixa (US-041 / T-026).
 *
 * Vocabulário reaproveitado, nunca duplicado: `TabelaPreco`/`OrigemPreco` da US-038,
 * `EscopoGiro`/`FrescorGiro` da US-039, `ClassificacaoSaldo`/`ElegibilidadeDto` da T-025 e
 * `CorredorMarkupDto`/`AvaliacaoPrecoDto` da T-024. A faixa aplicada **é** o corredor da
 * régua, embutido verbatim — copiar seus campos criaria uma segunda verdade sobre piso e teto.
 *
 * **Sem class-validator**, pelo mesmo motivo declarado em `faixa-markup.dto.ts`: o
 * `compras-service` não tem `ValidationPipe` global (medido), e decorator de validação que
 * ninguém executa é pior que nenhum. A validação de entrada vive no service.
 */

/**
 * Folha da árvore de decisão que produziu a proposta — o `ramo` é a chave estável para o
 * extrato de decisão do EP-014; o `motivo` é a linha em prosa.
 *
 * Os quatro primeiros são os ACs 1–4 da US-041. Os três seguintes são folhas que a árvore
 * exige e os ACs não nomeiam, declaradas aqui em vez de silenciosamente colapsadas em outra:
 *
 * - `custo_caiu_sem_estoque_sinal_ausente` — custo caiu, estoque zerado e **nenhum** sinal de
 *   performance (sem giro e, sem estoque, a idade do saldo é `nao-aplicavel`). Os ACs 2 e 3
 *   cobrem giro ≥ esperado e giro < esperado; a ausência de giro não é nenhum dos dois. Regra
 *   aplicada: "não diminui preço sem motivo" ⇒ mantém o anterior e marca para confirmação.
 * - `custo_estavel` — custo novo igual ao anterior. Nenhum AC o cobre; mover o preço aqui
 *   contrariaria a âncora do PRD.
 * - `sem_historico_pela_faixa` — não há preço anterior nesta tabela: proposta derivada da
 *   faixa, posicionada pelo sinal de performance (PRD#escopo, regra 3).
 * - `sem_historico_herdado_do_grupo` — não há preço anterior **e** o grupo de itens iguais tem
 *   similar com markup praticado: o markup é herdado deles (US-042 / T-029, ver
 *   `CRITERIO_HERANCA_GRUPO`). Precede o fallback pela faixa; sem similar, a faixa continua
 *   sendo o caminho.
 */
export type RamoProposta =
  | 'custo_caiu_com_estoque'
  | 'custo_caiu_sem_estoque_performance_boa'
  | 'custo_caiu_sem_estoque_performance_ruim'
  | 'custo_caiu_sem_estoque_sinal_ausente'
  | 'custo_subiu'
  | 'custo_estavel'
  | 'sem_historico_herdado_do_grupo'
  | 'sem_historico_pela_faixa'
  | 'inelegivel';

/** Direção da variação do custo composto da nota contra o custo do estoque vivo. */
export type DirecaoCusto = 'caiu' | 'subiu' | 'estavel' | 'indeterminado';

/**
 * Posição dentro do corredor da faixa, ditada pelo sinal de performance.
 *
 * **Três degraus discretos, não interpolação contínua** — decisão desta task (ver
 * `FRACAO_POSICAO`).
 */
export type PosicaoPerformance = 'alta' | 'neutra' | 'baixa';

/** O que sustentou a posição: o par giro real × esperado, a idade do saldo, ou nada. */
export type BaseSinalPerformance = 'giro' | 'idade_saldo' | 'ausente';

/** O que aconteceu com o markup que o item já praticava (AC4 pede isso explícito). */
export type DestinoMarkupAnterior = 'mantido' | 'cedido_ao_teto' | 'elevado_ao_piso';

/** Por que a tabela não tem histórico de preço — a ausência é nomeada, nunca zerada. */
export type MotivoHistoricoAusente = 'preco_anterior_ausente' | 'preco_anterior_zerado';

/**
 * # Função de posicionamento dentro da faixa — decisão da T-026
 *
 * A faixa de custo dá o **corredor**; o sinal de performance diz **onde** dentro dele o preço
 * do item sem histórico nasce. Nenhum artefato especifica a função (US-041 declara *que* a
 * performance posiciona; PRD#escopo regra 3 idem), então ela é decidida aqui.
 *
 * **Decisão: três degraus discretos**, aplicados sobre o **preço** e não sobre o markup —
 * `preco_minimo` (arredondado PARA CIMA) e `preco_maximo` (PARA BAIXO) já vêm da régua
 * (T-024), então interpolar entre eles reaproveita o arredondamento seguro da régua em vez de
 * criar um segundo:
 *
 * | posição  | fração do corredor | quando                                                |
 * |----------|--------------------|-------------------------------------------------------|
 * | `alta`   | 1,0 → `preco_maximo` | giro real ≥ giro esperado (ambos > 0)               |
 * | `neutra` | 0,5 → meio do corredor | sinal ausente: sem giro e sem idade julgável       |
 * | `baixa`  | 0,0 → `preco_minimo` | giro real < giro esperado, **ou** idade do saldo ≥ 365 dias |
 *
 * ## Alternativa descartada: interpolação linear sobre a razão `giro_real / giro_esperado`
 *
 * Seria `fração = clamp((razão − a) / (b − a), 0, 1)` — contínua e, à primeira vista, mais
 * informativa. Descartada por quatro razões:
 *
 * 1. **Exige duas constantes que nenhum artefato declara.** A razão é ilimitada superiormente,
 *    então a função linear precisa de um ponto de saturação (`b`: razão 1,5? 2? 3?) e de um
 *    ponto de partida (`a`). Escolher esses números seria inventar a especificação que a task
 *    foi encarregada de **decidir e registrar** — mas registraria dois números arbitrários em
 *    vez de uma regra derivável dos artefatos. Os três degraus não introduzem constante
 *    nenhuma: usam exatamente o comparador `giro_real ≥ giro_esperado` que os ACs 2 e 3 da
 *    US-041 já escrevem, e o corte de 365 dias que a T-025 já declarou.
 * 2. **Falsa precisão sobre dado de cadência lenta.** O par giro real/esperado vem de
 *    `com_fifo_completo`, materializada pelo ETL do `analise-estoque-service` a cada ~3 dias
 *    (a janela de frescura é 72h justamente por isso). Uma função contínua faria o preço
 *    proposto variar centavo a centavo com o ruído de um número que só muda a cada ciclo, e
 *    dois itens com razão 1,21 e 1,23 sairiam com preços diferentes sem que o aprovador
 *    conseguisse explicar por quê.
 * 3. **O corredor não é campo contínuo de decisão.** O PRD é explícito: a faixa é *"corredor
 *    de validação"*, não escolha aberta; e a curva "custo menor ⇒ markup maior" (regra 4) está
 *    **confirmada mas não computável** hoje, por falta de preço final mínimo declarado. Uma
 *    função contínua sobre a razão de giro pareceria a curva que a regra 4 pede e **não é**,
 *    convidando a lê-la como se o motor já modelasse rentabilidade.
 * 4. **Auditabilidade.** O extrato de decisão do EP-014 precisa do motivo em **uma linha**.
 *    "posicionado no topo do corredor porque o giro real supera o esperado" é revisável por um
 *    humano; "posicionado a 63,4% do corredor" não é — e o item sem histórico sai marcado
 *    *precisa de confirmação de mercado* exatamente para ser revisado.
 *
 * **Limite respeitado:** a posição depende **só** da performance. O custo entra apenas por
 * **selecionar a faixa** (como a régua da T-024 já faz); não há termo de custo na fração — a
 * regra 4 do PRD#escopo **não** é reintroduzida por esta decisão.
 */
export const FRACAO_POSICAO: Readonly<Record<PosicaoPerformance, number>> = {
  alta: 1,
  neutra: 0.5,
  baixa: 0,
};

/**
 * Marcação de exceção do PRD#escopo (regra 3), grafada como o artefato a declara.
 *
 * **Exceção e proposta são independentes** (US-041): marcar não suprime o preço — o item
 * marcado recebe as duas coisas. A única condição que suprime a proposta é custo não
 * confiável (`MOTIVO_CUSTO_NAO_CONFIAVEL`, T-025).
 */
export const MARCACAO_CONFIRMACAO_MERCADO = 'precisa de confirmação de mercado';

/**
 * # Critério de derivação do markup do grupo — decisão da T-029 (US-042)
 *
 * **Decisão: mediana simples dos markups praticados pelos similares do grupo, limitada ao
 * corredor da faixa de custo do item.** Extrato em uma linha: *"markup herdado de N similares
 * do grupo (mediana X%), limitado ao corredor Y%–Z% da faixa"*.
 *
 * Definições operacionais:
 *
 * - **Similar** = outro produto no mesmo `group_id` de `com_relacionamento_itens` — o vínculo
 *   **vivo**, o mesmo ativo que sustenta o giro (US-039), e não o `group_id` materializado em
 *   `com_fifo_completo`, que pode estar atrás do agrupamento.
 * - **Markup praticado** = `preço vigente da tabela ÷ custo do similar − 1`, e só conta quando
 *   **as duas** pontas são positivas. Similar sem preço ou sem custo **não entra** na mediana:
 *   é ausência, nunca zero herdado.
 * - **Por tabela.** A mediana é dos markups *daquela* tabela — a invariante 1 do
 *   `PropostaService` (três tabelas independentes) continua valendo.
 * - **Piso pela trava da régua.** O markup herdado vira preço candidato, é limitado ao corredor
 *   e passa por `ReguaService.avaliarPreco` como qualquer outro. A comparação de piso **não** é
 *   reimplementada.
 *
 * ## Alternativa descartada: média ponderada pela venda de cada similar
 *
 * Descartada com números medidos na base (registrados em US-042): dos 75 itens que hoje têm
 * similar com markup praticado, mediana e média divergem em apenas **6** (8%), e em **32** (43%)
 * existe **1** único similar — onde todo critério devolve o mesmo número. Além de não comprar
 * quase nada, ponderar pela venda concentra a decisão no item mais sujeito a preço promocional
 * (o de maior giro), exige constantes que nenhum artefato declara (qual janela de venda? o que
 * fazer com venda 0?) e destrói a auditabilidade que o item herdado — que ainda sai para
 * confirmação — precisa ter. Também descartado *"só o similar de maior giro"*: é a mediana de
 * uma amostra de 1, com o máximo da fragilidade a outlier.
 *
 * **Cobertura medida (2026-07-30, run corrente):** 13.375 produtos (34,3%) em grupo com ≥ 2
 * membros; dos 303 candidatos à herança no varejo, 75 (24,8%) têm similar com markup praticado
 * e em 70 a herança muda a proposta. O fallback pela faixa segue sendo o caminho da maioria.
 */
export const CRITERIO_HERANCA_GRUPO = 'mediana simples, limitada ao corredor da faixa';

/** Tolerância de comparação de custo, em R$ — abaixo dela o custo é considerado estável. */
export const TOLERANCIA_CUSTO_REAIS = 0.0001;

/** Sinal de performance do item, com o que o sustentou e a posição que ele dita na faixa. */
export class SinalPerformanceDto {
  @ApiProperty({
    description: 'Degrau do corredor que este sinal dita.',
    enum: ['alta', 'neutra', 'baixa'],
    example: 'alta',
  })
  posicao!: PosicaoPerformance;

  @ApiProperty({
    description: 'Fração do corredor correspondente à posição (1 = teto, 0,5 = meio, 0 = piso).',
    example: 1,
  })
  fracao_faixa!: number;

  @ApiProperty({
    description: 'O que sustentou a posição.',
    enum: ['giro', 'idade_saldo', 'ausente'],
    example: 'giro',
  })
  base!: BaseSinalPerformance;

  @ApiPropertyOptional({
    description: 'Giro real no escopo efetivo, em un/dia; null quando a coluna está vazia.',
    example: 3.42,
    nullable: true,
  })
  giro_real!: number | null;

  @ApiPropertyOptional({
    description: 'Giro esperado (demanda de planejamento) no escopo efetivo, em un/dia.',
    example: 2.1,
    nullable: true,
  })
  giro_esperado!: number | null;

  @ApiPropertyOptional({
    description:
      'Razão giro real / giro esperado — **informativa apenas**: a posição vem do ' +
      'comparador `real ≥ esperado`, não desta razão (ver FRACAO_POSICAO).',
    example: 1.6286,
    nullable: true,
  })
  razao_giro!: number | null;

  @ApiProperty({
    description: 'Escopo sobre o qual o giro foi medido — o grupo de itens iguais quando há.',
    enum: ['grupo', 'produto'],
    example: 'grupo',
  })
  escopo_giro!: EscopoGiro;

  @ApiProperty({
    description: 'Classificação por idade do saldo (T-025) — decide quando não há giro.',
    enum: ['ruim', 'ausente', 'nao-aplicavel'],
    example: 'nao-aplicavel',
  })
  classificacao_saldo!: ClassificacaoSaldo;

  @ApiProperty({
    description: 'Frescura da materialização que originou os números de giro.',
    enum: ['fresco', 'obsoleto', 'sem-materializacao'],
    example: 'fresco',
  })
  frescor!: FrescorGiro;

  @ApiProperty({
    description:
      'true quando o giro veio de materialização velha ou ausente — o preço proposto sai ' +
      'marcado para confirmação de mercado, nunca aceito em silêncio.',
    example: false,
  })
  dado_desatualizado!: boolean;

  @ApiProperty({
    description: 'Explicação curta do sinal, para compor o motivo em uma linha.',
    example: 'giro real 3,42 ≥ esperado 2,10 un/dia (escopo grupo)',
  })
  detalhe!: string;
}

/** Proposta de UMA tabela de preço — as três são julgadas de forma independente. */
export class PropostaTabelaDto {
  @ApiProperty({ enum: ['varejo', 'atacado_especial', 'atacado'], example: 'varejo' })
  tabela!: TabelaPreco;

  @ApiProperty({ example: 'Varejo' })
  rotulo!: string;

  @ApiProperty({
    description: 'Folha da árvore de decisão que produziu esta proposta.',
    enum: [
      'custo_caiu_com_estoque',
      'custo_caiu_sem_estoque_performance_boa',
      'custo_caiu_sem_estoque_performance_ruim',
      'custo_caiu_sem_estoque_sinal_ausente',
      'custo_subiu',
      'custo_estavel',
      'sem_historico_herdado_do_grupo',
      'sem_historico_pela_faixa',
      'inelegivel',
    ],
    example: 'custo_caiu_com_estoque',
  })
  ramo!: RamoProposta;

  @ApiProperty({
    description: 'Motivo em UMA linha, em pt-BR — consumível pelo extrato de decisão (EP-014).',
    example:
      'Custo caiu de R$ 44,00 para R$ 40,48, há 12 un em estoque e o markup anterior de ' +
      '331,4% está dentro do corredor 200%–1000% da faixa: preço mantido em R$ 189,90.',
  })
  motivo!: string;

  @ApiProperty({
    description:
      'true quando esta tabela tem preço anterior utilizável como âncora. false manda o item ' +
      'para o fallback pela faixa — nunca para "sem proposta".',
    example: true,
  })
  tem_historico!: boolean;

  @ApiPropertyOptional({
    description:
      'Por que não há histórico nesta tabela; null quando há. `preco_anterior_zerado` ' +
      'significa que a fonte devolveu 0 de fato — o zero é exibido como veio, nunca inventado.',
    enum: ['preco_anterior_ausente', 'preco_anterior_zerado'],
    nullable: true,
  })
  historico_ausente_motivo!: MotivoHistoricoAusente | null;

  @ApiPropertyOptional({
    description:
      'Preço vigente da tabela, em R$, **verbatim da fonte**: null quando ausente — jamais ' +
      'coalescido para 0 (item sem histórico expõe ausência explícita, não zero).',
    example: 189.9,
    nullable: true,
  })
  preco_anterior!: number | null;

  @ApiPropertyOptional({
    description: 'De onde o preço anterior foi lido; null quando não há preço anterior.',
    enum: ['erp', 'espelho', 'ausente'],
    nullable: true,
  })
  preco_anterior_origem!: OrigemPreco | null;

  @ApiPropertyOptional({
    description:
      'Markup que o preço anterior representava sobre o custo anterior, em % — a âncora da ' +
      'US-041. null sem histórico.',
    example: 331.4,
    nullable: true,
  })
  markup_anterior_pct!: number | null;

  @ApiPropertyOptional({
    description:
      'Preço proposto, em R$. **null SOMENTE quando o item é inelegível** (custo não ' +
      'confiável) — nenhum item elegível sai sem proposta nas três tabelas.',
    example: 189.9,
    nullable: true,
  })
  preco_proposto!: number | null;

  @ApiPropertyOptional({
    description: 'Markup do preço proposto sobre o custo atual, em %.',
    example: 369.1,
    nullable: true,
  })
  markup_proposto_pct!: number | null;

  @ApiPropertyOptional({
    description: 'preco_proposto − preco_anterior, em R$; null sem histórico.',
    example: 0,
    nullable: true,
  })
  variacao_preco!: number | null;

  @ApiPropertyOptional({
    description: 'O que aconteceu com o markup anterior; null sem histórico.',
    enum: ['mantido', 'cedido_ao_teto', 'elevado_ao_piso'],
    nullable: true,
  })
  markup_anterior_destino!: DestinoMarkupAnterior | null;

  @ApiPropertyOptional({
    description:
      'Quantos similares do grupo sustentaram o markup herdado (US-042). null quando a ' +
      'proposta não veio de herança — o extrato mostra "de quantos", nunca um número inventado.',
    example: 3,
    nullable: true,
  })
  similares_considerados!: number | null;

  @ApiPropertyOptional({
    description:
      'Mediana dos markups praticados pelos similares, em %, **antes** da limitação ao ' +
      'corredor da faixa. null quando não houve herança.',
    example: 142.7,
    nullable: true,
  })
  markup_herdado_pct!: number | null;

  @ApiPropertyOptional({
    description:
      'Corredor da régua aplicado (T-024), verbatim. Para item **sem** histórico é a "faixa ' +
      'aplicada" que o extrato exibe no lugar dos campos "anterior". null só no item inelegível.',
    type: CorredorMarkupDto,
    nullable: true,
  })
  faixa_aplicada!: CorredorMarkupDto | null;

  @ApiPropertyOptional({
    description: 'Posição dentro do corredor efetivamente usada nesta tabela.',
    enum: ['alta', 'neutra', 'baixa'],
    nullable: true,
  })
  posicao_na_faixa!: PosicaoPerformance | null;

  @ApiPropertyOptional({
    description: 'Fração do corredor usada (0 = piso, 1 = teto).',
    example: 0.5,
    nullable: true,
  })
  fracao_na_faixa!: number | null;

  @ApiPropertyOptional({
    description:
      'Veredito da trava de piso da régua (T-024) sobre o preço proposto — a validação é ' +
      'dela, não replicada aqui. null no item inelegível (não há preço a avaliar).',
    type: AvaliacaoPrecoDto,
    nullable: true,
  })
  avaliacao_piso!: AvaliacaoPrecoDto | null;

  @ApiProperty({
    description: 'true quando a trava da régua aceitou o preço (piso não furado).',
    example: true,
  })
  piso_respeitado!: boolean;

  @ApiProperty({
    description:
      'true quando o preço passa do teto da faixa — aviso, não recusa (semântica da T-024). ' +
      'Caso típico: atacado, cujo markup é valor único (67%), com preço anterior mais alto.',
    example: false,
  })
  acima_do_teto!: boolean;

  @ApiProperty({
    description: 'true quando esta proposta precisa de aval humano antes de valer.',
    example: false,
  })
  precisa_confirmacao_mercado!: boolean;

  @ApiProperty({
    description: 'Marcações de negócio da proposta (ex.: "precisa de confirmação de mercado").',
    type: [String],
    example: [],
  })
  marcacoes!: string[];

  @ApiProperty({
    description: 'Avisos para o extrato — teto excedido, piso bloqueando redução, dado velho.',
    type: [String],
    example: [],
  })
  avisos!: string[];
}

/** Proposta de UM item da nota: o custo, o sinal, e as três tabelas julgadas separadamente. */
export class PropostaItemDto {
  @ApiProperty({ example: 33090 })
  pro_codigo!: number;

  @ApiProperty({ example: 'PASTILHA DE FREIO DIANTEIRA' })
  pro_descricao!: string;

  @ApiProperty({ description: 'Chaves de NF-e que cobrem o item.', type: [String] })
  chaves_nfe!: string[];

  @ApiProperty({ description: 'Quantidade faturada na unidade base.', example: 12 })
  quantidade!: number;

  @ApiProperty({
    description: 'Custo composto da nota entrando (US-037), em R$ por unidade — o custo ATUAL.',
    example: 40.48,
  })
  custo_atual!: number;

  @ApiPropertyOptional({
    description:
      'Custo FIFO do estoque vivo (`com_fifo_completo.custo_unitario`), em R$ — o custo ' +
      'ANTERIOR. null é ausência de custo no ERP, **nunca** zero.',
    example: 44,
    nullable: true,
  })
  custo_anterior!: number | null;

  @ApiPropertyOptional({
    description: 'Degrau da cadeia de custo do valor anterior (`custo_fonte`).',
    example: 'camada_viva',
    nullable: true,
  })
  custo_anterior_fonte!: string | null;

  @ApiPropertyOptional({
    description: 'custo_atual − custo_anterior, em R$; null quando não há custo anterior.',
    example: -3.52,
    nullable: true,
  })
  variacao_custo!: number | null;

  @ApiProperty({
    description:
      'Direção da variação do custo. `indeterminado` quando não há custo anterior — e aí a ' +
      'âncora não existe.',
    enum: ['caiu', 'subiu', 'estavel', 'indeterminado'],
    example: 'caiu',
  })
  direcao_custo!: DirecaoCusto;

  @ApiPropertyOptional({
    description: 'Saldo disponível (`com_fifo_completo.estoque_disponivel`).',
    example: 12,
    nullable: true,
  })
  estoque_disponivel!: number | null;

  @ApiProperty({
    description: 'true quando `estoque_disponivel > 0`.',
    example: true,
  })
  com_estoque!: boolean;

  @ApiProperty({
    description:
      'true quando há **camada FIFO antiga viva** — `custo_fonte = "camada_viva"` com saldo ' +
      'disponível. Enquanto ela vive, custo e preço anteriores permanecem (restrição de custo ' +
      'FIFO do PRD): é a condição que sustenta o ramo `custo_caiu_com_estoque`.',
    example: true,
  })
  camada_fifo_antiga_viva!: boolean;

  @ApiProperty({
    description: 'Veredito das travas de elegibilidade (T-025), verbatim.',
    type: ElegibilidadeDto,
  })
  elegibilidade!: ElegibilidadeDto;

  @ApiProperty({
    description:
      'true quando o item pode receber proposta automática. false é o **único** caso sem ' +
      'preço nas três tabelas.',
    example: true,
  })
  elegivel!: boolean;

  @ApiProperty({
    description: 'Sinal de performance do item — o mesmo para as três tabelas (é do produto).',
    type: SinalPerformanceDto,
  })
  sinal_performance!: SinalPerformanceDto;

  @ApiProperty({
    description: 'Sempre 3 entradas — varejo, atacado especial e atacado, nesta ordem.',
    type: [PropostaTabelaDto],
  })
  propostas!: PropostaTabelaDto[];
}

/** Totais da nota — o medidor dos invariantes da US-041. */
export class TotaisPropostaDto {
  @ApiProperty({ example: 42 })
  itens!: number;

  @ApiProperty({ example: 40 })
  elegiveis!: number;

  @ApiProperty({ example: 2 })
  inelegiveis!: number;

  @ApiProperty({
    description: 'Propostas de preço emitidas (itens elegíveis × 3 tabelas).',
    example: 120,
  })
  propostas!: number;

  @ApiProperty({
    description:
      'Itens **elegíveis** sem proposta em alguma das três tabelas. Invariante da US-041: ' +
      'sempre 0.',
    example: 0,
  })
  elegiveis_sem_proposta!: number;

  @ApiProperty({
    description: 'Propostas derivadas da faixa por ausência de preço anterior.',
    example: 9,
  })
  sem_historico!: number;

  @ApiProperty({
    description:
      'Propostas sem preço anterior cujo markup foi **herdado dos similares do grupo** ' +
      '(US-042) — precedem o fallback pela faixa e não são contadas em `sem_historico`.',
    example: 2,
  })
  sem_historico_herdado!: number;

  @ApiProperty({ description: 'Propostas marcadas para confirmação de mercado.', example: 9 })
  marcadas_confirmacao_mercado!: number;

  @ApiProperty({
    description: 'Propostas que a trava de piso da régua recusou. Invariante: sempre 0.',
    example: 0,
  })
  piso_furado!: number;

  @ApiProperty({ description: 'Propostas acima do teto da faixa (aviso, não recusa).', example: 3 })
  acima_do_teto!: number;
}

/** Resposta do endpoint: a nota inteira em UMA requisição (padrão da T-020). */
export class PropostaNotaDto {
  @ApiProperty({ example: 'clx0k9v2h0000abcd1234efgh' })
  pedido_id!: string;

  @ApiPropertyOptional({
    description: 'Chave da NF consultada; null quando a consulta foi pelo pedido inteiro.',
    nullable: true,
  })
  chave_nfe!: string | null;

  @ApiPropertyOptional({ example: 1234, nullable: true })
  for_codigo!: number | null;

  @ApiProperty({ type: TotaisPropostaDto })
  totais!: TotaisPropostaDto;

  @ApiProperty({ type: [PropostaItemDto] })
  itens!: PropostaItemDto[];

  @ApiProperty({
    description: 'Instante em que a proposta foi montada.',
    example: '2026-07-30T12:00:00.000Z',
  })
  proposto_em!: string;
}
