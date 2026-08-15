import { Injectable, Logger } from '@nestjs/common';
import { CustoService } from './custo.service';
import { CustoComposicaoDto, CustoItemDto } from './dto/custo-item.dto';
import {
  AvaliacaoElegibilidadeInput,
  CUSTO_FONTE_CONFIAVEL,
  ElegibilidadeDto,
  MOTIVO_CUSTO_NAO_CONFIAVEL,
} from './dto/elegibilidade.dto';
import { CorredorMarkupDto } from './dto/faixa-markup.dto';
import { PrecoVigenteDto, TABELAS_PRECO, TabelaPreco } from './dto/preco-vigente.dto';
import {
  DestinoMarkupAnterior,
  DirecaoCusto,
  FRACAO_POSICAO,
  MARCACAO_CONFIRMACAO_MERCADO,
  MotivoHistoricoAusente,
  PosicaoPerformance,
  PropostaItemDto,
  PropostaNotaDto,
  PropostaTabelaDto,
  RamoProposta,
  SinalPerformanceDto,
  TOLERANCIA_CUSTO_REAIS,
  TotaisPropostaDto,
} from './dto/proposta-preco.dto';
import { SinalGiroDto } from './dto/sinal-giro.dto';
import { ElegibilidadeService } from './elegibilidade.service';
import { EstoqueFifoRow, numeroOuNulo, PropostaRepository } from './proposta.repository';
import { ReguaService } from './regua.service';

/**
 * Markups praticados pelos similares do grupo de um item, **por tabela** (US-042 / T-029).
 * Tabela ausente do mapa = nenhum similar com as duas pontas (preço e custo) positivas.
 */
type MarkupsHerdados = Map<TabelaPreco, number[]>;

/**
 * Motor de proposta de preço (US-041 / T-026).
 *
 * Orquestra as cinco fatias anteriores e produz, para **cada item** de uma NF conferida, **uma
 * proposta por tabela de preço** com o motivo em uma linha:
 *
 * | fatia | vem de | papel aqui |
 * |---|---|---|
 * | custo composto da nota (US-037) | `CustoService` | custo **atual** e `chaves_nfe` |
 * | três tabelas vigentes (US-038) | `TabelasPrecoService` | preço **anterior** por tabela |
 * | giro real × esperado (US-039) | `GiroService` | sinal de performance |
 * | régua e piso (US-040 / T-024) | `ReguaService` | corredor da faixa e **a trava de piso** |
 * | travas de elegibilidade (T-025) | `ElegibilidadeService` | quem pode receber proposta |
 *
 * ---
 * ## Invariantes que este service sustenta (regra de 2026-07-29)
 *
 * 1. **As três tabelas são julgadas de forma independente.** Cada tabela tem seu próprio
 *    corredor (a faixa é por custo, mas o markup é por tabela), seu próprio preço anterior e
 *    portanto seu próprio ramo e motivo. Não existe veredito único replicado — o laço por
 *    tabela roda a árvore inteira de novo.
 * 2. **Nenhum item elegível sai sem proposta.** Sem preço anterior, o motor tenta primeiro
 *    **herdar o markup dos similares do grupo** (US-042 / T-029,
 *    `sem_historico_herdado_do_grupo`); sem similar com markup praticado, cai no fallback pela
 *    faixa
 *    (`sem_historico_pela_faixa`) — a proposta é derivada do corredor, posicionada pelo sinal
 *    de performance. `totais.elegiveis_sem_proposta` mede isso e é 0 por construção.
 * 3. **Item inelegível é o único caso sem preço.** `custo nao confiavel` (T-025) suprime a
 *    proposta porque não há base sobre a qual aplicar a faixa.
 * 4. **Nenhuma proposta fura o piso — pela trava da T-024, não por checagem própria.** Todo
 *    preço candidato passa por `ReguaService.avaliarPreco`; recusa (única recusa dura da régua)
 *    faz o preço ser corrigido para `preco_minimo` do corredor e o ajuste é registrado em
 *    `avisos`. Este service **não** reimplementa a comparação de piso.
 * 5. **Extrato honesto.** Item com histórico expõe custo anterior → atual e preço anterior →
 *    proposto. Item **sem** histórico expõe os campos "anterior" como `null` — nunca zerados —
 *    e traz no lugar `faixa_aplicada` + `sinal_performance`.
 *
 * ## Semântica de teto herdada da T-024
 *
 * Acima do teto **não é recusa**: `avaliarPreco` devolve `aceito: true` com
 * `acima_do_teto: true` e um aviso. Caso corriqueiro no atacado, cujo markup é valor único
 * (67% = piso = teto): manter um preço anterior mais alto que 67% marca a proposta, não a
 * invalida. A âncora do PRD ("não diminui preço sem motivo") vence o teto quando há histórico.
 *
 * ## Posicionamento dentro da faixa
 *
 * Decisão desta task, com a alternativa descartada, registrada em
 * `dto/proposta-preco.dto.ts` (`FRACAO_POSICAO`): **três degraus discretos** sobre o preço do
 * corredor (piso / meio / teto), ditados pelo sinal de performance. A posição **não** tem termo
 * de custo — a curva "custo menor ⇒ markup maior" da regra 4 do PRD#escopo está confirmada mas
 * **não é computável hoje** (falta preço final mínimo declarado) e não é reintroduzida aqui.
 *
 * ## Onde mora o acesso a dados
 *
 * Em `proposta.repository.ts` (extraído pela T-027). A T-026 tinha deixado as três leituras
 * aqui dentro como **desvio de convenção declarado**, porque os `## Outputs` daquela task não
 * incluíam repositório algum; a dívida foi registrada no BOARD e está quitada. Este service
 * voltou a ser só orquestração + cálculo: nenhuma linha de Prisma mora mais nele.
 */
@Injectable()
export class PropostaService {
  private readonly logger = new Logger(PropostaService.name);

  constructor(
    private readonly repositorio: PropostaRepository,
    private readonly custo: CustoService,
    private readonly regua: ReguaService,
    private readonly elegibilidade: ElegibilidadeService,
  ) {}

  // ---------------------------------------------------------------------------
  // Entradas (a nota inteira em UMA requisição — padrão da T-020)
  // ---------------------------------------------------------------------------

  /** Propõe preço para todos os itens cobertos por uma chave de NF-e. */
  async porNota(chaveNfe: string): Promise<PropostaNotaDto[]> {
    const composicoes = await this.custo.custoDaNota(chaveNfe);
    return this.propor(composicoes);
  }

  /** Propõe preço para todos os itens de um pedido (nota ainda não vinculada). */
  async porPedido(pedidoId: string): Promise<PropostaNotaDto> {
    const composicao = await this.custo.custoDoPedido(pedidoId);
    const [dto] = await this.propor([composicao]);
    return dto;
  }

  // ---------------------------------------------------------------------------
  // Orquestração
  // ---------------------------------------------------------------------------

  async propor(composicoes: CustoComposicaoDto[]): Promise<PropostaNotaDto[]> {
    const lista = composicoes ?? [];
    const codigos = Array.from(
      new Set(
        lista.flatMap((c) => (c.itens ?? []).map((i) => Number(i.pro_codigo))).filter((c) => c > 0),
      ),
    );

    const [estoque, precos, sinais, grupos] = await Promise.all([
      this.repositorio.lerEstoqueFifo(codigos),
      this.repositorio.lerPrecos(codigos),
      this.repositorio.lerSinais(codigos),
      this.repositorio.lerGruposDosItens(codigos),
    ]);

    const herdados = await this.markupsDosSimilares(codigos, grupos, estoque, precos);

    const propostoEm = new Date().toISOString();
    const saida: PropostaNotaDto[] = [];

    for (const composicao of lista) {
      const itens: PropostaItemDto[] = [];
      for (const item of composicao.itens ?? []) {
        itens.push(
          await this.proporItem(
            item,
            estoque.get(Number(item.pro_codigo)) ?? null,
            precos.get(Number(item.pro_codigo)) ?? null,
            sinais.get(Number(item.pro_codigo)) ?? null,
            herdados.get(Number(item.pro_codigo)) ?? null,
          ),
        );
      }
      saida.push({
        pedido_id: composicao.pedido_id,
        chave_nfe: composicao.chave_nfe ?? null,
        for_codigo: composicao.for_codigo ?? null,
        totais: this.totalizar(itens),
        itens,
        proposto_em: propostoEm,
      });
    }

    return saida;
  }

  // ---------------------------------------------------------------------------
  // Um item: elegibilidade, sinal, e as três tabelas independentes
  // ---------------------------------------------------------------------------

  private async proporItem(
    item: CustoItemDto,
    fifo: EstoqueFifoRow | null,
    precos: PrecoVigenteDto | null,
    sinalGiro: SinalGiroDto | null,
    herdado: MarkupsHerdados | null,
  ): Promise<PropostaItemDto> {
    const proCodigo = Number(item.pro_codigo);
    const custoAtual = numeroOuNulo(item.custo_unitario) ?? 0;
    const custoAnterior = fifo?.custo_unitario ?? null;
    const estoqueDisponivel = fifo?.estoque_disponivel ?? null;
    const giroReal = this.giroDe(sinalGiro, 'giro_real');
    const giroEsperado = this.giroDe(sinalGiro, 'giro_esperado');

    const entrada: AvaliacaoElegibilidadeInput = {
      pro_codigo: proCodigo,
      custo_unitario: custoAnterior,
      custo_fonte: fifo?.custo_fonte ?? null,
      estoque_disponivel: estoqueDisponivel,
      giro_real: giroReal,
      tempo_medio_saldo_atual: fifo?.tempo_medio_saldo_atual ?? null,
      chaves_nfe: item.chaves_nfe ?? [],
    };
    const elegibilidade: ElegibilidadeDto = this.elegibilidade.avaliar(entrada);

    // Guarda desta task: custo composto da nota não positivo não é base de preço. Compartilha
    // o motivo de negócio `custo nao confiavel` de propósito — a invariante "item inelegível é
    // o único caso sem preço" continua verdadeira, com um único motivo de record.
    const custoAtualUtilizavel = custoAtual > 0;
    const elegivel = elegibilidade.elegivel && custoAtualUtilizavel;

    const comEstoque = elegibilidade.com_estoque;
    const camadaFifoAntigaViva =
      (fifo?.custo_fonte ?? null) === CUSTO_FONTE_CONFIAVEL && comEstoque;

    const sinal = this.sinalPerformance(sinalGiro, elegibilidade, giroReal, giroEsperado);
    const { direcao, variacao } = this.direcaoCusto(custoAtual, custoAnterior);

    const propostas: PropostaTabelaDto[] = [];
    for (const meta of TABELAS_PRECO) {
      propostas.push(
        await this.proporTabela({
          tabela: meta.tabela,
          rotulo: meta.rotulo,
          elegivel,
          elegibilidade,
          custoAtualUtilizavel,
          custoAtual,
          custoAnterior,
          direcao,
          comEstoque,
          camadaFifoAntigaViva,
          estoqueDisponivel,
          sinal,
          precos,
          herdado,
        }),
      );
    }

    return {
      pro_codigo: proCodigo,
      pro_descricao: item.pro_descricao,
      chaves_nfe: item.chaves_nfe ?? [],
      quantidade: item.quantidade,
      custo_atual: custoAtual,
      custo_anterior: custoAnterior,
      custo_anterior_fonte: fifo?.custo_fonte ?? null,
      variacao_custo: variacao,
      direcao_custo: direcao,
      estoque_disponivel: estoqueDisponivel,
      com_estoque: comEstoque,
      camada_fifo_antiga_viva: camadaFifoAntigaViva,
      elegibilidade,
      elegivel,
      sinal_performance: sinal,
      propostas,
    };
  }

  // ---------------------------------------------------------------------------
  // A árvore de decisão, por tabela
  // ---------------------------------------------------------------------------

  private async proporTabela(ctx: {
    tabela: TabelaPreco;
    rotulo: string;
    elegivel: boolean;
    elegibilidade: ElegibilidadeDto;
    custoAtualUtilizavel: boolean;
    custoAtual: number;
    custoAnterior: number | null;
    direcao: DirecaoCusto;
    comEstoque: boolean;
    camadaFifoAntigaViva: boolean;
    estoqueDisponivel: number | null;
    sinal: SinalPerformanceDto;
    precos: PrecoVigenteDto | null;
    herdado: MarkupsHerdados | null;
  }): Promise<PropostaTabelaDto> {
    const linha = (ctx.precos?.precos ?? []).find((p) => p.tabela === ctx.tabela) ?? null;
    const precoAnterior = linha?.valor ?? null;
    const precoAnteriorOrigem = linha?.origem ?? null;

    // ---- Item inelegível: o ÚNICO caso sem preço --------------------------------
    if (!ctx.elegivel) {
      return this.propostaInelegivel(ctx, precoAnterior, precoAnteriorOrigem);
    }

    const corredor = await this.regua.corredor(ctx.tabela, ctx.custoAtual);

    const historicoAusente = this.historicoAusente(precoAnterior);
    const temHistorico = historicoAusente == null && ctx.custoAnterior != null;

    if (!temHistorico) {
      // US-042: os similares do grupo vêm ANTES da faixa pura. Sem similar com markup
      // praticado nesta tabela, o fallback pela faixa continua sendo o caminho.
      const markups = ctx.herdado?.get(ctx.tabela) ?? [];
      if (markups.length) {
        return this.propostaHerdadaDoGrupo(
          ctx,
          corredor,
          precoAnterior,
          precoAnteriorOrigem,
          markups,
        );
      }
      return this.propostaPelaFaixa(ctx, corredor, precoAnterior, precoAnteriorOrigem, linha);
    }

    return this.propostaAncorada(
      ctx,
      corredor,
      precoAnterior as number,
      precoAnteriorOrigem,
      ctx.custoAnterior as number,
    );
  }

  /** Sem custo confiável não há base sobre a qual aplicar a faixa (US-041). */
  private propostaInelegivel(
    ctx: {
      tabela: TabelaPreco;
      rotulo: string;
      elegibilidade: ElegibilidadeDto;
      custoAtualUtilizavel: boolean;
      custoAtual: number;
      sinal: SinalPerformanceDto;
    },
    precoAnterior: number | null,
    precoAnteriorOrigem: ReturnType<() => PropostaTabelaDto['preco_anterior_origem']>,
  ): PropostaTabelaDto {
    const detalhes = ctx.elegibilidade.travas.map((t) => t.detalhe);
    if (!ctx.custoAtualUtilizavel) {
      detalhes.push(
        `custo composto da nota não positivo (${this.brl(ctx.custoAtual)}) — sem base de cálculo`,
      );
    }
    return {
      tabela: ctx.tabela,
      rotulo: ctx.rotulo,
      ramo: 'inelegivel',
      motivo:
        `Sem proposta para a tabela ${ctx.rotulo}: ${MOTIVO_CUSTO_NAO_CONFIAVEL} — ` +
        `${detalhes.join('; ')}. Item vai para exceção humana.`,
      tem_historico: false,
      historico_ausente_motivo: null,
      preco_anterior: precoAnterior,
      preco_anterior_origem: precoAnteriorOrigem,
      markup_anterior_pct: null,
      preco_proposto: null,
      markup_proposto_pct: null,
      variacao_preco: null,
      markup_anterior_destino: null,
      similares_considerados: null,
      markup_herdado_pct: null,
      faixa_aplicada: null,
      posicao_na_faixa: null,
      fracao_na_faixa: null,
      avaliacao_piso: null,
      piso_respeitado: true,
      acima_do_teto: false,
      precisa_confirmacao_mercado: true,
      marcacoes: [MOTIVO_CUSTO_NAO_CONFIAVEL],
      avisos: [],
    };
  }

  /**
   * Herança do markup dos similares do grupo (US-042 / T-029) — **antes** do fallback pela
   * faixa. O critério e a alternativa descartada estão em
   * `dto/proposta-preco.dto.ts` (`CRITERIO_HERANCA_GRUPO`): **mediana simples** dos markups
   * praticados pelos similares, **limitada ao corredor** da faixa de custo do item.
   *
   * Os campos "anterior" continuam `null` — o item não tem histórico próprio, e herdar markup
   * não inventa preço anterior. O que o extrato ganha é `similares_considerados` (de quantos) e
   * `markup_herdado_pct` (a mediana antes da limitação).
   */
  private async propostaHerdadaDoGrupo(
    ctx: {
      tabela: TabelaPreco;
      rotulo: string;
      custoAtual: number;
      sinal: SinalPerformanceDto;
    },
    corredor: CorredorMarkupDto,
    precoAnterior: number | null,
    precoAnteriorOrigem: PropostaTabelaDto['preco_anterior_origem'],
    markups: number[],
  ): Promise<PropostaTabelaDto> {
    const mediana = this.mediana(markups);
    const bruto = ctx.custoAtual * (1 + mediana / 100);
    const teto = Math.max(corredor.preco_maximo, corredor.preco_minimo);
    const candidato = Math.min(Math.max(this.arred2Cima(bruto), corredor.preco_minimo), teto);

    const avisos = ctx.sinal.dado_desatualizado
      ? [`Sinal de performance com dado desatualizado (frescor: ${ctx.sinal.frescor}).`]
      : [];
    if (candidato > this.arred2Cima(bruto)) {
      avisos.push(
        `Markup herdado ${this.pct(mediana)} abaixo do piso do corredor ` +
          `${this.pct(corredor.markup_min_pct)}: preço elevado ao limite inferior da faixa.`,
      );
    } else if (candidato < this.arred2Cima(bruto)) {
      avisos.push(
        `Markup herdado ${this.pct(mediana)} acima do teto do corredor ` +
          `${this.pct(corredor.markup_max_pct)}: preço cedido ao limite superior da faixa.`,
      );
    }

    const plural = markups.length === 1 ? 'similar' : 'similares';
    const motivo =
      `Tabela ${ctx.rotulo} sem preço anterior: markup herdado de ${markups.length} ` +
      `${plural} do grupo (mediana ${this.pct(this.arred4(mediana))}), limitado ao corredor ` +
      `${this.pct(corredor.markup_min_pct)}–${this.pct(corredor.markup_max_pct)} da faixa ` +
      `${this.faixaTexto(corredor)} — ${this.brl(candidato)}. ${MARCACAO_CONFIRMACAO_MERCADO}.`;

    return this.fechar({
      tabela: ctx.tabela,
      rotulo: ctx.rotulo,
      ramo: 'sem_historico_herdado_do_grupo',
      motivo,
      temHistorico: false,
      historicoAusenteMotivo: this.historicoAusente(precoAnterior),
      precoAnterior,
      precoAnteriorOrigem,
      markupAnteriorPct: null,
      destino: null,
      corredor,
      posicao: null,
      fracao: null,
      candidato,
      custoAtual: ctx.custoAtual,
      marcacoes: [MARCACAO_CONFIRMACAO_MERCADO],
      precisaConfirmacao: true,
      similaresConsiderados: markups.length,
      markupHerdadoPct: mediana,
      avisos,
    });
  }

  /**
   * Fallback pela faixa (PRD#escopo, regra 3): sem preço anterior, o corredor dá os limites e
   * o sinal de performance posiciona dentro dele. **A proposta nunca sai vazia.**
   *
   * Os campos "anterior" saem `null` — explicitamente ausentes, nunca zerados — e o extrato
   * recebe no lugar `faixa_aplicada` + `posicao_na_faixa` + o `sinal_performance` do item.
   */
  private async propostaPelaFaixa(
    ctx: {
      tabela: TabelaPreco;
      rotulo: string;
      custoAtual: number;
      custoAnterior: number | null;
      sinal: SinalPerformanceDto;
    },
    corredor: CorredorMarkupDto,
    precoAnterior: number | null,
    precoAnteriorOrigem: PropostaTabelaDto['preco_anterior_origem'],
    linha: { origem: string } | null,
  ): Promise<PropostaTabelaDto> {
    const posicao = ctx.sinal.posicao;
    const fracao = FRACAO_POSICAO[posicao];
    const candidato = this.precoNaFaixa(corredor, fracao);

    const ausencia = this.historicoAusente(precoAnterior);
    const semAncora =
      ausencia == null
        ? 'sem custo anterior no estoque vivo'
        : ausencia === 'preco_anterior_zerado'
          ? 'preço anterior zerado no cadastro'
          : linha?.origem === 'ausente'
            ? 'sem preço anterior cadastrado'
            : 'preço anterior indisponível na leitura';

    const motivo =
      `Tabela ${ctx.rotulo} ${semAncora}: proposta derivada da faixa de custo ` +
      `${this.faixaTexto(corredor)} (corredor ${this.pct(corredor.markup_min_pct)}–` +
      `${this.pct(corredor.markup_max_pct)}), posicionada no ${this.posicaoTexto(posicao)} do ` +
      `corredor pelo sinal de performance (${ctx.sinal.detalhe}) — ` +
      `${this.brl(candidato)}. ${MARCACAO_CONFIRMACAO_MERCADO}.`;

    return this.fechar({
      tabela: ctx.tabela,
      rotulo: ctx.rotulo,
      ramo: 'sem_historico_pela_faixa',
      motivo,
      temHistorico: false,
      historicoAusenteMotivo: ausencia,
      precoAnterior,
      precoAnteriorOrigem,
      markupAnteriorPct: null,
      destino: null,
      corredor,
      posicao,
      fracao,
      candidato,
      custoAtual: ctx.custoAtual,
      marcacoes: [MARCACAO_CONFIRMACAO_MERCADO],
      precisaConfirmacao: true,
      avisos: ctx.sinal.dado_desatualizado
        ? [`Sinal de performance com dado desatualizado (frescor: ${ctx.sinal.frescor}).`]
        : [],
    });
  }

  /**
   * Proposta ancorada no markup anterior — os quatro ramos dos ACs 1–4 da US-041, mais as duas
   * folhas que a árvore exige e os ACs não nomeiam (`custo_estavel`,
   * `custo_caiu_sem_estoque_sinal_ausente`).
   */
  private async propostaAncorada(
    ctx: {
      tabela: TabelaPreco;
      rotulo: string;
      custoAtual: number;
      direcao: DirecaoCusto;
      comEstoque: boolean;
      camadaFifoAntigaViva: boolean;
      estoqueDisponivel: number | null;
      sinal: SinalPerformanceDto;
    },
    corredor: CorredorMarkupDto,
    precoAnterior: number,
    precoAnteriorOrigem: PropostaTabelaDto['preco_anterior_origem'],
    custoAnterior: number,
  ): Promise<PropostaTabelaDto> {
    const markupAnterior = this.markupPct(precoAnterior, custoAnterior);
    const dentroDoCorredor =
      markupAnterior >= corredor.markup_min_pct && markupAnterior <= corredor.markup_max_pct;
    const custoTexto = `custo ${ctx.direcao} de ${this.brl(custoAnterior)} para ${this.brl(
      ctx.custoAtual,
    )}`;

    let ramo: RamoProposta;
    let candidato: number;
    let destino: DestinoMarkupAnterior = 'mantido';
    let posicao: PosicaoPerformance | null = null;
    let fracao: number | null = null;
    let motivo: string;
    const avisos: string[] = [];

    if (ctx.direcao === 'caiu' && ctx.comEstoque) {
      // AC1 — custo caiu, há estoque: a camada FIFO antiga viva mantém custo e preço anteriores.
      ramo = 'custo_caiu_com_estoque';
      candidato = precoAnterior;
      motivo =
        `Tabela ${ctx.rotulo}: ${custoTexto}, há ${this.qtd(ctx.estoqueDisponivel)} em estoque ` +
        `(camada FIFO antiga viva) e o markup anterior de ${this.pct(markupAnterior)} ` +
        `${dentroDoCorredor ? 'está dentro' : 'está fora'} do corredor ` +
        `${this.pct(corredor.markup_min_pct)}–${this.pct(corredor.markup_max_pct)} da faixa ` +
        `${this.faixaTexto(corredor)}: preço mantido em ${this.brl(precoAnterior)}.`;
    } else if (ctx.direcao === 'caiu' && ctx.sinal.posicao === 'alta') {
      // AC2 — custo caiu, estoque zerado, giro real ≥ esperado: performance sustenta o preço.
      ramo = 'custo_caiu_sem_estoque_performance_boa';
      candidato = precoAnterior;
      motivo =
        `Tabela ${ctx.rotulo}: ${custoTexto} e estoque zerado, mas a performance sustenta o ` +
        `preço — ${ctx.sinal.detalhe}: preço mantido em ${this.brl(precoAnterior)} ` +
        `(markup anterior ${this.pct(markupAnterior)}).`;
    } else if (ctx.direcao === 'caiu' && ctx.sinal.posicao === 'baixa') {
      // AC3 — custo caiu, estoque zerado, performance ruim: reduz, sem furar o piso.
      ramo = 'custo_caiu_sem_estoque_performance_ruim';
      posicao = 'baixa';
      fracao = FRACAO_POSICAO.baixa;
      const alvo = this.precoNaFaixa(corredor, fracao);
      candidato = alvo;
      if (alvo < precoAnterior) {
        motivo =
          `Tabela ${ctx.rotulo}: ${custoTexto}, estoque zerado e performance ruim — ` +
          `${ctx.sinal.detalhe}: preço reduzido de ${this.brl(precoAnterior)} para ` +
          `${this.brl(alvo)}, no piso do corredor ${this.pct(corredor.markup_min_pct)} da ` +
          `faixa ${this.faixaTexto(corredor)}.`;
      } else {
        destino = 'elevado_ao_piso';
        avisos.push(
          `Piso da faixa impede a redução: o preço anterior de ${this.brl(precoAnterior)} já ` +
            `está no ou abaixo do mínimo de ${this.brl(corredor.preco_minimo)}.`,
        );
        motivo =
          `Tabela ${ctx.rotulo}: ${custoTexto}, estoque zerado e performance ruim ` +
          `(${ctx.sinal.detalhe}), mas o piso da faixa ${this.faixaTexto(corredor)} impede ` +
          `redução: preço fica no mínimo de ${this.brl(alvo)} ` +
          `(markup ${this.pct(corredor.markup_min_pct)}).`;
      }
    } else if (ctx.direcao === 'caiu') {
      // Folha declarada: custo caiu, estoque zerado e NENHUM sinal (nem giro, nem idade
      // julgável). "Não diminui preço sem motivo" ⇒ mantém e marca para confirmação.
      ramo = 'custo_caiu_sem_estoque_sinal_ausente';
      candidato = precoAnterior;
      motivo =
        `Tabela ${ctx.rotulo}: ${custoTexto} e estoque zerado, sem sinal de performance ` +
        `(${ctx.sinal.detalhe}): preço mantido em ${this.brl(precoAnterior)} — não se reduz ` +
        `preço sem motivo. ${MARCACAO_CONFIRMACAO_MERCADO}.`;
    } else if (ctx.direcao === 'subiu') {
      // AC4 — custo subiu: preserva o markup anterior; se o teto não couber, cede o markup.
      ramo = 'custo_subiu';
      const preservandoMarkup = this.arred2Cima(ctx.custoAtual * (1 + markupAnterior / 100));
      if (preservandoMarkup > corredor.preco_maximo && corredor.preco_maximo >= precoAnterior) {
        destino = 'cedido_ao_teto';
        candidato = corredor.preco_maximo;
        motivo =
          `Tabela ${ctx.rotulo}: ${custoTexto}; manter o markup anterior de ` +
          `${this.pct(markupAnterior)} exigiria ${this.brl(preservandoMarkup)}, acima do teto ` +
          `${this.pct(corredor.markup_max_pct)} da faixa ${this.faixaTexto(corredor)} — ` +
          `markup CEDIDO ao teto: preço sobe de ${this.brl(precoAnterior)} para ` +
          `${this.brl(candidato)}.`;
      } else {
        candidato = Math.max(preservandoMarkup, precoAnterior);
        if (candidato > corredor.preco_maximo) {
          avisos.push(
            `Preço acima do teto da faixa, mantido por ser o preço anterior — a âncora vence ` +
              `o teto (teto é aviso, não recusa).`,
          );
        }
        motivo =
          `Tabela ${ctx.rotulo}: ${custoTexto} — markup anterior de ` +
          `${this.pct(markupAnterior)} MANTIDO: preço sobe de ${this.brl(precoAnterior)} para ` +
          `${this.brl(candidato)} na faixa ${this.faixaTexto(corredor)}.`;
      }
    } else {
      // Folha declarada: custo estável. A âncora do PRD proíbe mover preço sem motivo.
      ramo = 'custo_estavel';
      candidato = precoAnterior;
      motivo =
        `Tabela ${ctx.rotulo}: custo estável em ${this.brl(ctx.custoAtual)} — nenhum motivo ` +
        `para mover o preço: mantido em ${this.brl(precoAnterior)} ` +
        `(markup ${this.pct(markupAnterior)}).`;
    }

    // Piso inviolável tem precedência sobre a âncora: markup anterior abaixo do piso é elevado.
    if (candidato < corredor.preco_minimo) {
      destino = 'elevado_ao_piso';
      avisos.push(
        `Markup anterior de ${this.pct(markupAnterior)} abaixo do piso ` +
          `${this.pct(corredor.markup_min_pct)}: preço elevado de ${this.brl(candidato)} para ` +
          `o mínimo de ${this.brl(corredor.preco_minimo)}.`,
      );
      candidato = corredor.preco_minimo;
    }

    if (ctx.sinal.dado_desatualizado) {
      avisos.push(`Sinal de performance com dado desatualizado (frescor: ${ctx.sinal.frescor}).`);
    }

    return this.fechar({
      tabela: ctx.tabela,
      rotulo: ctx.rotulo,
      ramo,
      motivo,
      temHistorico: true,
      historicoAusenteMotivo: null,
      precoAnterior,
      precoAnteriorOrigem,
      markupAnteriorPct: markupAnterior,
      destino,
      corredor,
      posicao,
      fracao,
      candidato,
      custoAtual: ctx.custoAtual,
      marcacoes: ramo === 'custo_caiu_sem_estoque_sinal_ausente' ? [MARCACAO_CONFIRMACAO_MERCADO] : [],
      precisaConfirmacao:
        ramo === 'custo_caiu_sem_estoque_sinal_ausente' || ctx.sinal.dado_desatualizado,
      avisos,
    });
  }

  /**
   * Fecha a proposta pela **trava da T-024** — `avaliarPreco` é a única autoridade sobre o
   * piso. Recusa (o preço ficou abaixo do mínimo) corrige o preço para `preco_minimo` e
   * registra o ajuste; a comparação de piso **não** é reimplementada aqui.
   */
  private async fechar(p: {
    tabela: TabelaPreco;
    rotulo: string;
    ramo: RamoProposta;
    motivo: string;
    temHistorico: boolean;
    historicoAusenteMotivo: MotivoHistoricoAusente | null;
    precoAnterior: number | null;
    precoAnteriorOrigem: PropostaTabelaDto['preco_anterior_origem'];
    markupAnteriorPct: number | null;
    destino: DestinoMarkupAnterior | null;
    corredor: CorredorMarkupDto;
    posicao: PosicaoPerformance | null;
    fracao: number | null;
    candidato: number;
    custoAtual: number;
    marcacoes: string[];
    precisaConfirmacao: boolean;
    /** US-042: quantos similares sustentaram o markup herdado; ausente fora da herança. */
    similaresConsiderados?: number | null;
    /** US-042: a mediana herdada, ANTES da limitação ao corredor. */
    markupHerdadoPct?: number | null;
    avisos: string[];
  }): Promise<PropostaTabelaDto> {
    let preco = this.arred2(p.candidato);
    let avaliacao = await this.regua.avaliarPreco(p.tabela, p.custoAtual, preco);
    const avisos = [...p.avisos];
    let destino = p.destino;

    if (!avaliacao.aceito) {
      // A trava recusou: o piso vence. Corrige e registra — sem duplicar a comparação.
      avisos.push(
        `Preço ajustado ao piso pela trava da régua: ${avaliacao.motivo ?? 'abaixo do piso'}`,
      );
      destino = p.temHistorico ? 'elevado_ao_piso' : destino;
      preco = p.corredor.preco_minimo;
      avaliacao = await this.regua.avaliarPreco(p.tabela, p.custoAtual, preco);
      if (!avaliacao.aceito) {
        this.logger.error(
          `[proposta] piso não satisfeito nem no mínimo da faixa ${p.corredor.faixa_id} ` +
            `(tabela ${p.tabela}, custo ${p.custoAtual}) — proposta suprimida.`,
        );
      }
    }
    if (avaliacao.acima_do_teto && avaliacao.aviso) {
      avisos.push(avaliacao.aviso);
    }

    return {
      tabela: p.tabela,
      rotulo: p.rotulo,
      ramo: p.ramo,
      motivo: p.motivo,
      tem_historico: p.temHistorico,
      historico_ausente_motivo: p.historicoAusenteMotivo,
      preco_anterior: p.precoAnterior,
      preco_anterior_origem: p.precoAnteriorOrigem,
      markup_anterior_pct: p.markupAnteriorPct == null ? null : this.arred4(p.markupAnteriorPct),
      preco_proposto: preco,
      markup_proposto_pct: this.arred4(this.markupPct(preco, p.custoAtual)),
      variacao_preco: p.precoAnterior == null ? null : this.arred2(preco - p.precoAnterior),
      markup_anterior_destino: destino,
      similares_considerados: p.similaresConsiderados ?? null,
      markup_herdado_pct: p.markupHerdadoPct == null ? null : this.arred4(p.markupHerdadoPct),
      faixa_aplicada: p.corredor,
      posicao_na_faixa: p.posicao,
      fracao_na_faixa: p.fracao,
      avaliacao_piso: avaliacao,
      piso_respeitado: avaliacao.aceito,
      acima_do_teto: avaliacao.acima_do_teto,
      precisa_confirmacao_mercado: p.precisaConfirmacao,
      marcacoes: Array.from(new Set(p.marcacoes)),
      avisos,
    };
  }

  // ---------------------------------------------------------------------------
  // Sinal de performance
  // ---------------------------------------------------------------------------

  /**
   * Deriva a posição no corredor. Ordem de precedência:
   *
   * 1. **Par giro real × esperado** (US-039), quando ambos são positivos — é o proxy declarado
   *    de performance e o comparador literal dos ACs 2 e 3.
   * 2. **Idade do saldo** (T-025), quando não há giro: `classificacao_saldo = 'ruim'` (idade
   *    ≥ 365 dias) ⇒ posição baixa.
   * 3. **Ausente** ⇒ posição neutra (meio do corredor) — nunca `baixa`, porque baixa autoriza
   *    redução e não se corta preço na falta de sinal.
   */
  private sinalPerformance(
    sinalGiro: SinalGiroDto | null,
    elegibilidade: ElegibilidadeDto,
    giroReal: number | null,
    giroEsperado: number | null,
  ): SinalPerformanceDto {
    const temPar = giroReal != null && giroReal > 0 && giroEsperado != null && giroEsperado > 0;

    let posicao: PosicaoPerformance;
    let base: SinalPerformanceDto['base'];
    let detalhe: string;

    if (temPar) {
      base = 'giro';
      posicao = (giroReal as number) >= (giroEsperado as number) ? 'alta' : 'baixa';
      const comparador = posicao === 'alta' ? '≥' : '<';
      detalhe =
        `giro real ${this.num(giroReal)} ${comparador} esperado ${this.num(giroEsperado)} un/dia ` +
        `(escopo ${sinalGiro?.escopo ?? 'produto'})`;
    } else if (elegibilidade.classificacao_saldo === 'ruim') {
      base = 'idade_saldo';
      posicao = 'baixa';
      detalhe =
        `sem giro, saldo parado há ${this.num(elegibilidade.idade_saldo_dias)} dias ` +
        `(corte ${elegibilidade.corte_idade_saldo_dias} dias)`;
    } else {
      base = 'ausente';
      posicao = 'neutra';
      detalhe = elegibilidade.idade_saldo_anomala
        ? 'sinal ausente: idade de saldo anômala (negativa) — não julgável'
        : giroReal != null && giroReal > 0
          ? 'sinal ausente: giro esperado não materializado'
          : 'sinal ausente: sem giro medido e sem idade de saldo julgável';
    }

    return {
      posicao,
      fracao_faixa: FRACAO_POSICAO[posicao],
      base,
      giro_real: giroReal,
      giro_esperado: giroEsperado,
      razao_giro: temPar ? this.arred4((giroReal as number) / (giroEsperado as number)) : null,
      escopo_giro: sinalGiro?.escopo ?? 'produto',
      classificacao_saldo: elegibilidade.classificacao_saldo,
      frescor: sinalGiro?.frescor ?? 'sem-materializacao',
      dado_desatualizado: sinalGiro?.dado_desatualizado ?? true,
      detalhe,
    };
  }

  private giroDe(sinal: SinalGiroDto | null, metrica: string): number | null {
    return (sinal?.giro ?? []).find((m) => m.metrica === metrica)?.valor ?? null;
  }

  // ---------------------------------------------------------------------------
  // Cálculo e utilitários
  // ---------------------------------------------------------------------------

  /**
   * Preço na posição `fracao` do corredor. Interpola entre `preco_minimo` (já arredondado PARA
   * CIMA pela régua) e `preco_maximo` (PARA BAIXO) e arredonda PARA CIMA — assim o
   * arredondamento da T-024 é reaproveitado e nenhuma fração de centavo fura o piso. Quando
   * `markup_min = markup_max` (atacado 67%, atacado especial 50%) o arredondamento pode deixar
   * `preco_maximo` um centavo abaixo de `preco_minimo`: o piso vence.
   */
  private precoNaFaixa(corredor: CorredorMarkupDto, fracao: number): number {
    const teto = Math.max(corredor.preco_maximo, corredor.preco_minimo);
    const bruto = corredor.preco_minimo + fracao * (teto - corredor.preco_minimo);
    return Math.min(Math.max(this.arred2Cima(bruto), corredor.preco_minimo), teto);
  }

  /**
   * Markups praticados pelos **similares do grupo**, por item e por tabela (US-042).
   *
   * Só entra na lista o similar cujas duas pontas são positivas — preço vigente da tabela e
   * custo do estoque vivo. Similar sem preço ou sem custo é **ausência**, nunca markup zero.
   * O próprio item nunca é similar de si mesmo.
   */
  private async markupsDosSimilares(
    codigos: number[],
    grupos: Map<number, string>,
    estoque: Map<number, EstoqueFifoRow>,
    precos: Map<number, PrecoVigenteDto>,
  ): Promise<Map<number, MarkupsHerdados>> {
    const saida = new Map<number, MarkupsHerdados>();
    if (!grupos.size) return saida;

    const membrosPorGrupo = await this.repositorio.lerMembrosDoGrupo(
      Array.from(new Set(grupos.values())),
    );
    if (!membrosPorGrupo.size) return saida;

    const jaLidos = new Set(codigos);
    const extras = Array.from(
      new Set(
        Array.from(membrosPorGrupo.values())
          .flat()
          .filter((c) => !jaLidos.has(c)),
      ),
    );

    const [estoqueExtra, precosExtra] = extras.length
      ? await Promise.all([
          this.repositorio.lerEstoqueFifo(extras),
          this.repositorio.lerPrecos(extras),
        ])
      : [new Map<number, EstoqueFifoRow>(), new Map<number, PrecoVigenteDto>()];

    const custoDe = (cod: number): number | null =>
      (estoque.get(cod) ?? estoqueExtra.get(cod))?.custo_unitario ?? null;
    const precoDe = (cod: number, tabela: TabelaPreco): number | null =>
      ((precos.get(cod) ?? precosExtra.get(cod))?.precos ?? []).find((p) => p.tabela === tabela)
        ?.valor ?? null;

    for (const cod of codigos) {
      const grupo = grupos.get(cod);
      if (grupo == null) continue;
      const similares = (membrosPorGrupo.get(grupo) ?? []).filter((m) => m !== cod);
      if (!similares.length) continue;

      const porTabela: MarkupsHerdados = new Map();
      for (const meta of TABELAS_PRECO) {
        const markups: number[] = [];
        for (const similar of similares) {
          const custo = custoDe(similar);
          const preco = precoDe(similar, meta.tabela);
          if (custo == null || !(custo > 0) || preco == null || !(preco > 0)) continue;
          markups.push(this.markupPct(preco, custo));
        }
        if (markups.length) porTabela.set(meta.tabela, markups);
      }
      if (porTabela.size) saida.set(cod, porTabela);
    }

    return saida;
  }

  /** Mediana simples — o critério decidido na T-029 (ver `CRITERIO_HERANCA_GRUPO`). */
  private mediana(valores: number[]): number {
    const ordenados = [...valores].sort((a, b) => a - b);
    const meio = Math.floor(ordenados.length / 2);
    return ordenados.length % 2 === 1
      ? ordenados[meio]
      : (ordenados[meio - 1] + ordenados[meio]) / 2;
  }

  private direcaoCusto(
    custoAtual: number,
    custoAnterior: number | null,
  ): { direcao: DirecaoCusto; variacao: number | null } {
    if (custoAnterior == null) return { direcao: 'indeterminado', variacao: null };
    const delta = custoAtual - custoAnterior;
    if (Math.abs(delta) < TOLERANCIA_CUSTO_REAIS) {
      return { direcao: 'estavel', variacao: this.arred4(delta) };
    }
    return { direcao: delta < 0 ? 'caiu' : 'subiu', variacao: this.arred4(delta) };
  }

  /** `null` quando há histórico utilizável; o motivo nomeado quando não há. */
  private historicoAusente(precoAnterior: number | null): MotivoHistoricoAusente | null {
    if (precoAnterior == null) return 'preco_anterior_ausente';
    if (precoAnterior <= 0) return 'preco_anterior_zerado';
    return null;
  }

  private markupPct(preco: number, custo: number): number {
    if (!(custo > 0)) return 0;
    return (preco / custo - 1) * 100;
  }

  private arred2(v: number): number {
    return Number(v.toFixed(2));
  }

  private arred2Cima(v: number): number {
    return Math.ceil(Number((v * 100).toFixed(6))) / 100;
  }

  private arred4(v: number): number {
    return Number(v.toFixed(4));
  }

  private brl(v: number | null): string {
    if (v == null) return 'ausente';
    return `R$ ${v.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }

  private pct(v: number | null): string {
    if (v == null) return 'ausente';
    return `${v.toLocaleString('pt-BR', { maximumFractionDigits: 4 })}%`;
  }

  private num(v: number | null): string {
    if (v == null) return 'ausente';
    return v.toLocaleString('pt-BR', { maximumFractionDigits: 4 });
  }

  private qtd(v: number | null): string {
    return v == null ? 'saldo ausente' : `${this.num(v)} un`;
  }

  private faixaTexto(c: CorredorMarkupDto): string {
    const max = c.faixa_custo_max == null ? 'sem limite' : this.brl(c.faixa_custo_max);
    return `${this.brl(c.faixa_custo_min)}–${max}`;
  }

  private posicaoTexto(p: PosicaoPerformance): string {
    return p === 'alta' ? 'teto' : p === 'baixa' ? 'piso' : 'meio';
  }

  private totalizar(itens: PropostaItemDto[]): TotaisPropostaDto {
    const elegiveis = itens.filter((i) => i.elegivel);
    const todas = itens.flatMap((i) => i.propostas);
    const comPreco = todas.filter((p) => p.preco_proposto != null);
    return {
      itens: itens.length,
      elegiveis: elegiveis.length,
      inelegiveis: itens.length - elegiveis.length,
      propostas: comPreco.length,
      elegiveis_sem_proposta: elegiveis.filter((i) =>
        i.propostas.some((p) => p.preco_proposto == null),
      ).length,
      sem_historico: todas.filter((p) => p.ramo === 'sem_historico_pela_faixa').length,
      sem_historico_herdado: todas.filter((p) => p.ramo === 'sem_historico_herdado_do_grupo')
        .length,
      marcadas_confirmacao_mercado: todas.filter((p) => p.precisa_confirmacao_mercado).length,
      piso_furado: comPreco.filter((p) => !p.piso_respeitado).length,
      acima_do_teto: comPreco.filter((p) => p.acima_do_teto).length,
    };
  }

}
