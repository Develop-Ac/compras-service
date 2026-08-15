import {
  BadRequestException,
  Injectable,
  Logger,
  ServiceUnavailableException,
} from '@nestjs/common';
import {
  AlterarFaixaDto,
  AvaliacaoPrecoDto,
  CorredorMarkupDto,
  FaixaMarkupDto,
  MARKUP_PCT_MAXIMO,
} from './dto/faixa-markup.dto';
import { TABELAS_PRECO, TabelaPreco } from './dto/preco-vigente.dto';
import { FaixaReguaRow, NovaFaixaRegua, ReguaRepository } from './regua.repository';

/**
 * Janela do cache da régua. A régua é **dado**, não código: mudar uma faixa não pode
 * exigir rebuild nem restart (AC). O cache existe só para não ir ao banco a cada item
 * de um lote de precificação, e é **invalidado na escrita** — alteração feita por esta
 * aplicação vale na avaliação seguinte, sem esperar o TTL. O TTL cobre a alteração
 * feita FORA daqui (UPDATE manual no banco), que passa a valer em ≤ 60 s.
 */
const CACHE_TTL_MS = 60_000;

/** Tolerância em centavos para absorver ruído binário (1e-6 centavo = 1e-8 real). */
const EPSILON_CENTAVOS = 1e-6;

/**
 * Régua de margem das três tabelas de preço, com **piso inviolável** (US-040 / T-024).
 *
 * Duas responsabilidades:
 * 1. **Corredor:** dado (tabela, custo), qual o markup mínimo e máximo — lido de
 *    `com_precificacao_faixa`, nunca de constante em código.
 * 2. **Trava de piso:** dado (tabela, custo, preço), aceitar ou **recusar com motivo
 *    explícito**. Recusa dura acontece SOMENTE abaixo do piso; passar do teto devolve
 *    aviso e `aceito: true`, porque a story declara inviolável apenas o limite
 *    inferior — o teto governa a *proposta* (US-041), não a aceitação de um preço
 *    informado.
 *
 * Semântica de markup: **100% = preço 2× custo** (`preco = custo × (1 + pct/100)`),
 * nunca preço = custo.
 *
 * **Arredondamento com direção declarada:** o preço mínimo arredonda para CIMA no
 * centavo e o máximo para BAIXO. Arredondar o piso para baixo aceitaria um preço
 * abaixo do piso real por fração de centavo — e o piso é inviolável, não
 * aproximadamente inviolável.
 *
 * **A não-monotonicidade entre as duas últimas faixas do varejo é intencional**
 * (PRD#escopo). Nada aqui compara faixas vizinhas para "corrigir" a régua; a
 * validação de consistência olha apenas cobertura e sobreposição.
 */
@Injectable()
export class ReguaService {
  private readonly logger = new Logger(ReguaService.name);

  private cache: { faixas: FaixaReguaRow[]; carregadoEm: number } | null = null;

  constructor(private readonly repo: ReguaRepository) {}

  // ---------------------------------------------------------------------------
  // Leitura
  // ---------------------------------------------------------------------------

  /** Faixas vigentes das três tabelas, na ordem (tabela, custo). */
  async faixasVigentes(): Promise<FaixaMarkupDto[]> {
    const faixas = await this.carregar();
    return faixas.map((f) => this.paraDto(f));
  }

  /** Trilha de auditoria — vigentes e encerradas, com quem alterou e quando. */
  async historico(tabela?: string): Promise<FaixaMarkupDto[]> {
    const alvo = tabela ? this.normalizarTabela(tabela) : undefined;
    const linhas = await this.repo.historico(alvo);
    return linhas.map((f) => this.paraDto(f));
  }

  /** Corredor de markup aplicável a um custo numa tabela (AC1). */
  async corredor(tabela: string, custo: number): Promise<CorredorMarkupDto> {
    const alvo = this.normalizarTabela(tabela);
    const valor = this.normalizarCusto(custo);
    const faixas = await this.carregar();
    const faixa = this.faixaDe(faixas, alvo, valor);

    return {
      tabela: alvo,
      rotulo: this.rotulo(alvo),
      custo: valor,
      faixa_id: faixa.id,
      faixa_custo_min: faixa.custo_min,
      faixa_custo_max: faixa.custo_max,
      markup_min_pct: faixa.markup_min_pct,
      markup_max_pct: faixa.markup_max_pct,
      preco_minimo: this.precoMinimo(valor, faixa.markup_min_pct),
      preco_maximo: this.precoMaximo(valor, faixa.markup_max_pct),
      lido_em: new Date().toISOString(),
    };
  }

  /**
   * Trava de piso: recusa, com motivo explícito, qualquer preço abaixo do limite
   * inferior da faixa — nas três tabelas (AC2 / DoD).
   */
  async avaliarPreco(tabela: string, custo: number, preco: number): Promise<AvaliacaoPrecoDto> {
    const alvo = this.normalizarTabela(tabela);
    const valorCusto = this.normalizarCusto(custo);
    const valorPreco = this.numeroFinito(preco, 'preco');
    if (valorPreco < 0) {
      throw new BadRequestException(`preco não pode ser negativo (recebido ${preco}).`);
    }

    const faixas = await this.carregar();
    const faixa = this.faixaDe(faixas, alvo, valorCusto);

    const precoMinimo = this.precoMinimo(valorCusto, faixa.markup_min_pct);
    const precoMaximo = this.precoMaximo(valorCusto, faixa.markup_max_pct);
    const markupPct = this.arredondar((valorPreco / valorCusto - 1) * 100, 4);

    // Comparação em CENTAVOS inteiros: comparar reais em ponto flutuante deixaria a
    // aceitação do preço exatamente no piso à mercê de erro de representação.
    const precoCent = Math.round(valorPreco * 100);
    const minimoCent = Math.round(precoMinimo * 100);
    const maximoCent = Math.round(precoMaximo * 100);

    const aceito = precoCent >= minimoCent;
    const acimaDoTeto = precoCent > maximoCent;

    const faixaTexto =
      `faixa de custo ${this.moeda(faixa.custo_min)} a ` +
      (faixa.custo_max == null ? 'sem limite' : `${this.moeda(faixa.custo_max)} (exclusivo)`);

    return {
      aceito,
      tabela: alvo,
      custo: valorCusto,
      preco: valorPreco,
      markup_pct: markupPct,
      piso_markup_pct: faixa.markup_min_pct,
      preco_minimo: precoMinimo,
      preco_maximo: precoMaximo,
      motivo: aceito
        ? null
        : `Preço de ${this.moeda(valorPreco)} abaixo do piso da tabela ${this.rotulo(alvo)}: ` +
          `markup de ${this.pct(markupPct)} menor que o mínimo de ${this.pct(faixa.markup_min_pct)} ` +
          `— o menor preço aceito é ${this.moeda(precoMinimo)} para custo de ` +
          `${this.moeda(valorCusto)} (${faixaTexto}). Operação recusada: o piso é inviolável.`,
      acima_do_teto: acimaDoTeto,
      aviso: acimaDoTeto
        ? `Preço de ${this.moeda(valorPreco)} acima do teto da tabela ${this.rotulo(alvo)} ` +
          `(${this.pct(faixa.markup_max_pct)} — máximo ${this.moeda(precoMaximo)}). ` +
          `Não recusa: apenas o limite inferior é inviolável.`
        : null,
      faixa_id: faixa.id,
    };
  }

  // ---------------------------------------------------------------------------
  // Escrita versionada
  // ---------------------------------------------------------------------------

  /**
   * Substitui (ou cria) faixas, registrando **quem** alterou e **quando**.
   *
   * Aceita um LOTE porque dividir uma faixa em duas mexe em duas linhas ao mesmo
   * tempo: aplicar só uma deixaria a régua com buraco de cobertura, que o AC3 proíbe.
   * O conjunto resultante é validado ANTES de gravar — a régua nunca fica
   * inconsistente no banco.
   */
  async alterarFaixas(entradas: AlterarFaixaDto[], autorHeader?: string): Promise<FaixaMarkupDto[]> {
    if (!Array.isArray(entradas) || !entradas.length) {
      throw new BadRequestException('Informe ao menos uma faixa para alterar.');
    }

    const agora = new Date();
    const novas: NovaFaixaRegua[] = entradas.map((e) => this.validarEntrada(e, autorHeader));

    // Conjunto prospectivo: as vigentes, com as alteradas trocadas no lugar.
    const vigentes = await this.carregar();
    const prospectivo: FaixaReguaRow[] = vigentes
      .filter((v) => !novas.some((n) => n.tabela === v.tabela && n.custo_min === v.custo_min))
      .concat(
        novas.map((n, i) => ({
          id: -(i + 1),
          tabela: n.tabela,
          custo_min: n.custo_min,
          custo_max: n.custo_max,
          markup_min_pct: n.markup_min_pct,
          markup_max_pct: n.markup_max_pct,
          vigencia_inicio: agora,
          vigencia_fim: null,
          criado_por: n.autor,
          criado_em: agora,
          encerrado_por: null,
          encerrado_em: null,
          motivo: n.motivo,
        })),
      );

    for (const tabela of new Set(novas.map((n) => n.tabela))) {
      const defeito = this.defeitoDeCobertura(prospectivo, tabela);
      if (defeito) {
        throw new BadRequestException(
          `Alteração recusada: deixaria a régua da tabela ${this.rotulo(tabela)} inconsistente ` +
            `— ${defeito}. Envie no MESMO lote as faixas vizinhas afetadas.`,
        );
      }
    }

    const criadas = await this.repo.substituirFaixas(novas, agora);
    this.invalidarCache();
    return criadas.map((f) => this.paraDto(f));
  }

  /** Descarta o cache — a próxima avaliação vai ao banco. */
  invalidarCache(): void {
    this.cache = null;
  }

  // ---------------------------------------------------------------------------
  // Carga e consistência
  // ---------------------------------------------------------------------------

  private async carregar(): Promise<FaixaReguaRow[]> {
    const agora = Date.now();
    if (this.cache && agora - this.cache.carregadoEm < CACHE_TTL_MS) {
      return this.cache.faixas;
    }

    const faixas = await this.repo.faixasVigentes();
    if (!faixas.length) {
      // Falhar alto é obrigatório: cair para uma régua embutida em código seria
      // exatamente o que o AC proíbe ("nunca constante em código").
      throw new ServiceUnavailableException(
        'Régua de margem não configurada: nenhuma faixa vigente em com_precificacao_faixa. ' +
          'Aplique compras-service/sql/2026-07-30_precificacao_faixas.sql (DDL manual) e rode ' +
          'npx prisma generate.',
      );
    }

    for (const meta of TABELAS_PRECO) {
      const defeito = this.defeitoDeCobertura(faixas, meta.tabela);
      if (defeito) {
        // Servir preço com régua furada é pior que não servir: um custo sem faixa não
        // tem piso, e sem piso não existe recusa.
        this.logger.error(`[regua] cobertura inválida em ${meta.tabela}: ${defeito}`);
        throw new ServiceUnavailableException(
          `Régua de margem inconsistente na tabela ${meta.rotulo}: ${defeito}. ` +
            'Corrija com_precificacao_faixa antes de precificar.',
        );
      }
    }

    this.cache = { faixas, carregadoEm: agora };
    return faixas;
  }

  /**
   * Cobertura total e sem sobreposição de uma tabela: começa em 0, cada faixa emenda
   * na seguinte (`custo_max` = `custo_min` da próxima) e a última é aberta.
   *
   * Não há aqui NENHUMA verificação de monotonicidade de markup entre faixas — a
   * queda de 150% para 110% e a volta a 150% na última faixa do varejo são
   * declaração do responsável (PRD#escopo).
   */
  private defeitoDeCobertura(faixas: FaixaReguaRow[], tabela: TabelaPreco): string | null {
    const daTabela = faixas
      .filter((f) => f.tabela === tabela)
      .sort((a, b) => a.custo_min - b.custo_min);

    if (!daTabela.length) return 'nenhuma faixa vigente';
    if (daTabela[0].custo_min !== 0) {
      return `a primeira faixa começa em ${this.moeda(daTabela[0].custo_min)}, não em R$ 0,00 ` +
        '(custo abaixo disso ficaria sem faixa)';
    }

    for (let i = 0; i < daTabela.length - 1; i++) {
      const atual = daTabela[i];
      const proxima = daTabela[i + 1];
      if (atual.custo_max == null) {
        return `a faixa que começa em ${this.moeda(atual.custo_min)} é aberta mas não é a última`;
      }
      if (atual.custo_max !== proxima.custo_min) {
        return atual.custo_max < proxima.custo_min
          ? `buraco entre ${this.moeda(atual.custo_max)} e ${this.moeda(proxima.custo_min)}`
          : `sobreposição entre ${this.moeda(proxima.custo_min)} e ${this.moeda(atual.custo_max)}`;
      }
    }

    const ultima = daTabela[daTabela.length - 1];
    if (ultima.custo_max != null) {
      return `a última faixa termina em ${this.moeda(ultima.custo_max)} — custo acima disso ` +
        'ficaria sem faixa (a última precisa ter custo_max nulo)';
    }
    return null;
  }

  private faixaDe(faixas: FaixaReguaRow[], tabela: TabelaPreco, custo: number): FaixaReguaRow {
    // Intervalo MEIO-ABERTO [custo_min, custo_max): é o que garante que 400,00 caia na
    // faixa 99,01–400,00 e 400,01 na de "acima de 400,00", sem buraco entre elas.
    const faixa = faixas.find(
      (f) => f.tabela === tabela && custo >= f.custo_min && (f.custo_max == null || custo < f.custo_max),
    );
    if (!faixa) {
      // Inalcançável enquanto defeitoDeCobertura() estiver verde — mas se chegar aqui,
      // é falha de configuração, nunca "sem piso".
      throw new ServiceUnavailableException(
        `Nenhuma faixa da tabela ${this.rotulo(tabela)} cobre o custo de ${this.moeda(custo)}.`,
      );
    }
    return faixa;
  }

  // ---------------------------------------------------------------------------
  // Validação de entrada
  // ---------------------------------------------------------------------------

  private validarEntrada(e: AlterarFaixaDto, autorHeader?: string): NovaFaixaRegua {
    const tabela = this.normalizarTabela(e?.tabela as string);
    const autor = String(e?.alterado_por ?? autorHeader ?? '').trim();
    if (!autor) {
      throw new BadRequestException(
        'Informe quem está alterando a régua (campo alterado_por ou header x-user-id): ' +
          'faixa alterada sem autor não é auditável.',
      );
    }

    const custoMin = this.numeroFinito(e?.custo_min, 'custo_min');
    if (custoMin < 0) throw new BadRequestException('custo_min não pode ser negativo.');

    const custoMax =
      e?.custo_max == null ? null : this.numeroFinito(e.custo_max, 'custo_max');
    if (custoMax != null && custoMax <= custoMin) {
      throw new BadRequestException(
        `custo_max (${custoMax}) precisa ser maior que custo_min (${custoMin}) — o intervalo é ` +
          '[custo_min, custo_max).',
      );
    }

    const min = this.numeroFinito(e?.markup_min_pct, 'markup_min_pct');
    const max = this.numeroFinito(e?.markup_max_pct, 'markup_max_pct');
    if (min < 0) throw new BadRequestException('markup_min_pct não pode ser negativo.');
    if (max < min) {
      throw new BadRequestException(
        `markup_max_pct (${max}) não pode ser menor que markup_min_pct (${min}) dentro da mesma faixa.`,
      );
    }
    if (max > MARKUP_PCT_MAXIMO) {
      // Achado da T-023: margem_pct é Decimal(8,4). Deixar entrar 10000% gravaria
      // overflow no banco no INSERT — melhor recusar com o motivo na cara.
      throw new BadRequestException(
        `markup_max_pct (${max}) acima do teto de ${MARKUP_PCT_MAXIMO}% imposto pela precisão ` +
          'Decimal(8,4) das colunas de markup (mesma de com_fifo_completo.margem_pct).',
      );
    }

    return {
      tabela,
      custo_min: this.arredondar(custoMin, 2),
      custo_max: custoMax == null ? null : this.arredondar(custoMax, 2),
      markup_min_pct: this.arredondar(min, 4),
      markup_max_pct: this.arredondar(max, 4),
      autor,
      motivo: e?.motivo ? String(e.motivo).trim() : null,
    };
  }

  private normalizarTabela(v: string): TabelaPreco {
    const alvo = String(v ?? '').trim().toLowerCase();
    const meta = TABELAS_PRECO.find((t) => t.tabela === alvo);
    if (!meta) {
      throw new BadRequestException(
        `Tabela de preço inválida: "${v}". Válidas: ${TABELAS_PRECO.map((t) => t.tabela).join(', ')}.`,
      );
    }
    return meta.tabela;
  }

  /**
   * Custo tem de ser > 0: com custo zero não existe markup (divisão por zero) e um
   * piso de 0 aceitaria qualquer preço — o oposto de piso inviolável.
   */
  private normalizarCusto(v: number): number {
    const n = this.numeroFinito(v, 'custo');
    if (n <= 0) {
      throw new BadRequestException(
        `custo precisa ser maior que zero para aplicar a régua (recebido ${v}).`,
      );
    }
    return n;
  }

  private numeroFinito(v: unknown, campo: string): number {
    const n = typeof v === 'number' ? v : Number(String(v ?? '').replace(',', '.').trim());
    if (!Number.isFinite(n)) {
      throw new BadRequestException(`${campo} inválido: "${String(v)}".`);
    }
    return n;
  }

  // ---------------------------------------------------------------------------
  // Cálculo e formatação
  // ---------------------------------------------------------------------------

  /** Piso em R$, arredondado PARA CIMA no centavo (nunca abaixo do piso real). */
  private precoMinimo(custo: number, markupMinPct: number): number {
    const cent = custo * (1 + markupMinPct / 100) * 100;
    return Math.ceil(cent - EPSILON_CENTAVOS) / 100;
  }

  /** Teto em R$, arredondado PARA BAIXO no centavo (nunca acima do teto real). */
  private precoMaximo(custo: number, markupMaxPct: number): number {
    const cent = custo * (1 + markupMaxPct / 100) * 100;
    return Math.floor(cent + EPSILON_CENTAVOS) / 100;
  }

  private arredondar(v: number, casas: number): number {
    const f = 10 ** casas;
    return Math.round(v * f) / f;
  }

  private moeda(v: number): string {
    return `R$ ${v.toFixed(2).replace('.', ',')}`;
  }

  private pct(v: number): string {
    const texto = Number.isInteger(v) ? String(v) : v.toFixed(4).replace(/0+$/, '');
    return `${texto.replace('.', ',')}%`;
  }

  private rotulo(tabela: TabelaPreco): string {
    return TABELAS_PRECO.find((t) => t.tabela === tabela)?.rotulo ?? tabela;
  }

  private paraDto(f: FaixaReguaRow): FaixaMarkupDto {
    return {
      id: f.id,
      tabela: f.tabela,
      rotulo: this.rotulo(f.tabela),
      custo_min: f.custo_min,
      custo_max: f.custo_max,
      markup_min_pct: f.markup_min_pct,
      markup_max_pct: f.markup_max_pct,
      vigencia_inicio: f.vigencia_inicio?.toISOString() ?? '',
      vigencia_fim: f.vigencia_fim ? f.vigencia_fim.toISOString() : null,
      criado_por: f.criado_por,
      criado_em: f.criado_em?.toISOString() ?? '',
      encerrado_por: f.encerrado_por,
      encerrado_em: f.encerrado_em ? f.encerrado_em.toISOString() : null,
      motivo: f.motivo,
    };
  }
}
