import { Injectable } from '@nestjs/common';
import { CustoService } from './custo.service';
import type {
  CustoComposicaoDto,
  CustoItemDto,
  ParcelaCustoDto,
} from './dto/custo-item.dto';
import type { TabelaPreco } from './dto/preco-vigente.dto';
import type { PropostaItemDto, PropostaNotaDto } from './dto/proposta-preco.dto';
import {
  ChaveMotivoExcecao,
  ContagemMotivoDto,
  ExcecaoTriadaDto,
  ItemTriadoDto,
  META_TAXA_EXCECAO_PCT,
  MOTIVO_EXCECAO,
  MOTIVOS_QUE_SUPRIMEM_PROPOSTA,
  PRECEDENCIA_MOTIVOS,
  RegistroTriagemDto,
  SerieTriagemDto,
  TaxaExcecaoDto,
  TriagemNotaDto,
} from './dto/triagem-nota.dto';
import { PropostaService } from './proposta.service';
import { TriagemRepository } from './triagem.repository';

/** As três tabelas, na ordem canônica — motivo do produto afeta todas. */
const TODAS_AS_TABELAS: TabelaPreco[] = ['varejo', 'atacado_especial', 'atacado'];

/** Rótulo das cinco parcelas do custo composto, para o detalhe da divergência. */
const ROTULO_PARCELA: Record<string, string> = {
  unitario: 'unitário',
  icms_st: 'ICMS ST',
  icms_st_complementar: 'ICMS ST complementar',
  ipi: 'IPI',
  frete: 'frete',
};

/**
 * Triagem da nota entre automáticos e exceções (US-043 / T-030).
 *
 * Responde **"o que o motor decidiu sozinho, e o que precisa de aval — e por quê?"**. Aqui
 * não se propõe preço: a proposta chega pronta pelo contrato (`PropostaNotaDto`) e é
 * **classificada**.
 *
 * ---
 * ## Por que esta é uma fatia própria, e não mais um pedaço de `PropostaService`
 *
 * `PropostaService` responde *"quanto?"*; esta fatia responde *"quem revisa?"*. São perguntas
 * de donos diferentes — o motor e o comprador — e a segunda é derivável **inteiramente** do
 * retorno da primeira. Estendê-lo faria a triagem herdar as três leituras que a proposta já
 * orquestra e amarraria a classificação à forma interna da árvore de decisão, quando o que ela
 * precisa é do **contrato**.
 *
 * O precedente de forma é o `ElegibilidadeService` (T-025): **política pura** sobre dados já
 * lidos. `triarNota` não faz I/O algum e é onde vive toda a regra; `porNota`/`porPedido` são a
 * casca fina que busca os insumos e registra a taxa.
 *
 * ---
 * ## A triagem separa aval de preço — não preço de ausência de preço
 *
 * Dos sete motivos, **um só** suprime a proposta: `custo nao confiavel`. Nos outros seis o item
 * chega ao comprador **com preço nas três tabelas**, e ele confirma ou corrige. É por isso que
 * `TaxaExcecaoDto.excecoes_sem_proposta` existe: ele mede a invariante, e qualquer valor > 0 é
 * defeito, não caso de negócio.
 *
 * Item inelegível dispara **só** `custo nao confiavel`, e nenhum dos outros seis: os demais são
 * julgamentos sobre uma proposta que, ali, não existe — marcá-los seria opinar sobre números
 * que o motor se recusou a calcular.
 *
 * ---
 * ## `preço anterior ilegível` × "sem preço" — a distinção material (2026-07-30)
 *
 * Até aqui, `preco_anterior_origem: 'ausente'` misturava dois casos que **não são o mesmo**:
 *
 * | caso | o que é | o que fazer |
 * |---|---|---|
 * | produto sem preço cadastrado no ERP | ausência de **preço** | propor pela faixa — correto |
 * | ERP indisponível, resposta degradada para o espelho | ausência de **leitura** | exceção |
 *
 * A assinatura do segundo caso é lida do próprio contrato, sem campo novo: quando o ERP não
 * responde, `TabelasPrecoService` cai no espelho `com_fifo_completo`, que espelha varejo e
 * atacado especial mas **não** `pro.preco5` — então o item sai com alguma tabela
 * `origem: 'espelho'` **e** o atacado `origem: 'ausente'`. Ausência acompanhada de degradação
 * é ausência de leitura: existe um preço no ERP que não conseguimos ler, e propor pela faixa
 * ali significa ignorá-lo.
 *
 * Sem degradação (as demais tabelas vieram `origem: 'erp'`), a ausência é verdadeira — o
 * produto não tem aquele preço cadastrado — e o caminho da faixa segue valendo, sem exceção
 * por este motivo.
 *
 * A triagem **classifica**, não recalcula: o item continua saindo com o preço que a proposta
 * derivou, agora marcado para o comprador confirmar ou corrigir.
 *
 * ---
 * ## Registro da taxa: a série histórica nunca desliga a triagem
 *
 * O registro em `com_precificacao_triagem_execucao` é **best-effort**. A DDL é manual neste
 * projeto e sua aplicação é ato do responsável; a régua da T-024 mostrou o custo de amarrar a
 * função à DDL (sem a tabela de faixas, a régua responde 503 e a proposta propaga). Aqui a
 * classificação e a taxa saem na resposta **com ou sem** a tabela aplicada, e
 * `RegistroTriagemDto` declara sem esconder se a linha da série foi gravada.
 */
@Injectable()
export class TriagemService {
  constructor(
    private readonly proposta: PropostaService,
    private readonly custo: CustoService,
    private readonly repo: TriagemRepository,
  ) {}

  /** Triagem de todos os pedidos cobertos por uma chave de NF-e. */
  async porNota(chaveNfe: string): Promise<TriagemNotaDto[]> {
    const [propostas, custos] = await Promise.all([
      this.proposta.porNota(chaveNfe),
      this.custo.custoDaNota(chaveNfe),
    ]);
    const porPedido = new Map(custos.map((c) => [c.pedido_id, c]));
    return Promise.all(
      propostas.map((p) => this.montar(p, porPedido.get(p.pedido_id) ?? null)),
    );
  }

  /** Triagem de um pedido inteiro. */
  async porPedido(pedidoId: string): Promise<TriagemNotaDto> {
    const [proposta, custo] = await Promise.all([
      this.proposta.porPedido(pedidoId),
      this.custo.custoDoPedido(pedidoId),
    ]);
    return this.montar(proposta, custo);
  }

  /** Série histórica das taxas — o que torna M5 acompanhável ao longo do tempo. */
  async serie(limite?: number): Promise<SerieTriagemDto> {
    return this.repo.serie(limite);
  }

  // ---------------------------------------------------------------------------
  // Política pura — nenhuma I/O daqui para baixo
  // ---------------------------------------------------------------------------

  /**
   * Classifica a nota inteira a partir do que a proposta já decidiu.
   *
   * Determinística e sem I/O: o custo composto entra como parâmetro (é o mesmo objeto que a
   * proposta já consumiu), e o instante é injetável para o teste não depender do relógio.
   */
  triarNota(
    proposta: PropostaNotaDto,
    custo: CustoComposicaoDto | null,
    triadoEm: string = new Date().toISOString(),
  ): TriagemNotaDto {
    const custoPorProduto = new Map<number, CustoItemDto>(
      (custo?.itens ?? []).map((i) => [i.pro_codigo, i]),
    );

    const itens = (proposta.itens ?? []).map((item) =>
      this.triarItem(item, custoPorProduto.get(item.pro_codigo) ?? null),
    );

    return {
      pedido_id: proposta.pedido_id,
      chave_nfe: proposta.chave_nfe,
      for_codigo: proposta.for_codigo,
      taxa: this.medirTaxa(itens),
      itens,
      registro: { persistido: false, execucao_id: null, indisponivel_motivo: null },
      triado_em: triadoEm,
    };
  }

  /** UM item: dispara os motivos aplicáveis, ordena por precedência e classifica. */
  private triarItem(item: PropostaItemDto, custo: CustoItemDto | null): ItemTriadoDto {
    const excecoes = this.excecoesDoItem(item, custo);
    const comProposta = (item.propostas ?? []).every((p) => p.preco_proposto != null);

    return {
      pro_codigo: item.pro_codigo,
      pro_descricao: item.pro_descricao,
      classificacao: excecoes.length ? 'excecao' : 'automatico',
      motivo_principal: excecoes.length ? excecoes[0].motivo : null,
      motivos: excecoes.map((e) => e.motivo),
      excecoes,
      com_proposta: comProposta,
    };
  }

  /**
   * Os motivos disparados por um item, já na ordem de `PRECEDENCIA_MOTIVOS`.
   *
   * Item inelegível dispara **somente** `custo nao confiavel` — ver o cabeçalho da classe.
   */
  private excecoesDoItem(item: PropostaItemDto, custo: CustoItemDto | null): ExcecaoTriadaDto[] {
    if (!item.elegivel) {
      const detalhes = (item.elegibilidade?.travas ?? []).map((t) => t.detalhe);
      return [
        this.excecao(
          'custo_nao_confiavel',
          detalhes.length ? detalhes.join('; ') : 'nenhuma âncora de custo aceita',
          TODAS_AS_TABELAS,
        ),
      ];
    }

    const achados: ExcecaoTriadaDto[] = [];
    const push = (e: ExcecaoTriadaDto | null) => {
      if (e) achados.push(e);
    };

    push(this.precoAnteriorIlegivel(item));
    push(this.precoAnteriorInconsistente(item));
    push(this.markupForaDoCorredor(item));
    push(this.custoDivergenteDoPedido(item, custo));
    push(this.sinalGiroAusenteOuVelho(item));
    push(this.confirmacaoDeMercado(item));

    const ordem = (c: ChaveMotivoExcecao) => PRECEDENCIA_MOTIVOS.indexOf(c);
    return achados.sort((a, b) => ordem(a.chave) - ordem(b.chave));
  }

  /**
   * Ausência de **leitura**, não de preço: o ERP não respondeu e a resposta degradou para o
   * espelho, que não espelha `pro.preco5`.
   */
  private precoAnteriorIlegivel(item: PropostaItemDto): ExcecaoTriadaDto | null {
    const propostas = item.propostas ?? [];
    const degradou = propostas.some((p) => p.preco_anterior_origem === 'espelho');
    if (!degradou) return null;

    const ilegiveis = propostas.filter(
      (p) => p.preco_anterior == null && p.preco_anterior_origem === 'ausente',
    );
    if (!ilegiveis.length) return null;

    return this.excecao(
      'preco_anterior_ilegivel',
      `ERP não respondeu (as demais tabelas vieram do espelho com_fifo_completo) e ` +
        `${ilegiveis.map((p) => p.rotulo).join(', ')} não é espelhada — há preço no ERP que ` +
        'não foi possível ler. A proposta veio da faixa e precisa ser confirmada contra o ' +
        'preço real, não aceita como se o produto não tivesse preço.',
      ilegiveis.map((p) => p.tabela),
    );
  }

  /**
   * O preço anterior existe mas não serve de âncora: atacado especial **acima** do varejo
   * (94 itens medidos) ou markup anterior **negativo** (49 itens) — US-043.
   */
  private precoAnteriorInconsistente(item: PropostaItemDto): ExcecaoTriadaDto | null {
    const propostas = item.propostas ?? [];
    const preco = (t: TabelaPreco) => propostas.find((p) => p.tabela === t)?.preco_anterior ?? null;
    const varejo = preco('varejo');
    const especial = preco('atacado_especial');

    const detalhes: string[] = [];
    const tabelas: TabelaPreco[] = [];

    if (varejo != null && especial != null && especial > varejo) {
      detalhes.push(
        `atacado especial R$ ${especial.toFixed(2)} acima do varejo R$ ${varejo.toFixed(2)}`,
      );
      tabelas.push('varejo', 'atacado_especial');
    }

    for (const p of propostas) {
      if (p.markup_anterior_pct != null && p.markup_anterior_pct < 0) {
        detalhes.push(`${p.rotulo} com markup anterior negativo (${p.markup_anterior_pct}%)`);
        if (!tabelas.includes(p.tabela)) tabelas.push(p.tabela);
      }
    }

    if (!detalhes.length) return null;
    return this.excecao('preco_anterior_inconsistente', detalhes.join('; '), tabelas);
  }

  /**
   * O markup que o item praticava estava fora do corredor da faixa e teve de ser movido — a
   * âncora existia, mas não é aproveitável como veio.
   */
  private markupForaDoCorredor(item: PropostaItemDto): ExcecaoTriadaDto | null {
    const fora = (item.propostas ?? []).filter(
      (p) =>
        p.markup_anterior_destino === 'cedido_ao_teto' ||
        p.markup_anterior_destino === 'elevado_ao_piso',
    );
    if (!fora.length) return null;

    return this.excecao(
      'markup_anterior_fora_do_corredor',
      fora
        .map(
          (p) =>
            `${p.rotulo}: markup anterior ${p.markup_anterior_pct}% ${
              p.markup_anterior_destino === 'cedido_ao_teto'
                ? 'acima do teto, cedido ao teto'
                : 'abaixo do piso, elevado ao piso'
            } do corredor ${p.faixa_aplicada?.markup_min_pct}%–${p.faixa_aplicada?.markup_max_pct}%`,
        )
        .join('; '),
      fora.map((p) => p.tabela),
    );
  }

  /**
   * O custo composto da nota não saiu inteiro do XML: alguma parcela caiu no **fallback do
   * pedido** (`origem: 'pedido'`), isto é, no valor negociado e não no faturado.
   *
   * Só se avalia em item **com** vínculo de XML: item sem nenhuma chave já é inelegível pela
   * trava `custo_nota_sem_xml` (T-025) e sai por `custo nao confiavel`, sem duplicar motivo.
   * A proposta continua sendo a que o motor fez sobre o custo composto — a divergência é
   * apontada, não corrigida aqui.
   */
  private custoDivergenteDoPedido(
    item: PropostaItemDto,
    custo: CustoItemDto | null,
  ): ExcecaoTriadaDto | null {
    if (!custo || !(item.chaves_nfe ?? []).length) return null;

    const doPedido = Object.entries(custo.parcelas ?? {}).filter(
      ([, parcela]) => (parcela as ParcelaCustoDto)?.origem === 'pedido',
    );
    if (!doPedido.length) return null;

    return this.excecao(
      'custo_divergente_do_pedido',
      `item com XML vinculado, mas ${doPedido
        .map(([campo]) => ROTULO_PARCELA[campo] ?? campo)
        .join(', ')} veio do valor negociado do pedido, não do faturado — o custo composto ` +
        'diverge da nota e a proposta precisa ser conferida contra ela.',
      TODAS_AS_TABELAS,
    );
  }

  /** Sem sinal julgável, ou com sinal derivado de materialização velha/ausente (US-039). */
  private sinalGiroAusenteOuVelho(item: PropostaItemDto): ExcecaoTriadaDto | null {
    const sinal = item.sinal_performance;
    if (!sinal) return null;
    if (sinal.base !== 'ausente' && !sinal.dado_desatualizado) return null;

    const partes: string[] = [];
    if (sinal.base === 'ausente') partes.push(`sem sinal julgável (${sinal.detalhe})`);
    if (sinal.dado_desatualizado) {
      partes.push(`materialização do giro "${sinal.frescor}"`);
    }

    return this.excecao('sinal_giro_ausente_ou_velho', partes.join('; '), TODAS_AS_TABELAS);
  }

  /**
   * A marcação seletiva que a própria proposta já fez (T-026) — reaproveitada, nunca
   * reimplementada: quem decide se o proxy de giro sustenta a decisão sozinho é o motor.
   */
  private confirmacaoDeMercado(item: PropostaItemDto): ExcecaoTriadaDto | null {
    const marcadas = (item.propostas ?? []).filter((p) => p.precisa_confirmacao_mercado);
    if (!marcadas.length) return null;

    return this.excecao(
      'confirmacao_de_mercado',
      marcadas.map((p) => `${p.rotulo}: ${p.ramo}`).join('; '),
      marcadas.map((p) => p.tabela),
    );
  }

  private excecao(
    chave: ChaveMotivoExcecao,
    detalhe: string,
    tabelas: TabelaPreco[],
  ): ExcecaoTriadaDto {
    return { chave, motivo: MOTIVO_EXCECAO[chave], detalhe, tabelas };
  }

  // ---------------------------------------------------------------------------
  // A taxa
  // ---------------------------------------------------------------------------

  private medirTaxa(itens: ItemTriadoDto[]): TaxaExcecaoDto {
    const excecoes = itens.filter((i) => i.classificacao === 'excecao');
    const total = itens.length;
    const taxa = total ? this.arred4((excecoes.length / total) * 100) : 0;

    const principalPorChave = new Map<ChaveMotivoExcecao, number>();
    const itensPorChave = new Map<ChaveMotivoExcecao, number>();
    for (const item of itens) {
      const chaves = item.excecoes.map((e) => e.chave);
      chaves.forEach((c) => itensPorChave.set(c, (itensPorChave.get(c) ?? 0) + 1));
      if (chaves.length) {
        principalPorChave.set(chaves[0], (principalPorChave.get(chaves[0]) ?? 0) + 1);
      }
    }

    const por_motivo: ContagemMotivoDto[] = PRECEDENCIA_MOTIVOS.filter((c) =>
      itensPorChave.has(c),
    ).map((chave) => ({
      chave,
      motivo: MOTIVO_EXCECAO[chave],
      itens: itensPorChave.get(chave) ?? 0,
      itens_como_principal: principalPorChave.get(chave) ?? 0,
    }));

    return {
      itens: total,
      automaticos: total - excecoes.length,
      excecoes: excecoes.length,
      taxa_excecao_pct: taxa,
      meta_pct: META_TAXA_EXCECAO_PCT,
      dentro_da_meta: taxa <= META_TAXA_EXCECAO_PCT,
      excecoes_sem_proposta: excecoes.filter(
        (i) => !i.com_proposta && !this.suprimeProposta(i.excecoes[0].chave),
      ).length,
      por_motivo,
    };
  }

  private suprimeProposta(chave: ChaveMotivoExcecao): boolean {
    return MOTIVOS_QUE_SUPRIMEM_PROPOSTA.includes(chave);
  }

  private arred4(v: number): number {
    return Math.round(v * 10_000) / 10_000;
  }

  // ---------------------------------------------------------------------------
  // Orquestração
  // ---------------------------------------------------------------------------

  /** Triagem + registro best-effort da taxa na série histórica. */
  private async montar(
    proposta: PropostaNotaDto,
    custo: CustoComposicaoDto | null,
  ): Promise<TriagemNotaDto> {
    const dto = this.triarNota(proposta, custo);
    const registro: RegistroTriagemDto = await this.repo.registrar(dto);
    return { ...dto, registro };
  }
}
