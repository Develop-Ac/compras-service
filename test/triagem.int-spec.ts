import { CustoComposicaoDto, CustoItemDto } from '../src/compras/precificacao/dto/custo-item.dto';
import { ElegibilidadeDto } from '../src/compras/precificacao/dto/elegibilidade.dto';
import { CorredorMarkupDto } from '../src/compras/precificacao/dto/faixa-markup.dto';
import { TabelaPreco, TABELAS_PRECO } from '../src/compras/precificacao/dto/preco-vigente.dto';
import {
  PropostaItemDto,
  PropostaNotaDto,
  PropostaTabelaDto,
  SinalPerformanceDto,
} from '../src/compras/precificacao/dto/proposta-preco.dto';
import {
  META_TAXA_EXCECAO_PCT,
  MOTIVO_EXCECAO,
  MotivoExcecao,
} from '../src/compras/precificacao/dto/triagem-nota.dto';
import { TriagemRepository } from '../src/compras/precificacao/triagem.repository';
import { TriagemService } from '../src/compras/precificacao/triagem.service';

/**
 * US-043 / T-030 — triagem da nota entre automáticos e exceções.
 *
 * A regra é **política pura** sobre o contrato da proposta (`PropostaNotaDto`), no molde do
 * `ElegibilidadeService` (T-025): nenhuma I/O, logo o teste exercita `triarNota` diretamente e
 * o banco não entra. A perna que exige Postgres real — provar que a linha da série de fato
 * grava — fica `pendente-provisao` até a DDL manual
 * (`sql/2026-07-30_precificacao_triagem_execucao.sql`) ser aplicada pelo responsável; o que se
 * prova aqui é o **contrário**, e é o que importa para não repetir a régua da T-024: com a
 * tabela ausente a triagem continua respondendo por inteiro.
 *
 * `faixa_aplicada` e `avaliacao_piso` entram como `null` (o contrato os declara nulláveis) ou,
 * onde o corredor é o objeto do teste, com um recorte declarado — a triagem lê **só**
 * `markup_min_pct`/`markup_max_pct` deles.
 */

const CHAVE_NFE = '35260712345678901234550010000123451234567890';

/** Corredor mínimo — a triagem só lê os dois limites de markup. */
const CORREDOR = { markup_min_pct: 200, markup_max_pct: 1000 } as CorredorMarkupDto;

function elegibilidade(over: Partial<ElegibilidadeDto> = {}): ElegibilidadeDto {
  return {
    pro_codigo: 33090,
    elegivel: true,
    motivos: [],
    travas: [],
    custo_estoque_confiavel: true,
    custo_ancora_origem: 'camada_viva',
    custo_ancora_declaracao: 'Âncora: custo médio das camadas FIFO vivas — estoque presente.',
    custo_unitario: 44,
    custo_fonte: 'camada_viva',
    custo_nota_vinculado_xml: true,
    com_estoque: true,
    com_giro: true,
    classificacao_saldo: 'nao-aplicavel',
    idade_saldo_dias: 47.5,
    idade_saldo_anomala: false,
    corte_idade_saldo_dias: 365,
    avaliado_em: '2026-07-30T12:00:00.000Z',
    ...over,
  };
}

function sinal(over: Partial<SinalPerformanceDto> = {}): SinalPerformanceDto {
  return {
    posicao: 'alta',
    fracao_faixa: 1,
    base: 'giro',
    giro_real: 3.42,
    giro_esperado: 2.1,
    razao_giro: 1.6286,
    escopo_giro: 'grupo',
    classificacao_saldo: 'nao-aplicavel',
    frescor: 'fresco',
    dado_desatualizado: false,
    detalhe: 'giro real 3,42 ≥ esperado 2,10 un/dia (escopo grupo)',
    ...over,
  };
}

/** Uma tabela saudável: preço anterior lido do ERP, markup dentro do corredor. */
function tabela(t: TabelaPreco, over: Partial<PropostaTabelaDto> = {}): PropostaTabelaDto {
  const meta = TABELAS_PRECO.find((m) => m.tabela === t)!;
  return {
    tabela: t,
    rotulo: meta.rotulo,
    ramo: 'custo_caiu_com_estoque',
    motivo: 'Custo caiu e o markup anterior está dentro do corredor: preço mantido.',
    tem_historico: true,
    historico_ausente_motivo: null,
    preco_anterior: 189.9,
    preco_anterior_origem: 'erp',
    markup_anterior_pct: 331.4,
    preco_proposto: 189.9,
    markup_proposto_pct: 369.1,
    variacao_preco: 0,
    markup_anterior_destino: 'mantido',
    similares_considerados: null,
    markup_herdado_pct: null,
    faixa_aplicada: CORREDOR,
    posicao_na_faixa: null,
    fracao_na_faixa: null,
    avaliacao_piso: null,
    piso_respeitado: true,
    acima_do_teto: false,
    precisa_confirmacao_mercado: false,
    marcacoes: [],
    avisos: [],
    ...over,
  };
}

/** Item saudável: elegível, com sinal de giro fresco e as três tabelas ancoradas. */
function item(over: Partial<PropostaItemDto> = {}): PropostaItemDto {
  return {
    pro_codigo: 33090,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    chaves_nfe: [CHAVE_NFE],
    quantidade: 12,
    custo_atual: 40.48,
    custo_anterior: 44,
    custo_anterior_fonte: 'camada_viva',
    variacao_custo: -3.52,
    direcao_custo: 'caiu',
    estoque_disponivel: 12,
    com_estoque: true,
    camada_fifo_antiga_viva: true,
    elegibilidade: elegibilidade(),
    elegivel: true,
    sinal_performance: sinal(),
    propostas: [tabela('varejo'), tabela('atacado_especial'), tabela('atacado')],
    ...over,
  };
}

function nota(itens: PropostaItemDto[]): PropostaNotaDto {
  return {
    pedido_id: 'clx0k9v2h0000abcd1234efgh',
    chave_nfe: CHAVE_NFE,
    for_codigo: 1234,
    totais: {} as PropostaNotaDto['totais'],
    itens,
    proposto_em: '2026-07-30T12:00:00.000Z',
  };
}

function custoDaNota(itens: Array<Partial<CustoItemDto>>): CustoComposicaoDto {
  const parcela = (origem: string) => ({ valor: 1, origem: origem as never });
  return {
    pedido_id: 'clx0k9v2h0000abcd1234efgh',
    for_codigo: 1234,
    ipi_no_valor: false,
    chave_nfe: CHAVE_NFE,
    totais: {} as CustoComposicaoDto['totais'],
    itens: itens.map((i) => ({
      pro_codigo: 33090,
      pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
      unidade: 'UN',
      quantidade: 12,
      chaves_nfe: [CHAVE_NFE],
      custo_unitario: 40.48,
      custo_total: 485.76,
      parcelas: {
        unitario: parcela('xml'),
        icms_st: parcela('xml'),
        icms_st_complementar: parcela('ausente'),
        ipi: parcela('xml'),
        frete: parcela('cte'),
      },
      ...i,
    })) as CustoItemDto[],
  };
}

/** Repositório dublê: a série nunca é o objeto do teste de política. */
const repoMudo = () =>
  ({
    registrar: jest.fn().mockResolvedValue({
      persistido: false,
      execucao_id: null,
      indisponivel_motivo: null,
    }),
    serie: jest.fn(),
  }) as unknown as TriagemRepository;

const service = () =>
  new TriagemService({} as never, {} as never, repoMudo());

/** Os sete motivos, como valores — a fonte é o mapa, nunca strings redigitadas. */
const OS_SETE: MotivoExcecao[] = Object.values(MOTIVO_EXCECAO);

describe('TriagemService (US-043 / T-030)', () => {
  // Classificação de toda a nota -----------------------------------------------
  describe('Dada uma NF conferida com N itens processados pelo motor', () => {
    it('Então cada item sai classificado como automático ou exceção', () => {
      const dto = service().triarNota(nota([item(), item({ pro_codigo: 33091 })]), null);

      expect(dto.itens.map((i) => i.classificacao)).toEqual(['automatico', 'automatico']);
    });

    it('E todo item classificado como exceção tem motivo não-vazio', () => {
      const dto = service().triarNota(
        nota([
          item(),
          item({ pro_codigo: 1, sinal_performance: sinal({ base: 'ausente' }) }),
          item({ pro_codigo: 2, elegivel: false }),
        ]),
        null,
      );

      const excecoes = dto.itens.filter((i) => i.classificacao === 'excecao');
      expect(excecoes.length).toBe(2);
      for (const e of excecoes) {
        expect(e.motivo_principal).toBeTruthy();
        expect(e.motivos.length).toBeGreaterThan(0);
      }
    });

    it('E o item automático não carrega motivo algum', () => {
      const dto = service().triarNota(nota([item()]), null);

      expect(dto.itens[0]).toMatchObject({ motivo_principal: null, motivos: [], excecoes: [] });
    });
  });

  // Conjunto fechado de sete ----------------------------------------------------
  describe('Dada uma nota sintética que dispara os sete motivos', () => {
    /** Uma nota com um representante de cada motivo, mais um item limpo. */
    const notaSintetica = () =>
      nota([
        // 1 — automático, sem motivo nenhum
        item({ pro_codigo: 100 }),
        // 2 — custo nao confiavel (o único sem proposta)
        item({
          pro_codigo: 200,
          elegivel: false,
          elegibilidade: elegibilidade({
            elegivel: false,
            travas: [
              {
                trava: 'custo_estoque_nao_confiavel',
                motivo: MOTIVO_EXCECAO.custo_nao_confiavel,
                detalhe: 'custo_fonte = "cadastro"',
                campo_origem: 'com_fifo_completo.custo_fonte',
              },
            ],
          }),
          propostas: [
            tabela('varejo', { preco_proposto: null }),
            tabela('atacado_especial', { preco_proposto: null }),
            tabela('atacado', { preco_proposto: null }),
          ],
        }),
        // 3 — preço anterior ilegível: ERP caiu, varejo/especial vieram do espelho
        item({
          pro_codigo: 300,
          propostas: [
            tabela('varejo', { preco_anterior_origem: 'espelho' }),
            tabela('atacado_especial', { preco_anterior_origem: 'espelho' }),
            tabela('atacado', {
              tem_historico: false,
              preco_anterior: null,
              preco_anterior_origem: 'ausente',
              markup_anterior_pct: null,
              markup_anterior_destino: null,
              ramo: 'sem_historico_pela_faixa',
            }),
          ],
        }),
        // 4 — preço anterior inconsistente: atacado especial acima do varejo
        item({
          pro_codigo: 400,
          propostas: [
            tabela('varejo', { preco_anterior: 100 }),
            tabela('atacado_especial', { preco_anterior: 150 }),
            tabela('atacado'),
          ],
        }),
        // 5 — markup anterior fora do corredor
        item({
          pro_codigo: 500,
          propostas: [
            tabela('varejo', { markup_anterior_destino: 'cedido_ao_teto' }),
            tabela('atacado_especial'),
            tabela('atacado'),
          ],
        }),
        // 6 — custo divergente do pedido (parcela no fallback do pedido, com XML)
        item({ pro_codigo: 600 }),
        // 7 — sinal de giro ausente ou velho
        item({ pro_codigo: 700, sinal_performance: sinal({ frescor: 'obsoleto', dado_desatualizado: true }) }),
        // 8 — precisa de confirmação de mercado
        item({
          pro_codigo: 800,
          propostas: [
            tabela('varejo', { precisa_confirmacao_mercado: true, ramo: 'sem_historico_pela_faixa' }),
            tabela('atacado_especial'),
            tabela('atacado'),
          ],
        }),
      ]);

    const custoSintetico = () =>
      custoDaNota([
        { pro_codigo: 600, parcelas: { unitario: { valor: 15.77, origem: 'pedido' } } as never },
      ]);

    it('Então 100% das exceções têm motivo do conjunto fechado', () => {
      const dto = service().triarNota(notaSintetica(), custoSintetico());

      const motivos = dto.itens.flatMap((i) => i.motivos);
      expect(motivos.length).toBeGreaterThan(0);
      for (const m of motivos) expect(OS_SETE).toContain(m);
    });

    it('E os SETE motivos são de fato alcançáveis — nenhum é ramo morto', () => {
      const dto = service().triarNota(notaSintetica(), custoSintetico());

      const disparados = new Set(dto.itens.flatMap((i) => i.motivos));
      expect([...disparados].sort()).toEqual([...OS_SETE].sort());
    });

    it('E nenhum item tem motivo genérico do tipo "revisar"', () => {
      const dto = service().triarNota(notaSintetica(), custoSintetico());

      for (const m of dto.itens.flatMap((i) => i.motivos)) {
        expect(m).not.toMatch(/revisar|verificar|analisar|conferir manualmente/i);
      }
    });

    it('E toda exceção cujo motivo não é "custo nao confiavel" traz preço nas três tabelas', () => {
      const dto = service().triarNota(notaSintetica(), custoSintetico());

      const semPreco = dto.itens.filter((i) => i.classificacao === 'excecao' && !i.com_proposta);
      expect(semPreco.map((i) => i.motivo_principal)).toEqual([
        MOTIVO_EXCECAO.custo_nao_confiavel,
      ]);
      expect(dto.taxa.excecoes_sem_proposta).toBe(0);
    });
  });

  // O sétimo motivo: ilegível ≠ sem preço ---------------------------------------
  describe('Dado um item cujo atacado não tem preço anterior', () => {
    const semAtacado = (origemDasOutras: 'erp' | 'espelho') =>
      item({
        propostas: [
          tabela('varejo', { preco_anterior_origem: origemDasOutras }),
          tabela('atacado_especial', { preco_anterior_origem: origemDasOutras }),
          tabela('atacado', {
            tem_historico: false,
            preco_anterior: null,
            preco_anterior_origem: 'ausente',
            markup_anterior_pct: null,
            markup_anterior_destino: null,
            ramo: 'sem_historico_pela_faixa',
          }),
        ],
      });

    it('Quando o ERP respondeu, Então a ausência é real e não vira "preço anterior ilegível"', () => {
      const dto = service().triarNota(nota([semAtacado('erp')]), null);

      expect(dto.itens[0].motivos).not.toContain(MOTIVO_EXCECAO.preco_anterior_ilegivel);
    });

    it('Quando o ERP caiu e as outras vieram do espelho, Então o motivo é "preço anterior ilegível"', () => {
      const dto = service().triarNota(nota([semAtacado('espelho')]), null);

      expect(dto.itens[0].motivo_principal).toBe(MOTIVO_EXCECAO.preco_anterior_ilegivel);
    });

    it('E o detalhe diz que existe preço no ERP que não foi possível ler', () => {
      const dto = service().triarNota(nota([semAtacado('espelho')]), null);

      const achado = dto.itens[0].excecoes.find(
        (e) => e.chave === 'preco_anterior_ilegivel',
      );
      expect(achado?.detalhe).toMatch(/não foi possível ler/);
      expect(achado?.tabelas).toEqual(['atacado']);
    });

    it('E o item continua COM preço proposto — ilegível não suprime a proposta', () => {
      const dto = service().triarNota(nota([semAtacado('espelho')]), null);

      expect(dto.itens[0].com_proposta).toBe(true);
    });
  });

  // Item inelegível --------------------------------------------------------------
  describe('Dado um item inelegível por custo não confiável', () => {
    const inelegivel = () =>
      item({
        elegivel: false,
        sinal_performance: sinal({ base: 'ausente', dado_desatualizado: true }),
        elegibilidade: elegibilidade({
          elegivel: false,
          travas: [
            {
              trava: 'custo_nota_sem_xml',
              motivo: MOTIVO_EXCECAO.custo_nao_confiavel,
              detalhe: 'chaves_nfe vazio',
              campo_origem: 'CustoItemDto.chaves_nfe',
            },
          ],
        }),
      });

    it('Então o motivo é "custo nao confiavel" e NENHUM outro', () => {
      const dto = service().triarNota(nota([inelegivel()]), null);

      expect(dto.itens[0].motivos).toEqual([MOTIVO_EXCECAO.custo_nao_confiavel]);
    });

    it('E o detalhe cita a trava concreta que disparou', () => {
      const dto = service().triarNota(nota([inelegivel()]), null);

      expect(dto.itens[0].excecoes[0].detalhe).toContain('chaves_nfe vazio');
    });
  });

  // Taxa de exceção ---------------------------------------------------------------
  describe('Dada a nota processada inteira', () => {
    /** 10 itens, 2 em exceção — taxa 20%, dentro da meta de M5. */
    const dezItens = () =>
      nota([
        ...Array.from({ length: 8 }, (_, n) => item({ pro_codigo: 1000 + n })),
        item({ pro_codigo: 2001, sinal_performance: sinal({ base: 'ausente' }) }),
        item({ pro_codigo: 2002, elegivel: false }),
      ]);

    it('Então a taxa de exceção da execução é medida', () => {
      const dto = service().triarNota(dezItens(), null);

      expect(dto.taxa).toMatchObject({ itens: 10, automaticos: 8, excecoes: 2, taxa_excecao_pct: 20 });
    });

    it('E ela é comparável ao alvo de M5, com a meta declarada no retorno', () => {
      const dto = service().triarNota(dezItens(), null);

      expect(dto.taxa.meta_pct).toBe(META_TAXA_EXCECAO_PCT);
      expect(dto.taxa.dentro_da_meta).toBe(true);
    });

    it('E a taxa acima do alvo é reportada como fora da meta, nunca mascarada', () => {
      const dto = service().triarNota(
        nota([item(), item({ pro_codigo: 2, elegivel: false })]),
        null,
      );

      expect(dto.taxa.taxa_excecao_pct).toBe(50);
      expect(dto.taxa.dentro_da_meta).toBe(false);
    });

    it('E a decomposição por motivo soma exatamente as exceções pelo motivo principal', () => {
      const dto = service().triarNota(dezItens(), null);

      const soma = dto.taxa.por_motivo.reduce((a, m) => a + m.itens_como_principal, 0);
      expect(soma).toBe(dto.taxa.excecoes);
    });

    it('E uma nota sem itens não divide por zero', () => {
      const dto = service().triarNota(nota([]), null);

      expect(dto.taxa).toMatchObject({ itens: 0, excecoes: 0, taxa_excecao_pct: 0 });
    });
  });

  // A série histórica não desliga a triagem -----------------------------------------
  describe('Dada a DDL da série ainda NÃO aplicada', () => {
    const repoIndisponivel = {
      registrar: jest.fn().mockResolvedValue({
        persistido: false,
        execucao_id: null,
        indisponivel_motivo: 'DDL de sql/2026-07-30_precificacao_triagem_execucao.sql ainda não aplicada — a série começa quando o responsável rodá-la.',
      }),
      serie: jest.fn(),
    } as unknown as TriagemRepository;

    it('Então a triagem por pedido responde por inteiro, com a taxa medida', async () => {
      const svc = new TriagemService(
        { porPedido: jest.fn().mockResolvedValue(nota([item()])) } as never,
        { custoDoPedido: jest.fn().mockResolvedValue(custoDaNota([{}])) } as never,
        repoIndisponivel,
      );

      const dto = await svc.porPedido('clx0k9v2h0000abcd1234efgh');

      expect(dto.taxa.itens).toBe(1);
      expect(dto.itens[0].classificacao).toBe('automatico');
    });

    it('E a indisponibilidade da série é declarada, nunca fingida como gravada', async () => {
      const svc = new TriagemService(
        { porPedido: jest.fn().mockResolvedValue(nota([item()])) } as never,
        { custoDoPedido: jest.fn().mockResolvedValue(custoDaNota([{}])) } as never,
        repoIndisponivel,
      );

      const dto = await svc.porPedido('clx0k9v2h0000abcd1234efgh');

      expect(dto.registro.persistido).toBe(false);
      expect(dto.registro.indisponivel_motivo).toMatch(/DDL .* ainda não aplicada/);
    });
  });
});
