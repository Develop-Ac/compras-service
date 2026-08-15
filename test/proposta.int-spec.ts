import { CustoService } from '../src/compras/precificacao/custo.service';
import {
  CustoComposicaoDto,
  CustoItemDto,
} from '../src/compras/precificacao/dto/custo-item.dto';
import {
  CUSTO_FONTE_CONFIAVEL,
  MOTIVO_CUSTO_NAO_CONFIAVEL,
} from '../src/compras/precificacao/dto/elegibilidade.dto';
import {
  PrecoVigenteDto,
  TABELAS_PRECO,
  TabelaPreco,
} from '../src/compras/precificacao/dto/preco-vigente.dto';
import {
  FRACAO_POSICAO,
  MARCACAO_CONFIRMACAO_MERCADO,
  PropostaNotaDto,
  PropostaTabelaDto,
} from '../src/compras/precificacao/dto/proposta-preco.dto';
import { SinalGiroDto } from '../src/compras/precificacao/dto/sinal-giro.dto';
import { ElegibilidadeService } from '../src/compras/precificacao/elegibilidade.service';
import { GiroRepository } from '../src/compras/precificacao/giro.repository';
import { GiroService } from '../src/compras/precificacao/giro.service';
import { PropostaRepository } from '../src/compras/precificacao/proposta.repository';
import { PropostaService } from '../src/compras/precificacao/proposta.service';
import {
  FaixaReguaRow,
  NovaFaixaRegua,
  ReguaRepository,
} from '../src/compras/precificacao/regua.repository';
import { ReguaService } from '../src/compras/precificacao/regua.service';
import { TabelasPrecoService } from '../src/compras/precificacao/tabelas-preco.service';

/**
 * US-041 / T-026 — proposta de preço ancorada no markup anterior, com fallback pela faixa.
 *
 * ## Como esta suíte é montada
 *
 * `ReguaService` e `ElegibilidadeService` são **reais**: a trava de piso da T-024 é justamente
 * o que a US-041 manda usar em vez de checagem própria, e a elegibilidade da T-025 é política
 * pura sem I/O. Só as **bordas de I/O** são stubadas — custo composto (US-037), preços vigentes
 * (US-038), sinal de giro (US-039) e a leitura de `com_fifo_completo`.
 *
 * A régua roda sobre um `FakeReguaRepository` que espelha o seed de
 * `sql/2026-07-30_precificacao_faixas.sql`, **porque a DDL das faixas não foi aplicada** — os
 * endpoints da régua respondem 503 contra o banco real hoje. A perna contra Postgres real está
 * marcada `pendente-provisao` no fim do arquivo, conforme TESTING.md#gates.
 *
 * ## Números das fixtures (todos derivados do seed, nenhum inventado)
 *
 * Custo atual R$ 40,48 (o par vivo `33090` da demo da sprint-04) cai na faixa de varejo
 * 40,01–99,01 → corredor 130%–150% → `preco_minimo` R$ 93,11 (arredondado PARA CIMA),
 * `preco_maximo` R$ 101,20 (PARA BAIXO). Atacado é valor único 67% → R$ 67,61 / R$ 67,60
 * (o piso vence o centavo); atacado especial 50% → R$ 60,72.
 */

const PRO_COM_XML = 33090;
const PRO_SEM_XML = 33091;
const CHAVE_NFE = '35260712345678901234550010000123451234567890';
const PEDIDO_ID = 'clx0k9v2h0000abcd1234efgh';

const CUSTO_ATUAL = 40.48;
/** Corredor de varejo para R$ 40,48 — conferido contra o seed. */
const VAREJO_MIN = 93.11;
const VAREJO_MAX = 101.2;

// ---------------------------------------------------------------------------
// Régua real sobre o seed versionado (mesma fixture da T-024)
// ---------------------------------------------------------------------------

const SEED: Array<[TabelaPreco, number, number | null, number, number]> = [
  ['varejo', 0, 15.01, 200, 1000],
  ['varejo', 15.01, 40.01, 150, 200],
  ['varejo', 40.01, 99.01, 130, 150],
  ['varejo', 99.01, 400.01, 100, 110],
  ['varejo', 400.01, null, 100, 150],
  ['atacado', 0, null, 67, 67],
  ['atacado_especial', 0, null, 50, 50],
];

class FakeReguaRepository {
  linhas: FaixaReguaRow[] = [];
  private seq = 0;

  constructor() {
    const agora = new Date('2026-07-30T09:00:00.000Z');
    for (const [tabela, min, max, mkMin, mkMax] of SEED) {
      this.linhas.push({
        id: ++this.seq,
        tabela,
        custo_min: min,
        custo_max: max,
        markup_min_pct: mkMin,
        markup_max_pct: mkMax,
        vigencia_inicio: agora,
        vigencia_fim: null,
        criado_por: 'responsavel (PRD#escopo, 2026-07-28)',
        criado_em: agora,
        encerrado_por: null,
        encerrado_em: null,
        motivo: 'régua inicial',
      });
    }
  }

  async faixasVigentes(): Promise<FaixaReguaRow[]> {
    return this.linhas.map((l) => ({ ...l }));
  }

  async historico(): Promise<FaixaReguaRow[]> {
    return this.linhas.map((l) => ({ ...l }));
  }

  async substituirFaixas(_novas: NovaFaixaRegua[], _agora: Date): Promise<FaixaReguaRow[]> {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Fixtures das bordas de I/O
// ---------------------------------------------------------------------------

/** Linha de `com_fifo_completo`: o custo ANTERIOR e o estoque do item. */
interface FifoFixture {
  pro_codigo?: number;
  custo_unitario?: number | null;
  custo_fonte?: string | null;
  estoque_disponivel?: number | null;
  tempo_medio_saldo_atual?: number | null;
}

/** Preços anteriores por tabela; `undefined` numa tabela = preço ausente na fonte. */
type PrecosFixture = Partial<Record<TabelaPreco, number | null>>;

interface GiroFixture {
  giro_real?: number | null;
  giro_esperado?: number | null;
  escopo?: 'grupo' | 'produto';
  frescor?: 'fresco' | 'obsoleto' | 'sem-materializacao';
}

function itemCusto(over: Partial<CustoItemDto> = {}): CustoItemDto {
  const parcela = { valor: 0, origem: 'xml' as const };
  return {
    pro_codigo: PRO_COM_XML,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    unidade: 'UN',
    quantidade: 12,
    chaves_nfe: [CHAVE_NFE],
    custo_unitario: CUSTO_ATUAL,
    custo_total: CUSTO_ATUAL * 12,
    parcelas: {
      unitario: { ...parcela, valor: CUSTO_ATUAL },
      icms_st: { ...parcela },
      icms_st_complementar: { ...parcela },
      ipi: { ...parcela },
      frete: { ...parcela },
    },
    ...over,
  };
}

function composicao(itens: CustoItemDto[], chaveNfe: string | null = CHAVE_NFE): CustoComposicaoDto {
  return {
    pedido_id: PEDIDO_ID,
    for_codigo: 1234,
    ipi_no_valor: false,
    chave_nfe: chaveNfe,
    totais: {
      itens: itens.length,
      custo_total: 0,
      unitario: 0,
      icms_st: 0,
      icms_st_complementar: 0,
      ipi: 0,
      frete: 0,
    },
    itens,
  };
}

function precoVigente(proCodigo: number, precos: PrecosFixture): PrecoVigenteDto {
  return {
    pro_codigo: proCodigo,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    precos: TABELAS_PRECO.map((m) => {
      const valor = Object.prototype.hasOwnProperty.call(precos, m.tabela)
        ? (precos[m.tabela] ?? null)
        : null;
      return {
        tabela: m.tabela,
        rotulo: m.rotulo,
        campo_erp: m.campo_erp,
        campo_espelho: m.campo_espelho,
        valor,
        origem: valor == null ? ('ausente' as const) : ('erp' as const),
        atualizado_em: valor == null ? null : '2026-07-30T11:00:00.000Z',
      };
    }),
    lido_em: '2026-07-30T12:00:00.000Z',
    espelho_atualizado_em: '2026-07-29T06:00:00.000Z',
    espelho_idade_horas: 30,
    erp_disponivel: true,
  };
}

function sinalGiro(proCodigo: number, g: GiroFixture): SinalGiroDto {
  const escopo = g.escopo ?? 'grupo';
  const frescor = g.frescor ?? 'fresco';
  return {
    pro_codigo: proCodigo,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    escopo,
    sem_grupo: escopo === 'produto',
    group_id: escopo === 'grupo' ? 'GRP-000123' : null,
    grupo_chave: escopo === 'grupo' ? 'PASTILHA FREIO DIANT / GOL G5' : null,
    grupo_qtd_itens: escopo === 'grupo' ? 4 : null,
    agrupamento_materializado: escopo === 'grupo',
    giro: [
      {
        metrica: 'giro_real',
        rotulo: 'Giro real',
        valor: g.giro_real ?? null,
        unidade: 'un/dia',
        escopo,
        campo_origem: 'com_fifo_completo.grp_demanda_media_dia',
      },
      {
        metrica: 'giro_esperado',
        rotulo: 'Giro esperado',
        valor: g.giro_esperado ?? null,
        unidade: 'un/dia',
        escopo,
        campo_origem: 'com_fifo_completo.grupo_demanda_dia',
      },
    ],
    vendas_ult_12m: 1240.5,
    tempo_medio_estoque_dias: 47.5,
    materializado_em: '2026-07-29T06:00:00.000Z',
    idade_horas: frescor === 'fresco' ? 6 : 96,
    idade_dias: frescor === 'fresco' ? 0.25 : 4,
    frescor,
    janela_frescura_horas: 72,
    dado_desatualizado: frescor !== 'fresco',
    lido_em: '2026-07-30T12:00:00.000Z',
  };
}

// ---------------------------------------------------------------------------
// Montagem do service sob teste
// ---------------------------------------------------------------------------

interface Cenario {
  itens?: CustoItemDto[];
  fifo?: FifoFixture[];
  precos?: Record<number, PrecosFixture>;
  giro?: Record<number, GiroFixture>;
  /** Vínculo VIVO produto → grupo (`com_relacionamento_itens`) — a herança da US-042 sai daqui. */
  grupos?: Record<number, string>;
}

function montar(c: Cenario = {}) {
  const itens = c.itens ?? [itemCusto()];
  const fifoBase: FifoFixture[] = c.fifo ?? [
    { custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 },
  ];

  // US-042: o vínculo VIVO produto ↔ grupo. O dublê responde às DUAS formas de consulta que a
  // herança usa — por `pro_codigo` (quem é o grupo do item) e por `group_id` (quem mais está
  // nele) — porque é isso que o repositório real faz.
  const vinculos = Object.entries(c.grupos ?? {}).map(([cod, gid]) => ({
    pro_codigo: String(cod),
    group_id: gid,
  }));

  const prisma = {
    com_relacionamento_itens: {
      findMany: jest.fn().mockImplementation(async (args: Record<string, any>) => {
        const where = (args?.where ?? {}) as Record<string, { in?: string[] }>;
        if (where.pro_codigo?.in) {
          const alvo = new Set(where.pro_codigo.in.map(String));
          return vinculos.filter((v) => alvo.has(v.pro_codigo));
        }
        if (where.group_id?.in) {
          const alvo = new Set(where.group_id.in.map(String));
          return vinculos.filter((v) => alvo.has(v.group_id));
        }
        return vinculos;
      }),
    },
    com_fifo_completo: {
      findMany: jest.fn().mockResolvedValue(
        fifoBase.map((f) => ({
          pro_codigo: String(f.pro_codigo ?? PRO_COM_XML),
          custo_unitario: f.custo_unitario === undefined ? 44 : f.custo_unitario,
          custo_fonte: f.custo_fonte === undefined ? CUSTO_FONTE_CONFIAVEL : f.custo_fonte,
          estoque_disponivel: f.estoque_disponivel === undefined ? 12 : f.estoque_disponivel,
          tempo_medio_saldo_atual:
            f.tempo_medio_saldo_atual === undefined ? 47.5 : f.tempo_medio_saldo_atual,
          data_processamento: new Date('2026-07-29T06:00:00.000Z'),
        })),
      ),
    },
  };

  const custoDaNota = jest.fn().mockResolvedValue([composicao(itens)]);
  const custoDoPedido = jest.fn().mockResolvedValue(composicao(itens, null));
  const custo = { custoDaNota, custoDoPedido } as unknown as CustoService;

  const tabelasPreco = {
    precosVigentesLote: jest
      .fn()
      .mockImplementation(async (codigos: number[]) =>
        codigos.map((cod) => precoVigente(cod, c.precos?.[cod] ?? { varejo: 101.2 })),
      ),
  } as unknown as TabelasPrecoService;

  const giro = {
    sinalGiroLote: jest
      .fn()
      .mockImplementation(async (codigos: number[]) =>
        codigos.map((cod) => sinalGiro(cod, c.giro?.[cod] ?? {})),
      ),
  } as unknown as GiroService;

  const regua = new ReguaService(new FakeReguaRepository() as unknown as ReguaRepository);

  // T-027: as três leituras saíram do service para `PropostaRepository` (refactor puro).
  // Só a FIAÇÃO mudou — os mesmos dublês, agora atrás da porta de leitura. Nenhuma asserção
  // desta suíte foi tocada.
  // T-029: a quarta porta é o vínculo vivo de grupo, delegado ao `GiroRepository` real sobre o
  // mesmo dublê de Prisma — um ativo, dois usos (giro e herança de markup).
  const repositorio = new PropostaRepository(
    prisma as never,
    tabelasPreco,
    giro,
    new GiroRepository(prisma as never),
  );

  const service = new PropostaService(repositorio, custo, regua, new ElegibilidadeService());

  return { service, custoDaNota, custoDoPedido, regua };
}

async function proporNota(c: Cenario = {}): Promise<PropostaNotaDto> {
  const { service } = montar(c);
  const [nota] = await service.porNota(CHAVE_NFE);
  return nota;
}

function daTabela(nota: PropostaNotaDto, tabela: TabelaPreco, indice = 0): PropostaTabelaDto {
  const p = nota.itens[indice].propostas.find((x) => x.tabela === tabela);
  if (!p) throw new Error(`tabela ${tabela} ausente na proposta`);
  return p;
}

// ===========================================================================
// AC1 — custo caiu, há estoque, markup anterior dentro do corredor
// ===========================================================================

describe('Dado um item com saldo em estoque, custo novo menor e markup anterior no corredor', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
    precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX } }, // markup anterior 130% sobre R$ 44
  };

  it('Então o preço proposto é igual ao preço anterior', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto).toBe(VAREJO_MAX);
  });

  it('E o ramo registrado é o do custo que caiu com estoque', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.ramo).toBe('custo_caiu_com_estoque');
  });

  it('E o motivo em uma linha cita custo, estoque e o corredor da margem', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.motivo).toMatch(/custo caiu de R\$ 44,00 para R\$ 40,48.*estoque.*dentro do corredor/);
  });

  it('E o motivo é UMA linha, consumível pelo extrato de decisão', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.motivo).not.toContain('\n');
  });

  it('E o markup anterior é declarado como mantido', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.markup_anterior_destino).toBe('mantido');
  });

  it('E o extrato expõe custo anterior → custo atual', async () => {
    const item = (await proporNota(cenario)).itens[0];

    expect([item.custo_anterior, item.custo_atual]).toEqual([44, CUSTO_ATUAL]);
  });

  it('E o extrato expõe preço anterior → preço proposto sem variação', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect([p.preco_anterior, p.preco_proposto, p.variacao_preco]).toEqual([
      VAREJO_MAX,
      VAREJO_MAX,
      0,
    ]);
  });
});

// ===========================================================================
// Camada FIFO antiga viva — custo e preço anteriores permanecem
// ===========================================================================

describe('Dado um item com camada FIFO antiga viva (custo_fonte camada_viva com saldo)', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
    precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX } },
  };

  it('Então a camada antiga viva é sinalizada no extrato', async () => {
    const item = (await proporNota(cenario)).itens[0];

    expect(item.camada_fifo_antiga_viva).toBe(true);
  });

  it('E o preço anterior PERMANECE, mesmo com o custo novo menor', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto).toBe(p.preco_anterior);
  });

  it('E o custo anterior permanece visível ao lado do novo, nunca substituído', async () => {
    const item = (await proporNota(cenario)).itens[0];

    expect(item.custo_anterior).toBe(44);
  });

  it('E sem saldo disponível a camada antiga viva deixa de ser sinalizada', async () => {
    const item = (
      await proporNota({
        ...cenario,
        fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 0 }],
        giro: { [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1 } },
      })
    ).itens[0];

    expect(item.camada_fifo_antiga_viva).toBe(false);
  });
});

// ===========================================================================
// AC2 — custo caiu, estoque zerado, giro real ≥ esperado
// ===========================================================================

describe('Dado um item com estoque zerado, custo menor e giro real ≥ esperado', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 0 }],
    precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX } },
    giro: { [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1 } },
  };

  it('Então o preço proposto é igual ao preço anterior', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto).toBe(VAREJO_MAX);
  });

  it('E o ramo é o da performance boa sem estoque', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.ramo).toBe('custo_caiu_sem_estoque_performance_boa');
  });

  it('E o motivo cita o giro medido do grupo', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.motivo).toMatch(/giro real 3,42 ≥ esperado 2,1 un\/dia \(escopo grupo\)/);
  });

  it('E o sinal de performance registra o escopo grupo como base', async () => {
    const item = (await proporNota(cenario)).itens[0];

    expect([item.sinal_performance.base, item.sinal_performance.posicao]).toEqual([
      'giro',
      'alta',
    ]);
  });
});

// ===========================================================================
// AC3 — custo caiu, estoque zerado, giro real < esperado
// ===========================================================================

describe('Dado um item com estoque zerado, custo menor e giro real < esperado', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 0 }],
    precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX } },
    giro: { [PRO_COM_XML]: { giro_real: 1, giro_esperado: 2.1 } },
  };

  it('Então o preço proposto é MENOR que o anterior', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto as number).toBeLessThan(p.preco_anterior as number);
  });

  it('E permanece no ou acima do limite inferior da tabela — a trava da régua aceita', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.piso_respeitado).toBe(true);
  });

  it('E o markup resultante não fica abaixo do piso da faixa', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.markup_proposto_pct as number).toBeGreaterThanOrEqual(
      p.faixa_aplicada!.markup_min_pct,
    );
  });

  it('E o preço aterra no piso do corredor — posição baixa da função de posicionamento', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto).toBe(VAREJO_MIN);
  });

  it('E o ramo é o da performance ruim sem estoque', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.ramo).toBe('custo_caiu_sem_estoque_performance_ruim');
  });

  it('E quando o piso já impede a redução, o aviso é explícito em vez de furar o piso', async () => {
    const p = daTabela(
      await proporNota({ ...cenario, precos: { [PRO_COM_XML]: { varejo: 90 } } }),
      'varejo',
    );

    expect(p.avisos.join(' ')).toMatch(/[Pp]iso da faixa impede a redução/);
  });
});

// ===========================================================================
// AC4 — custo subiu
// ===========================================================================

describe('Dado um item cujo custo novo é MAIOR que o anterior', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 36, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
    precos: { [PRO_COM_XML]: { varejo: 90 } }, // markup anterior 150% sobre R$ 36
  };

  it('Então o preço proposto é maior ou igual ao anterior', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.preco_proposto as number).toBeGreaterThanOrEqual(p.preco_anterior as number);
  });

  it('E o markup resultante nunca fica abaixo do limite inferior da faixa', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.markup_proposto_pct as number).toBeGreaterThanOrEqual(
      p.faixa_aplicada!.markup_min_pct,
    );
  });

  it('E o motivo indica que o markup anterior foi MANTIDO quando cabe na faixa', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect([p.markup_anterior_destino, /MANTIDO/.test(p.motivo)]).toEqual(['mantido', true]);
  });

  it('E quando o markup anterior não cabe no teto, o motivo indica que foi CEDIDO', async () => {
    const p = daTabela(
      await proporNota({ ...cenario, precos: { [PRO_COM_XML]: { varejo: 100 } } }),
      'varejo',
    );

    expect([p.markup_anterior_destino, /CEDIDO ao teto/.test(p.motivo)]).toEqual([
      'cedido_ao_teto',
      true,
    ]);
  });

  it('E o preço cedido ao teto continua sendo maior ou igual ao anterior', async () => {
    const p = daTabela(
      await proporNota({ ...cenario, precos: { [PRO_COM_XML]: { varejo: 100 } } }),
      'varejo',
    );

    expect(p.preco_proposto as number).toBeGreaterThanOrEqual(100);
  });

  it('E markup anterior abaixo do piso é ELEVADO ao piso, nunca mantido furando', async () => {
    const p = daTabela(
      await proporNota({
        ...cenario,
        fifo: [{ custo_unitario: 36, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
        precos: { [PRO_COM_XML]: { varejo: 50 } }, // markup anterior 38,9% — muito abaixo de 130%
      }),
      'varejo',
    );

    expect([p.markup_anterior_destino, p.preco_proposto, p.piso_respeitado]).toEqual([
      'elevado_ao_piso',
      VAREJO_MIN,
      true,
    ]);
  });
});

// ===========================================================================
// Fallback pela faixa — a proposta nunca sai vazia
// ===========================================================================

describe('Dado um item elegível SEM preço anterior na tabela', () => {
  const semHistorico: Cenario = {
    fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
    precos: { [PRO_COM_XML]: {} },
  };

  it('Então a proposta NÃO sai vazia — o preço é derivado da faixa', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect(p.preco_proposto).not.toBeNull();
  });

  it('E o ramo é o fallback pela faixa', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect(p.ramo).toBe('sem_historico_pela_faixa');
  });

  it('E a proposta sai marcada "precisa de confirmação de mercado"', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect(p.marcacoes).toContain(MARCACAO_CONFIRMACAO_MERCADO);
  });

  it('E marcar para confirmação NÃO suprime o preço — exceção e proposta são independentes', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect([p.precisa_confirmacao_mercado, p.preco_proposto != null]).toEqual([true, true]);
  });

  it('E os campos "anterior" saem explicitamente AUSENTES, nunca zerados', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect([p.preco_anterior, p.markup_anterior_pct, p.variacao_preco]).toEqual([
      null,
      null,
      null,
    ]);
  });

  it('E a ausência é nomeada, para o extrato não confundir com preço zero', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect([p.tem_historico, p.historico_ausente_motivo]).toEqual([
      false,
      'preco_anterior_ausente',
    ]);
  });

  it('E o extrato traz no lugar a FAIXA APLICADA', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect([p.faixa_aplicada!.markup_min_pct, p.faixa_aplicada!.markup_max_pct]).toEqual([
      130, 150,
    ]);
  });

  it('E o extrato traz o SINAL DE PERFORMANCE que sustentou a posição', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect(p.posicao_na_faixa).not.toBeNull();
  });

  it('E o motivo cita a faixa e a posição usada, em uma linha', async () => {
    const p = daTabela(await proporNota(semHistorico), 'varejo');

    expect(p.motivo).toMatch(/proposta derivada da faixa de custo .* posicionada no .* corredor/);
  });

  it('E preço anterior ZERADO na fonte é tratado como sem histórico, com o motivo próprio', async () => {
    const p = daTabela(
      await proporNota({ ...semHistorico, precos: { [PRO_COM_XML]: { varejo: 0 } } }),
      'varejo',
    );

    expect([p.tem_historico, p.historico_ausente_motivo, p.preco_proposto != null]).toEqual([
      false,
      'preco_anterior_zerado',
      true,
    ]);
  });
});

// ===========================================================================
// US-047 / T-028 — âncora da última entrada sem estoque, agora ponta a ponta
// ===========================================================================

/**
 * A T-028 provou os ramos 2 e 3 da US-041 em `elegibilidade.int-spec.ts` com um **espelho
 * declarado** do predicado, porque `test/proposta.int-spec.ts` não estava nos `## Outputs`
 * daquela task. Está nos da T-029: aqui o mesmo dado (`custo_fonte = "ultima_entrada"` com
 * saldo 0) atravessa o `PropostaService` **real** e a asserção é sobre o `ramo` que ele
 * devolve — verdade, não espelho.
 */
describe('Dado um item com custo_fonte "ultima_entrada" e estoque ZERADO (US-047)', () => {
  const semEstoqueUltimaEntrada = (giro: GiroFixture): Cenario => ({
    fifo: [{ custo_unitario: 44, custo_fonte: 'ultima_entrada', estoque_disponivel: 0 }],
    precos: { [PRO_COM_XML]: { varejo: 101.2 } },
    giro: { [PRO_COM_XML]: giro },
  });

  it('Então ele passa o gate de elegibilidade e recebe proposta nas três tabelas', async () => {
    const nota = await proporNota(semEstoqueUltimaEntrada({ giro_real: 5, giro_esperado: 2 }));

    expect([
      nota.itens[0].elegivel,
      nota.itens[0].propostas.every((p) => p.preco_proposto != null),
    ]).toEqual([true, true]);
  });

  it('E com giro real ≥ esperado o SERVIÇO devolve o ramo 2 — performance boa sem estoque', async () => {
    const p = daTabela(
      await proporNota(semEstoqueUltimaEntrada({ giro_real: 5, giro_esperado: 2 })),
      'varejo',
    );

    expect([p.ramo, p.preco_proposto]).toEqual([
      'custo_caiu_sem_estoque_performance_boa',
      101.2,
    ]);
  });

  it('E com giro real < esperado o SERVIÇO devolve o ramo 3 — performance ruim sem estoque', async () => {
    const p = daTabela(
      await proporNota(semEstoqueUltimaEntrada({ giro_real: 1, giro_esperado: 4 })),
      'varejo',
    );

    expect([p.ramo, p.preco_proposto! < 101.2, p.piso_respeitado]).toEqual([
      'custo_caiu_sem_estoque_performance_ruim',
      true,
      true,
    ]);
  });

  it('E a camada FIFO antiga viva NÃO é sinalizada — não há saldo que a sustente', async () => {
    const nota = await proporNota(semEstoqueUltimaEntrada({ giro_real: 5, giro_esperado: 2 }));

    expect([nota.itens[0].camada_fifo_antiga_viva, nota.itens[0].com_estoque]).toEqual([
      false,
      false,
    ]);
  });
});

// ===========================================================================
// US-042 / T-029 — herança do markup dos similares do grupo, ANTES da faixa
// ===========================================================================

/**
 * Números das fixtures, todos derivados do corredor real de varejo para R$ 40,48
 * (130%–150% ⇒ R$ 93,11–R$ 101,20):
 *
 * - similar com **145%** (custo R$ 40,00, preço R$ 98,00) ⇒ 40,48 × 2,45 = **R$ 99,18**, dentro
 *   do corredor e **diferente** dos R$ 97,16 que a faixa pura daria no meio (sinal ausente);
 * - similar com **100%** (custo R$ 40,00, preço R$ 80,00) ⇒ R$ 80,96, **abaixo** do piso
 *   R$ 93,11 ⇒ elevado ao piso;
 * - trio 130% / 145% / 150% ⇒ mediana **145%**, o mesmo R$ 99,18 — a mediana ignora as pontas.
 */
const GRUPO = 'GRP-000123';
const SIMILAR_145 = 33095;
const SIMILAR_130 = 33096;
const SIMILAR_150 = 33097;
const SIMILAR_SEM_PRECO = 33098;
/** Preço que a faixa PURA daria (meio do corredor, sinal ausente) — a linha de base. */
const FAIXA_PURA_MEIO = 97.16;

const custoSimilar = (pro_codigo: number) => ({
  pro_codigo,
  custo_unitario: 40,
  custo_fonte: CUSTO_FONTE_CONFIAVEL,
  estoque_disponivel: 5,
});

const comUmSimilar: Cenario = {
  fifo: [
    { custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 },
    custoSimilar(SIMILAR_145),
  ],
  precos: { [PRO_COM_XML]: {}, [SIMILAR_145]: { varejo: 98 } },
  grupos: { [PRO_COM_XML]: GRUPO, [SIMILAR_145]: GRUPO },
};

describe('Dado um item novo cujo grupo tem similar com markup praticado', () => {
  it('Então o markup proposto DERIVA dos similares, não da faixa pura', async () => {
    const p = daTabela(await proporNota(comUmSimilar), 'varejo');

    expect([p.ramo, p.preco_proposto]).toEqual(['sem_historico_herdado_do_grupo', 99.18]);
  });

  it('E o preço herdado é DIFERENTE do que a faixa pura daria — a herança muda a proposta', async () => {
    const herdado = daTabela(await proporNota(comUmSimilar), 'varejo');
    const puro = daTabela(
      await proporNota({ ...comUmSimilar, grupos: { [PRO_COM_XML]: GRUPO } }),
      'varejo',
    );

    expect([puro.ramo, puro.preco_proposto, herdado.preco_proposto]).toEqual([
      'sem_historico_pela_faixa',
      FAIXA_PURA_MEIO,
      99.18,
    ]);
  });

  it('E o motivo CITA QUANTOS similares sustentaram o cálculo', async () => {
    const p = daTabela(await proporNota(comUmSimilar), 'varejo');

    expect(p.motivo).toMatch(/markup herdado de 1 similar do grupo \(mediana 145%\)/);
  });

  it('E o extrato mostra DE QUANTOS, junto com a mediana antes da limitação', async () => {
    const p = daTabela(await proporNota(comUmSimilar), 'varejo');

    expect([p.similares_considerados, p.markup_herdado_pct]).toEqual([1, 145]);
  });

  it('E os campos "anterior" continuam explicitamente AUSENTES, nunca zerados', async () => {
    const p = daTabela(await proporNota(comUmSimilar), 'varejo');

    expect([
      p.tem_historico,
      p.historico_ausente_motivo,
      p.preco_anterior,
      p.markup_anterior_pct,
      p.variacao_preco,
    ]).toEqual([false, 'preco_anterior_ausente', null, null, null]);
  });

  it('E a proposta herdada continua marcada "precisa de confirmação de mercado"', async () => {
    const p = daTabela(await proporNota(comUmSimilar), 'varejo');

    expect([p.precisa_confirmacao_mercado, p.marcacoes]).toEqual([
      true,
      [MARCACAO_CONFIRMACAO_MERCADO],
    ]);
  });

  it('E com TRÊS similares o markup é a MEDIANA, não a média — as pontas não puxam', async () => {
    const p = daTabela(
      await proporNota({
        fifo: [
          { custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 },
          custoSimilar(SIMILAR_145),
          custoSimilar(SIMILAR_130),
          custoSimilar(SIMILAR_150),
        ],
        precos: {
          [PRO_COM_XML]: {},
          [SIMILAR_145]: { varejo: 98 },
          [SIMILAR_130]: { varejo: 92 },
          [SIMILAR_150]: { varejo: 100 },
        },
        grupos: {
          [PRO_COM_XML]: GRUPO,
          [SIMILAR_145]: GRUPO,
          [SIMILAR_130]: GRUPO,
          [SIMILAR_150]: GRUPO,
        },
      }),
      'varejo',
    );

    expect([p.similares_considerados, p.markup_herdado_pct, p.preco_proposto]).toEqual([
      3, 145, 99.18,
    ]);
  });

  it('E similar SEM preço na tabela não entra na mediana — é ausência, nunca markup zero', async () => {
    const p = daTabela(
      await proporNota({
        fifo: [
          { custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 },
          custoSimilar(SIMILAR_145),
          custoSimilar(SIMILAR_SEM_PRECO),
        ],
        precos: {
          [PRO_COM_XML]: {},
          [SIMILAR_145]: { varejo: 98 },
          [SIMILAR_SEM_PRECO]: {},
        },
        grupos: {
          [PRO_COM_XML]: GRUPO,
          [SIMILAR_145]: GRUPO,
          [SIMILAR_SEM_PRECO]: GRUPO,
        },
      }),
      'varejo',
    );

    expect([p.similares_considerados, p.markup_herdado_pct]).toEqual([1, 145]);
  });

  it('E os totais contam a herança separada do fallback pela faixa', async () => {
    const nota = await proporNota(comUmSimilar);

    expect([nota.totais.sem_historico_herdado, nota.totais.elegiveis_sem_proposta]).toEqual([
      1, 0,
    ]);
  });
});

describe('Dado um item novo cujo markup herdado ficaria ABAIXO do piso da faixa', () => {
  const abaixoDoPiso: Cenario = {
    fifo: [
      { custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 },
      custoSimilar(SIMILAR_145),
    ],
    precos: { [PRO_COM_XML]: {}, [SIMILAR_145]: { varejo: 80 } },
    grupos: { [PRO_COM_XML]: GRUPO, [SIMILAR_145]: GRUPO },
  };

  it('Então o markup aplicado é o LIMITE INFERIOR da faixa, nunca o herdado', async () => {
    const p = daTabela(await proporNota(abaixoDoPiso), 'varejo');

    expect([p.markup_herdado_pct, p.preco_proposto, p.faixa_aplicada!.preco_minimo]).toEqual([
      100, 93.11, 93.11,
    ]);
  });

  it('E a trava da régua continua aceitando — nenhuma proposta fura o piso', async () => {
    const p = daTabela(await proporNota(abaixoDoPiso), 'varejo');

    expect([p.piso_respeitado, p.markup_proposto_pct! >= 130]).toEqual([true, true]);
  });

  it('E o aviso declara a limitação, em vez de aplicar o herdado como veio', async () => {
    const p = daTabela(await proporNota(abaixoDoPiso), 'varejo');

    expect(p.avisos.some((a) => /abaixo do piso do corredor/.test(a))).toBe(true);
  });
});

describe('Dado um item novo SEM similar com markup praticado', () => {
  it('Então o comportamento atual é PRESERVADO — proposta pela faixa, marcada', async () => {
    const p = daTabela(
      await proporNota({
        fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
        precos: { [PRO_COM_XML]: {} },
        grupos: { [PRO_COM_XML]: GRUPO },
      }),
      'varejo',
    );

    expect([p.ramo, p.similares_considerados, p.precisa_confirmacao_mercado]).toEqual([
      'sem_historico_pela_faixa',
      null,
      true,
    ]);
  });

  it('E o motivo declara que veio da FAIXA, não de similar nem de histórico', async () => {
    const p = daTabela(
      await proporNota({ precos: { [PRO_COM_XML]: {} }, grupos: { [PRO_COM_XML]: GRUPO } }),
      'varejo',
    );

    expect([
      /proposta derivada da faixa de custo/.test(p.motivo),
      /similar/.test(p.motivo),
    ]).toEqual([true, false]);
  });

  it('E item sem grupo nenhum também segue pela faixa nas TRÊS tabelas', async () => {
    const nota = await proporNota({ precos: { [PRO_COM_XML]: {} } });

    expect(nota.itens[0].propostas.map((p) => [p.ramo, p.preco_proposto != null])).toEqual([
      ['sem_historico_pela_faixa', true],
      ['sem_historico_pela_faixa', true],
      ['sem_historico_pela_faixa', true],
    ]);
  });
});

// ===========================================================================
// A função de posicionamento dentro da faixa — três degraus discretos
// ===========================================================================

describe('Dada a função de posicionamento na faixa (três degraus pelo sinal de performance)', () => {
  const base: Cenario = { precos: { [PRO_COM_XML]: {} } };

  it('Então giro real ≥ esperado posiciona no TETO do corredor', async () => {
    const p = daTabela(
      await proporNota({
        ...base,
        fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
        giro: { [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1 } },
      }),
      'varejo',
    );

    expect([p.posicao_na_faixa, p.preco_proposto]).toEqual(['alta', VAREJO_MAX]);
  });

  it('E giro real < esperado posiciona no PISO do corredor', async () => {
    const p = daTabela(
      await proporNota({
        ...base,
        fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
        giro: { [PRO_COM_XML]: { giro_real: 1, giro_esperado: 2.1 } },
      }),
      'varejo',
    );

    expect([p.posicao_na_faixa, p.preco_proposto]).toEqual(['baixa', VAREJO_MIN]);
  });

  it('E sinal ausente posiciona no MEIO do corredor, nunca no piso', async () => {
    const p = daTabela(
      await proporNota({
        ...base,
        fifo: [
          {
            custo_unitario: 44,
            custo_fonte: CUSTO_FONTE_CONFIAVEL,
            estoque_disponivel: 12,
            tempo_medio_saldo_atual: 100,
          },
        ],
        giro: { [PRO_COM_XML]: { giro_real: null, giro_esperado: null } },
      }),
      'varejo',
    );

    // R$ 93,11 + 0,5 × (R$ 101,20 − R$ 93,11) = R$ 97,155 → arredondado PARA CIMA
    expect([p.posicao_na_faixa, p.preco_proposto]).toEqual(['neutra', 97.16]);
  });

  it('E sem giro com saldo parado ≥ 365 dias, a idade do saldo posiciona no PISO', async () => {
    const nota = await proporNota({
      ...base,
      fifo: [
        {
          custo_unitario: 44,
          custo_fonte: CUSTO_FONTE_CONFIAVEL,
          estoque_disponivel: 30,
          tempo_medio_saldo_atual: 2000,
        },
      ],
      giro: { [PRO_COM_XML]: { giro_real: null, giro_esperado: null } },
    });

    expect([
      nota.itens[0].sinal_performance.base,
      daTabela(nota, 'varejo').preco_proposto,
    ]).toEqual(['idade_saldo', VAREJO_MIN]);
  });

  it('E idade de saldo ANÔMALA (negativa) nunca posiciona no piso — cai em neutra', async () => {
    const nota = await proporNota({
      ...base,
      fifo: [
        {
          custo_unitario: 44,
          custo_fonte: CUSTO_FONTE_CONFIAVEL,
          estoque_disponivel: 30,
          tempo_medio_saldo_atual: -150,
        },
      ],
      giro: { [PRO_COM_XML]: { giro_real: null, giro_esperado: null } },
    });

    expect(nota.itens[0].sinal_performance.posicao).toBe('neutra');
  });

  it('E as frações declaradas são os três degraus, sem interpolação contínua', () => {
    expect(FRACAO_POSICAO).toEqual({ alta: 1, neutra: 0.5, baixa: 0 });
  });

  it('E a posição NÃO tem termo de custo — dois custos da MESMA faixa e mesmo sinal dão o mesmo markup', async () => {
    const markup = async (custo: number) => {
      const nota = await proporNota({
        ...base,
        itens: [itemCusto({ custo_unitario: custo })],
        fifo: [{ custo_unitario: 44, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 12 }],
        giro: { [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1 } },
      });
      return daTabela(nota, 'varejo').faixa_aplicada!.markup_max_pct;
    };

    // R$ 45 e R$ 90 estão na mesma faixa 40,01–99,01: o corredor é o mesmo, o custo não move
    // a posição (a curva "custo menor ⇒ markup maior" da regra 4 do PRD NÃO é reintroduzida).
    expect(await markup(45)).toBe(await markup(90));
  });
});

// ===========================================================================
// Item inelegível — o ÚNICO caso sem preço
// ===========================================================================

describe('Dado um item inelegível por custo do estoque não confiável', () => {
  const cenario: Cenario = {
    fifo: [{ custo_unitario: 40.48, custo_fonte: 'ultima_entrada', estoque_disponivel: 5 }],
  };

  it('Então nenhuma das três tabelas recebe preço', async () => {
    const nota = await proporNota(cenario);

    expect(nota.itens[0].propostas.map((p) => p.preco_proposto)).toEqual([null, null, null]);
  });

  it('E o ramo é inelegível nas três tabelas', async () => {
    const nota = await proporNota(cenario);

    expect(nota.itens[0].propostas.every((p) => p.ramo === 'inelegivel')).toBe(true);
  });

  it('E o motivo registrado é "custo nao confiavel"', async () => {
    const p = daTabela(await proporNota(cenario), 'varejo');

    expect(p.motivo).toContain(MOTIVO_CUSTO_NAO_CONFIAVEL);
  });

  it('E custo_unitario NULL também derruba a proposta, sem virar custo zero', async () => {
    const nota = await proporNota({
      fifo: [{ custo_unitario: null, custo_fonte: CUSTO_FONTE_CONFIAVEL, estoque_disponivel: 5 }],
    });

    expect([nota.itens[0].custo_anterior, nota.itens[0].elegivel]).toEqual([null, false]);
  });

  it('E o item 33091 da demo, de pedido sem vínculo de XML, também sai sem preço', async () => {
    const nota = await proporNota({
      itens: [itemCusto({ pro_codigo: PRO_SEM_XML, custo_unitario: 15.77, chaves_nfe: [] })],
      fifo: [
        {
          pro_codigo: PRO_SEM_XML,
          custo_unitario: 15.77,
          custo_fonte: CUSTO_FONTE_CONFIAVEL,
          estoque_disponivel: 5,
        },
      ],
      precos: { [PRO_SEM_XML]: { varejo: 40 } },
    });

    expect(nota.itens[0].propostas.map((p) => p.preco_proposto)).toEqual([null, null, null]);
  });
});

// ===========================================================================
// Invariantes da regra de 2026-07-29
// ===========================================================================

describe('Dada uma nota com itens de naturezas diferentes', () => {
  const cenario: Cenario = {
    itens: [
      itemCusto(),
      itemCusto({ pro_codigo: 40001 }),
      itemCusto({ pro_codigo: 40002 }),
      itemCusto({ pro_codigo: 40003, chaves_nfe: [] }),
    ],
    fifo: [
      { pro_codigo: PRO_COM_XML, custo_unitario: 44, estoque_disponivel: 12 },
      { pro_codigo: 40001, custo_unitario: 44, estoque_disponivel: 0 },
      { pro_codigo: 40002, custo_unitario: 36, estoque_disponivel: 12 },
      { pro_codigo: 40003, custo_unitario: 44, estoque_disponivel: 12 },
    ],
    precos: {
      [PRO_COM_XML]: { varejo: VAREJO_MAX },
      40001: {},
      40002: { varejo: 90, atacado: 70, atacado_especial: 60 },
      40003: { varejo: 90 },
    },
    giro: {
      [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1 },
      40001: { giro_real: 1, giro_esperado: 2.1 },
      40002: { giro_real: null, giro_esperado: null },
      40003: { giro_real: 3.42, giro_esperado: 2.1 },
    },
  };

  it('Então ZERO itens elegíveis ficam sem proposta nas três tabelas', async () => {
    const nota = await proporNota(cenario);

    expect(nota.totais.elegiveis_sem_proposta).toBe(0);
  });

  it('E nenhuma proposta fura o piso — 100% dos itens processados', async () => {
    const nota = await proporNota(cenario);

    expect(nota.totais.piso_furado).toBe(0);
  });

  it('E o único item sem preço é o inelegível', async () => {
    const nota = await proporNota(cenario);
    const semPreco = nota.itens.filter((i) => i.propostas.some((p) => p.preco_proposto == null));

    expect(semPreco.map((i) => [i.pro_codigo, i.elegivel])).toEqual([[40003, false]]);
  });

  it('E cada item elegível recebe exatamente três propostas com preço', async () => {
    const nota = await proporNota(cenario);
    const elegiveis = nota.itens.filter((i) => i.elegivel);

    expect(
      elegiveis.every((i) => i.propostas.filter((p) => p.preco_proposto != null).length === 3),
    ).toBe(true);
  });

  it('E a nota inteira volta em UMA requisição ao serviço de custo', async () => {
    const { service, custoDaNota } = montar(cenario);

    const [nota] = await service.porNota(CHAVE_NFE);

    expect([custoDaNota.mock.calls.length, nota.itens.length]).toEqual([1, 4]);
  });
});

describe('Dada a exigência de julgar as três tabelas de forma independente', () => {
  it('Então cada tabela recebe seu próprio corredor de markup', async () => {
    const nota = await proporNota({
      precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX, atacado: 75, atacado_especial: 66 } },
    });

    expect(nota.itens[0].propostas.map((p) => p.faixa_aplicada!.markup_min_pct)).toEqual([
      130, 50, 67,
    ]);
  });

  it('E cada tabela recebe seu próprio motivo, nunca um veredito único replicado', async () => {
    const nota = await proporNota({
      precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX, atacado: 75, atacado_especial: 66 } },
    });
    const motivos = nota.itens[0].propostas.map((p) => p.motivo);

    expect(new Set(motivos).size).toBe(3);
  });

  it('E tabelas do MESMO item podem cair em ramos DIFERENTES', async () => {
    // Varejo tem preço anterior (âncora); atacado não tem (fallback pela faixa).
    const nota = await proporNota({
      precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX, atacado_especial: 66 } },
    });

    expect([
      daTabela(nota, 'varejo').ramo,
      daTabela(nota, 'atacado').ramo,
    ]).toEqual(['custo_caiu_com_estoque', 'sem_historico_pela_faixa']);
  });

  it('E preço acima de 67% no atacado sai marcado acima do teto, sem ser recusado', async () => {
    const p = daTabela(
      await proporNota({ precos: { [PRO_COM_XML]: { varejo: VAREJO_MAX, atacado: 75 } } }),
      'atacado',
    );

    expect([p.acima_do_teto, p.piso_respeitado, p.preco_proposto]).toEqual([true, true, 75]);
  });

  it('E no atacado o markup é valor único: piso e teto coincidem', async () => {
    const p = daTabela(await proporNota(), 'atacado');

    expect(p.faixa_aplicada!.markup_min_pct).toBe(p.faixa_aplicada!.markup_max_pct);
  });
});

// ===========================================================================
// Propriedade: nenhum custo, em nenhuma posição, produz preço abaixo do piso
// ===========================================================================

describe('Dada a varredura dos sete custos de corte da régua nas três tabelas', () => {
  const CUSTOS = [0.5, 15, 15.01, 40, 99, 400, 401];
  const SINAIS: GiroFixture[] = [
    { giro_real: 3.42, giro_esperado: 2.1 }, // alta
    { giro_real: 1, giro_esperado: 2.1 }, // baixa
    { giro_real: null, giro_esperado: null }, // neutra
  ];

  it('Então nenhuma proposta, em nenhuma combinação, é recusada pela trava de piso', async () => {
    const recusadas: string[] = [];
    for (const custo of CUSTOS) {
      for (const g of SINAIS) {
        const nota = await proporNota({
          itens: [itemCusto({ custo_unitario: custo })],
          fifo: [
            {
              custo_unitario: 44,
              custo_fonte: CUSTO_FONTE_CONFIAVEL,
              estoque_disponivel: 12,
              tempo_medio_saldo_atual: 100,
            },
          ],
          precos: { [PRO_COM_XML]: {} },
          giro: { [PRO_COM_XML]: g },
        });
        for (const p of nota.itens[0].propostas) {
          if (!p.piso_respeitado) recusadas.push(`${custo}/${p.tabela}`);
        }
      }
    }

    expect(recusadas).toEqual([]);
  });

  it('E todo markup proposto fica no ou acima do piso da faixa aplicada', async () => {
    const furos: string[] = [];
    for (const custo of CUSTOS) {
      const nota = await proporNota({
        itens: [itemCusto({ custo_unitario: custo })],
        precos: { [PRO_COM_XML]: {} },
        giro: { [PRO_COM_XML]: { giro_real: 1, giro_esperado: 2.1 } },
      });
      for (const p of nota.itens[0].propostas) {
        if ((p.markup_proposto_pct as number) < p.faixa_aplicada!.markup_min_pct) {
          furos.push(`${custo}/${p.tabela}`);
        }
      }
    }

    expect(furos).toEqual([]);
  });

  it('E todo custo da varredura recebe faixa — nenhuma proposta sai sem corredor', async () => {
    const semCorredor: number[] = [];
    for (const custo of CUSTOS) {
      const nota = await proporNota({
        itens: [itemCusto({ custo_unitario: custo })],
        precos: { [PRO_COM_XML]: {} },
      });
      if (nota.itens[0].propostas.some((p) => p.faixa_aplicada == null)) semCorredor.push(custo);
    }

    expect(semCorredor).toEqual([]);
  });
});

// ===========================================================================
// Sinal desatualizado e endpoint por pedido
// ===========================================================================

describe('Dado um sinal de giro com materialização obsoleta', () => {
  it('Então a proposta sai marcada para confirmação, sem esconder o dado velho', async () => {
    const p = daTabela(
      await proporNota({
        giro: { [PRO_COM_XML]: { giro_real: 3.42, giro_esperado: 2.1, frescor: 'obsoleto' } },
      }),
      'varejo',
    );

    expect([p.precisa_confirmacao_mercado, /desatualizado/.test(p.avisos.join(' '))]).toEqual([
      true,
      true,
    ]);
  });
});

describe('Dado o endpoint por pedido (item sem vínculo de XML aparece por aqui)', () => {
  it('Então a proposta do pedido inteiro volta em uma resposta, sem chave de NF-e', async () => {
    const { service } = montar();

    const nota = await service.porPedido(PEDIDO_ID);

    expect([nota.chave_nfe, nota.itens.length]).toEqual([null, 1]);
  });
});

// ===========================================================================
// Perna pendente de provisão
// ===========================================================================

it.skip('pendente-provisao: proposta contra a régua no Postgres real (DDL das faixas não aplicada)', () => {
  // A DDL `sql/2026-07-30_precificacao_faixas.sql` foi entregue pela T-024 e ainda NÃO foi
  // aplicada: `com_precificacao_faixa` não existe, os endpoints da régua respondem 503 e a
  // proposta propaga o 503. Esta suíte roda contra o seed espelhado em `FakeReguaRepository`.
  // Assim que a DDL for aplicada, este teste passa a exercitar `GET
  // /compras/precificacao/proposta/nfe/:chaveNfe` ponta a ponta e confere que o corredor lido
  // do banco é o mesmo que o seed prevê.
  expect(true).toBe(true);
});
