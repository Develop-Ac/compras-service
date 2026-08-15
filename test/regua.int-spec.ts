import { BadRequestException, ServiceUnavailableException } from '@nestjs/common';
import { TabelaPreco } from '../src/compras/precificacao/dto/preco-vigente.dto';
import {
  FaixaReguaRow,
  NovaFaixaRegua,
  ReguaRepository,
} from '../src/compras/precificacao/regua.repository';
import { ReguaService } from '../src/compras/precificacao/regua.service';

/**
 * US-040 / T-024 — régua de margem configurável com piso inviolável.
 *
 * A régua é **dado**: os testes exercitam o service contra um duplo do repositório
 * que guarda as faixas em memória, com o MESMO conteúdo semeado por
 * `sql/2026-07-30_precificacao_faixas.sql`. Assim se prova o comportamento da régua
 * (corredor, piso, cobertura, versionamento) sem depender do Postgres — a perna que
 * exige banco real fica `pendente-provisao`, conforme TESTING.md#gates, porque a DDL
 * é aplicada MANUALMENTE pelo responsável.
 *
 * As faixas abaixo são fixture de teste, não constante de produção: o service jamais
 * conhece a régua, sempre a lê do repositório.
 */

/** Espelha, linha por linha, o seed de sql/2026-07-30_precificacao_faixas.sql. */
const SEED: Array<[TabelaPreco, number, number | null, number, number]> = [
  ['varejo', 0, 15.01, 200, 1000],
  ['varejo', 15.01, 40.01, 150, 200],
  ['varejo', 40.01, 99.01, 130, 150],
  ['varejo', 99.01, 400.01, 100, 110],
  // acima de 400,00 volta a 150%: não-monotonicidade INTENCIONAL (PRD#escopo)
  ['varejo', 400.01, null, 100, 150],
  ['atacado', 0, null, 67, 67],
  ['atacado_especial', 0, null, 50, 50],
];

const AUTOR_SEED = 'responsavel (PRD#escopo, 2026-07-28)';

class FakeReguaRepository {
  linhas: FaixaReguaRow[] = [];
  private seq = 0;

  constructor(seed: Array<[TabelaPreco, number, number | null, number, number]> = SEED) {
    const agora = new Date('2026-07-30T09:00:00.000Z');
    for (const [tabela, min, max, mkMin, mkMax] of seed) {
      this.linhas.push({
        id: ++this.seq,
        tabela,
        custo_min: min,
        custo_max: max,
        markup_min_pct: mkMin,
        markup_max_pct: mkMax,
        vigencia_inicio: agora,
        vigencia_fim: null,
        criado_por: AUTOR_SEED,
        criado_em: agora,
        encerrado_por: null,
        encerrado_em: null,
        motivo: 'régua inicial',
      });
    }
  }

  async faixasVigentes(): Promise<FaixaReguaRow[]> {
    return this.linhas
      .filter((l) => l.vigencia_fim == null)
      .sort((a, b) => a.tabela.localeCompare(b.tabela) || a.custo_min - b.custo_min)
      .map((l) => ({ ...l }));
  }

  async historico(tabela?: TabelaPreco): Promise<FaixaReguaRow[]> {
    return this.linhas
      .filter((l) => !tabela || l.tabela === tabela)
      .sort((a, b) => b.vigencia_inicio.getTime() - a.vigencia_inicio.getTime() || b.id - a.id)
      .map((l) => ({ ...l }));
  }

  async substituirFaixas(novas: NovaFaixaRegua[], agora: Date): Promise<FaixaReguaRow[]> {
    const criadas: FaixaReguaRow[] = [];
    for (const nova of novas) {
      for (const l of this.linhas) {
        if (l.tabela === nova.tabela && l.custo_min === nova.custo_min && l.vigencia_fim == null) {
          l.vigencia_fim = agora;
          l.encerrado_por = nova.autor;
          l.encerrado_em = agora;
        }
      }
      const criada: FaixaReguaRow = {
        id: ++this.seq,
        tabela: nova.tabela,
        custo_min: nova.custo_min,
        custo_max: nova.custo_max,
        markup_min_pct: nova.markup_min_pct,
        markup_max_pct: nova.markup_max_pct,
        vigencia_inicio: agora,
        vigencia_fim: null,
        criado_por: nova.autor,
        criado_em: agora,
        encerrado_por: null,
        encerrado_em: null,
        motivo: nova.motivo,
      };
      this.linhas.push(criada);
      criadas.push({ ...criada });
    }
    return criadas;
  }
}

function criar(repo: FakeReguaRepository = new FakeReguaRepository()): ReguaService {
  return new ReguaService(repo as unknown as ReguaRepository);
}

describe('ReguaService — corredor por faixa de custo (US-040 / T-024)', () => {
  it('AC1 — dado custo composto de R$ 12,00, quando calcula o corredor de varejo, então min 200% e max 1000%', async () => {
    const corredor = await criar().corredor('varejo', 12);

    expect({
      markup_min_pct: corredor.markup_min_pct,
      markup_max_pct: corredor.markup_max_pct,
      preco_minimo: corredor.preco_minimo,
      preco_maximo: corredor.preco_maximo,
    }).toEqual({ markup_min_pct: 200, markup_max_pct: 1000, preco_minimo: 36, preco_maximo: 132 });
  });

  it('dado markup de 100%, quando calcula o preço mínimo, então preço = 2× custo (não preço = custo)', async () => {
    // Semântica declarada na US-040: 100% é markup SOBRE o custo.
    const corredor = await criar().corredor('varejo', 200); // faixa 99,01–400,01 → 100%–110%

    expect({ minimo: corredor.preco_minimo, maximo: corredor.preco_maximo }).toEqual({
      minimo: 400,
      maximo: 420,
    });
  });

  it('AC3 — dados os sete custos do cenário, quando determina a faixa, então TODOS recebem faixa de varejo', async () => {
    const service = criar();
    const custos = [0.5, 15.0, 15.01, 40.0, 99.0, 400.0, 401.0];

    const faixas = await Promise.all(
      custos.map(async (c) => {
        const corredor = await service.corredor('varejo', c);
        return `${c} -> [${corredor.faixa_custo_min}, ${corredor.faixa_custo_max ?? '∞'}) ${corredor.markup_min_pct}-${corredor.markup_max_pct}%`;
      }),
    );

    expect(faixas).toEqual([
      '0.5 -> [0, 15.01) 200-1000%',
      '15 -> [0, 15.01) 200-1000%',
      '15.01 -> [15.01, 40.01) 150-200%',
      '40 -> [15.01, 40.01) 150-200%',
      '99 -> [40.01, 99.01) 130-150%',
      '400 -> [99.01, 400.01) 100-110%',
      '401 -> [400.01, ∞) 100-150%',
    ]);
  });

  it('dado custo de R$ 400,01, quando determina a faixa, então cai na regra de "acima de 400" — a mesma de um custo de R$ 500,00', async () => {
    const service = criar();

    const [em40001, em500, em40000] = await Promise.all([
      service.corredor('varejo', 400.01),
      service.corredor('varejo', 500),
      service.corredor('varejo', 400.0),
    ]);

    expect({
      faixa_de_400_01: em40001.faixa_id,
      faixa_de_500: em500.faixa_id,
      faixa_de_400_00: em40000.faixa_id,
      markup_400_01: em40001.markup_max_pct,
      markup_400_00: em40000.markup_max_pct,
    }).toEqual({
      faixa_de_400_01: em500.faixa_id,
      faixa_de_500: em500.faixa_id,
      faixa_de_400_00: em40000.faixa_id,
      // não-monotonicidade preservada: 110% na faixa anterior, 150% na última
      markup_400_01: 150,
      markup_400_00: 110,
    });
  });

  it('dado custo com mais de duas casas (R$ 15,004 — divisão por quantidade), quando determina a faixa, então NÃO fica sem faixa', async () => {
    // Intervalo fechado com passo de centavo (0–15,00 / 15,01–40,00) deixaria buraco aqui.
    const corredor = await criar().corredor('varejo', 15.004);

    expect({ min: corredor.faixa_custo_min, max: corredor.faixa_custo_max }).toEqual({
      min: 0,
      max: 15.01,
    });
  });

  it('dado custo zero, quando pede o corredor, então recusa com BadRequest (sem piso não há trava)', async () => {
    await expect(criar().corredor('varejo', 0)).rejects.toBeInstanceOf(BadRequestException);
  });

  it('dada tabela inexistente, quando pede o corredor, então recusa com BadRequest', async () => {
    await expect(criar().corredor('atacadao', 12)).rejects.toBeInstanceOf(BadRequestException);
  });
});

describe('ReguaService — piso inviolável nas três tabelas (US-040 / T-024)', () => {
  it.each<[TabelaPreco, number, number]>([
    ['varejo', 12, 36], // mínimo da faixa 0–15,01 → 200%
    ['atacado', 12, 20.04], // 67%
    ['atacado_especial', 12, 18], // 50%
  ])(
    'AC2 — dada a tabela %s com custo 12, quando o preço fica um centavo abaixo do piso, então recusa com motivo explícito',
    async (tabela, custo, pisoEsperado) => {
      const service = criar();

      const abaixo = await service.avaliarPreco(tabela, custo, pisoEsperado - 0.01);

      expect({
        aceito: abaixo.aceito,
        preco_minimo: abaixo.preco_minimo,
        motivo_menciona_piso: Boolean(abaixo.motivo?.includes('piso é inviolável')),
      }).toEqual({ aceito: false, preco_minimo: pisoEsperado, motivo_menciona_piso: true });
    },
  );

  it.each<[TabelaPreco, number, number]>([
    ['varejo', 12, 36],
    ['atacado', 12, 20.04],
    ['atacado_especial', 12, 18],
  ])(
    'dada a tabela %s, quando o preço é exatamente o piso, então aceita sem motivo de recusa',
    async (tabela, custo, piso) => {
      const avaliacao = await criar().avaliarPreco(tabela, custo, piso);

      expect({ aceito: avaliacao.aceito, motivo: avaliacao.motivo }).toEqual({
        aceito: true,
        motivo: null,
      });
    },
  );

  it('dado preço acima do teto da faixa, quando avalia, então aceita com aviso — só o limite inferior é inviolável', async () => {
    const avaliacao = await criar().avaliarPreco('varejo', 12, 200); // teto = R$ 132,00

    expect({
      aceito: avaliacao.aceito,
      acima_do_teto: avaliacao.acima_do_teto,
      tem_aviso: Boolean(avaliacao.aviso),
      motivo: avaliacao.motivo,
    }).toEqual({ aceito: true, acima_do_teto: true, tem_aviso: true, motivo: null });
  });

  it('dado o piso arredondado, quando o preço mínimo tem fração de centavo, então arredonda PARA CIMA (nunca fura o piso)', async () => {
    // custo 0,01 na tabela atacado_especial → 0,015 exato; para baixo aceitaria 0,01.
    const corredor = await criar().corredor('atacado_especial', 0.01);
    const noPisoTeorico = await criar().avaliarPreco('atacado_especial', 0.01, 0.01);

    expect({ minimo: corredor.preco_minimo, aceita_0_01: noPisoTeorico.aceito }).toEqual({
      minimo: 0.02,
      aceita_0_01: false,
    });
  });

  it('DoD — teste de propriedade: varrendo o domínio de custo, nenhum preço abaixo do piso é aceito e nenhum custo fica sem faixa', async () => {
    const service = criar();
    const tabelas: TabelaPreco[] = ['varejo', 'atacado', 'atacado_especial'];

    // Varredura do domínio + vizinhança de cada fronteira declarada (onde um erro de
    // inclusividade se esconderia).
    const custos = new Set<number>();
    for (let c = 0.01; c <= 600; c = Math.round((c + 0.37) * 100) / 100) custos.add(c);
    for (const b of [0.01, 15, 15.01, 40, 40.01, 99, 99.01, 400, 400.01]) {
      for (const d of [-0.01, -0.001, 0, 0.001, 0.01]) {
        const v = Math.round((b + d) * 1000) / 1000;
        if (v > 0) custos.add(v);
      }
    }

    const violacoes: string[] = [];
    for (const tabela of tabelas) {
      for (const custo of custos) {
        let corredor;
        try {
          corredor = await service.corredor(tabela, custo);
        } catch (err) {
          violacoes.push(`sem faixa: ${tabela} custo=${custo} (${(err as Error).message})`);
          continue;
        }

        const umCentavoAbaixo = Math.round((corredor.preco_minimo - 0.01) * 100) / 100;
        if (umCentavoAbaixo >= 0) {
          const recusa = await service.avaliarPreco(tabela, custo, umCentavoAbaixo);
          if (recusa.aceito) {
            violacoes.push(
              `aceitou abaixo do piso: ${tabela} custo=${custo} preco=${umCentavoAbaixo} piso=${corredor.preco_minimo}`,
            );
          }
          if (!recusa.motivo) {
            violacoes.push(`recusou sem motivo: ${tabela} custo=${custo}`);
          }
        }

        const noPiso = await service.avaliarPreco(tabela, custo, corredor.preco_minimo);
        if (!noPiso.aceito) {
          violacoes.push(`recusou o próprio piso: ${tabela} custo=${custo} piso=${corredor.preco_minimo}`);
        }
      }
    }

    expect({ custos_varridos: custos.size, violacoes }).toEqual({
      custos_varridos: custos.size,
      violacoes: [],
    });
  }, 60_000);
});

describe('ReguaService — régua em dado, versionada e auditável (US-040 / T-024)', () => {
  it('AC — dado um limite de faixa alterado na TABELA, quando avalia de novo, então o corredor muda sem rebuild nem restart', async () => {
    const repo = new FakeReguaRepository();
    const service = criar(repo);

    const antes = await service.corredor('varejo', 12);
    await service.alterarFaixas(
      [{ tabela: 'varejo', custo_min: 0, custo_max: 15.01, markup_min_pct: 220, markup_max_pct: 1000 }],
      'u-1042',
    );
    const depois = await service.corredor('varejo', 12);

    expect({ antes: antes.markup_min_pct, depois: depois.markup_min_pct, piso: depois.preco_minimo }).toEqual(
      { antes: 200, depois: 220, piso: 38.4 },
    );
  });

  it('dada alteração feita FORA da aplicação, quando o cache é invalidado, então a próxima avaliação já lê o novo limite', async () => {
    const repo = new FakeReguaRepository();
    const service = criar(repo);
    await service.corredor('varejo', 12); // aquece o cache

    repo.linhas[0].markup_min_pct = 250; // UPDATE manual no banco
    const comCache = await service.corredor('varejo', 12);
    service.invalidarCache();
    const semCache = await service.corredor('varejo', 12);

    expect({ comCache: comCache.markup_min_pct, semCache: semCache.markup_min_pct }).toEqual({
      comCache: 200,
      semCache: 250,
    });
  });

  it('DoD — dada uma faixa alterada, quando consulta o histórico, então mostra quem alterou e quando, sem perder a versão anterior', async () => {
    const repo = new FakeReguaRepository();
    const service = criar(repo);

    await service.alterarFaixas(
      [
        {
          tabela: 'varejo',
          custo_min: 0,
          custo_max: 15.01,
          markup_min_pct: 220,
          markup_max_pct: 1000,
          motivo: 'reajuste do mix de entrada',
        },
      ],
      'u-1042',
    );

    const historico = await service.historico('varejo');
    const nova = historico.find((f) => f.vigencia_fim == null && f.custo_min === 0);
    const antiga = historico.find((f) => f.vigencia_fim != null && f.custo_min === 0);

    expect({
      nova: { por: nova?.criado_por, markup: nova?.markup_min_pct, motivo: nova?.motivo },
      antiga: {
        encerrada_por: antiga?.encerrado_por,
        markup: antiga?.markup_min_pct,
        encerrada: Boolean(antiga?.encerrado_em),
      },
    }).toEqual({
      nova: { por: 'u-1042', markup: 220, motivo: 'reajuste do mix de entrada' },
      antiga: { encerrada_por: 'u-1042', markup: 200, encerrada: true },
    });
  });

  it('dada alteração sem autor (sem x-user-id e sem alterado_por), quando grava, então recusa — faixa sem autor não é auditável', async () => {
    const service = criar();

    await expect(
      service.alterarFaixas([
        { tabela: 'varejo', custo_min: 0, custo_max: 15.01, markup_min_pct: 220, markup_max_pct: 1000 },
      ]),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('dada alteração que deixaria BURACO de cobertura, quando grava, então recusa com 400 e nada é alterado', async () => {
    const repo = new FakeReguaRepository();
    const service = criar(repo);

    await expect(
      service.alterarFaixas(
        // encurta a primeira faixa sem mexer na vizinha → buraco entre 10 e 15,01
        [{ tabela: 'varejo', custo_min: 0, custo_max: 10, markup_min_pct: 200, markup_max_pct: 1000 }],
        'u-1042',
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repo.linhas.filter((l) => l.vigencia_fim != null)).toHaveLength(0);
  });

  it('dada uma faixa DIVIDIDA em duas no mesmo lote, quando grava, então a cobertura continua total', async () => {
    const repo = new FakeReguaRepository();
    const service = criar(repo);

    await service.alterarFaixas(
      [
        { tabela: 'varejo', custo_min: 0, custo_max: 8, markup_min_pct: 250, markup_max_pct: 1000 },
        { tabela: 'varejo', custo_min: 8, custo_max: 15.01, markup_min_pct: 200, markup_max_pct: 900 },
      ],
      'u-1042',
    );

    const [em5, em10] = await Promise.all([
      service.corredor('varejo', 5),
      service.corredor('varejo', 10),
    ]);
    expect([em5.markup_min_pct, em10.markup_min_pct]).toEqual([250, 200]);
  });

  it('dado markup acima de 9999,9999%, quando grava, então recusa (precisão Decimal(8,4) das colunas de markup)', async () => {
    // Achado da T-023: com_fifo_completo.margem_pct é Decimal(8,4) — precisão total 8.
    await expect(
      criar().alterarFaixas(
        [{ tabela: 'varejo', custo_min: 0, custo_max: 15.01, markup_min_pct: 200, markup_max_pct: 10000 }],
        'u-1042',
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('dado markup_max menor que markup_min na MESMA faixa, quando grava, então recusa', async () => {
    await expect(
      criar().alterarFaixas(
        [{ tabela: 'varejo', custo_min: 0, custo_max: 15.01, markup_min_pct: 300, markup_max_pct: 200 }],
        'u-1042',
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('dada a régua não-monotônica entre as duas últimas faixas do varejo, quando carrega, então NÃO reclama — a não-monotonicidade é intencional', async () => {
    const faixas = await criar().faixasVigentes();

    const varejo = faixas.filter((f) => f.tabela === 'varejo');
    expect(varejo.map((f) => f.markup_max_pct)).toEqual([1000, 200, 150, 110, 150]);
  });
});

describe('ReguaService — régua não configurada ou inconsistente (US-040 / T-024)', () => {
  it('dada a tabela de faixas VAZIA (DDL ainda não aplicada), quando avalia, então falha alto apontando o .sql — nunca cai em régua embutida', async () => {
    const service = criar(new FakeReguaRepository([]));

    await expect(service.corredor('varejo', 12)).rejects.toBeInstanceOf(ServiceUnavailableException);
    await expect(service.corredor('varejo', 12)).rejects.toThrow(
      /2026-07-30_precificacao_faixas\.sql/,
    );
  });

  it('dada régua com buraco entre faixas, quando carrega, então recusa precificar em vez de servir custo sem piso', async () => {
    const service = criar(
      new FakeReguaRepository([
        ['varejo', 0, 15.01, 200, 1000],
        ['varejo', 20, null, 150, 200], // buraco 15,01–20,00
        ['atacado', 0, null, 67, 67],
        ['atacado_especial', 0, null, 50, 50],
      ]),
    );

    await expect(service.corredor('varejo', 12)).rejects.toThrow(/buraco/);
  });

  it('dada régua que não começa em R$ 0,00, quando carrega, então recusa (custo abaixo do início ficaria sem piso)', async () => {
    const service = criar(
      new FakeReguaRepository([
        ['varejo', 1, null, 200, 1000],
        ['atacado', 0, null, 67, 67],
        ['atacado_especial', 0, null, 50, 50],
      ]),
    );

    await expect(service.corredor('varejo', 12)).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('dada régua cuja última faixa é fechada, quando carrega, então recusa (custo acima do fim ficaria sem faixa)', async () => {
    const service = criar(
      new FakeReguaRepository([
        ['varejo', 0, 15.01, 200, 1000],
        ['varejo', 15.01, 400, 150, 200],
        ['atacado', 0, null, 67, 67],
        ['atacado_especial', 0, null, 50, 50],
      ]),
    );

    await expect(service.corredor('varejo', 12)).rejects.toThrow(/última faixa/);
  });
});

describe('ReguaRepository — versionamento append-only (US-040 / T-024)', () => {
  function fakePrisma() {
    const updateMany = jest.fn().mockResolvedValue({ count: 1 });
    const create = jest
      .fn()
      .mockImplementation(async ({ data }: { data: Record<string, unknown> }) => ({ id: 99, ...data }));
    const findMany = jest.fn().mockResolvedValue([]);
    const delegate = { updateMany, create, findMany };
    const prisma = {
      com_precificacao_faixa: delegate,
      $transaction: async (fn: (tx: unknown) => Promise<unknown>) =>
        fn({ com_precificacao_faixa: delegate }),
    };
    return { repo: new ReguaRepository(prisma as never), updateMany, create, findMany };
  }

  const NOVA: NovaFaixaRegua = {
    tabela: 'varejo',
    custo_min: 0,
    custo_max: 15.01,
    markup_min_pct: 220,
    markup_max_pct: 1000,
    autor: 'u-1042',
    motivo: 'reajuste',
  };

  it('dado uma faixa substituída, quando grava, então ENCERRA a vigente em vez de sobrescrever seus limites', async () => {
    const { repo, updateMany } = fakePrisma();
    const agora = new Date('2026-07-30T12:00:00.000Z');

    await repo.substituirFaixas([NOVA], agora);

    expect(updateMany.mock.calls[0][0]).toEqual({
      where: { tabela_preco: 'varejo', custo_min: 0, vigencia_fim: null },
      data: { vigencia_fim: agora, encerrado_por: 'u-1042', encerrado_em: agora },
    });
  });

  it('dado o encerramento e a nova vigência, quando grava, então ambos usam o MESMO instante (sem vão sem faixa)', async () => {
    const { repo, updateMany, create } = fakePrisma();
    const agora = new Date('2026-07-30T12:00:00.000Z');

    await repo.substituirFaixas([NOVA], agora);

    expect(create.mock.calls[0][0].data.vigencia_inicio).toEqual(
      updateMany.mock.calls[0][0].data.vigencia_fim,
    );
  });

  it('dado um lote de duas faixas, quando grava, então as duas passam pela MESMA transação', async () => {
    const { repo, create } = fakePrisma();

    await repo.substituirFaixas([NOVA, { ...NOVA, custo_min: 15.01 }], new Date());

    expect(create).toHaveBeenCalledTimes(2);
  });

  it('quando lê as faixas vigentes, então filtra vigencia_fim nula e ordena por (tabela, custo)', async () => {
    const { repo, findMany } = fakePrisma();

    await repo.faixasVigentes();

    expect(findMany.mock.calls[0][0]).toEqual({
      where: { vigencia_fim: null },
      orderBy: [{ tabela_preco: 'asc' }, { custo_min: 'asc' }],
    });
  });

  it.skip('pendente-provisao — régua lida do Postgres real após aplicação MANUAL de sql/2026-07-30_precificacao_faixas.sql', () => {
    // TESTING.md#gates: a perna de integração contra banco real depende da aplicação
    // manual da DDL pelo responsável (dependência externa declarada na T-024).
  });
});
