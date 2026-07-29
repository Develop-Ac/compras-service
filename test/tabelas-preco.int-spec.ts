import { BadRequestException, NotFoundException } from '@nestjs/common';
import { TabelasPrecoRepository } from '../src/compras/precificacao/tabelas-preco.repository';
import { TabelasPrecoService } from '../src/compras/precificacao/tabelas-preco.service';

/**
 * US-038 / T-021 — três tabelas de preço vigentes.
 *
 * Cobre os três ACs contra duplos das duas fontes (ERP via OpenQueryService e
 * espelho `com_fifo_completo` via Prisma): a rotulagem, a precedência de origem e
 * o cálculo da idade do espelhamento são determinísticos e não dependem de banco.
 * A perna que exige Postgres + ERP reais (conferir o preco5 de uma amostra contra
 * o cadastro do ERP) fica `pendente-provisao`, conforme TESTING.md#gates.
 */

const PRO = 38883;
const AGORA = new Date('2026-07-29T12:00:00.000Z');
const ESPELHO_EM = new Date('2026-07-29T06:00:00.000Z'); // 6h de idade

function fakeRepo(over: Partial<TabelasPrecoRepository> = {}): TabelasPrecoRepository {
  const base = {
    buscarNoErp: jest.fn().mockResolvedValue(
      new Map([
        [
          PRO,
          {
            pro_codigo: PRO,
            pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
            varejo: 189.9,
            atacado_especial: 165.5,
            atacado: 142.3,
          },
        ],
      ]),
    ),
    buscarNoEspelho: jest.fn().mockResolvedValue(
      new Map([
        [
          PRO,
          {
            pro_codigo: PRO,
            pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
            varejo: 189.9,
            atacado_especial: 165.5,
            atualizado_em: ESPELHO_EM,
          },
        ],
      ]),
    ),
    ...over,
  };
  return base as unknown as TabelasPrecoRepository;
}

describe('TabelasPrecoService (US-038 / T-021)', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(AGORA);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it('AC1 — dado um produto com as três tabelas no ERP, quando consultado, então devolve 3 preços rotulados e nenhum nulo', async () => {
    const service = new TabelasPrecoService(fakeRepo());

    const dto = await service.precosVigentes(PRO);

    expect(
      dto.precos.map((p) => ({ tabela: p.tabela, rotulo: p.rotulo, valor: p.valor, origem: p.origem })),
    ).toEqual([
      { tabela: 'varejo', rotulo: 'Varejo', valor: 189.9, origem: 'erp' },
      { tabela: 'atacado_especial', rotulo: 'Atacado especial', valor: 165.5, origem: 'erp' },
      { tabela: 'atacado', rotulo: 'Atacado', valor: 142.3, origem: 'erp' },
    ]);
  });

  it('AC2 — dado um produto FORA do espelho com_fifo_completo, quando consultado, então o atacado (preco5) ainda vem do ERP', async () => {
    const service = new TabelasPrecoService(
      fakeRepo({ buscarNoEspelho: jest.fn().mockResolvedValue(new Map()) as never }),
    );

    const dto = await service.precosVigentes(PRO);

    expect(dto.precos.find((p) => p.tabela === 'atacado')).toMatchObject({
      valor: 142.3,
      origem: 'erp',
      campo_erp: 'produtos.preco5',
      campo_espelho: null,
    });
  });

  it('AC3 — dado que o espelho foi materializado há 6h, quando consultado, então devolve a data/hora e a idade do espelhamento', async () => {
    const service = new TabelasPrecoService(fakeRepo());

    const dto = await service.precosVigentes(PRO);

    expect({
      espelho_atualizado_em: dto.espelho_atualizado_em,
      espelho_idade_horas: dto.espelho_idade_horas,
      lido_em: dto.lido_em,
    }).toEqual({
      espelho_atualizado_em: ESPELHO_EM.toISOString(),
      espelho_idade_horas: 6,
      lido_em: AGORA.toISOString(),
    });
  });

  it('dado o ERP indisponível, quando consultado, então degrada para o espelho e o atacado sai ausente — nunca inventado', async () => {
    const service = new TabelasPrecoService(
      fakeRepo({ buscarNoErp: jest.fn().mockResolvedValue(null) as never }),
    );

    const dto = await service.precosVigentes(PRO);

    expect({
      erp_disponivel: dto.erp_disponivel,
      origens: dto.precos.map((p) => p.origem),
      atacado: dto.precos.find((p) => p.tabela === 'atacado')?.valor,
    }).toEqual({
      erp_disponivel: false,
      origens: ['espelho', 'espelho', 'ausente'],
      atacado: null,
    });
  });

  it('dado um produto sem o preco5 cadastrado no ERP, quando consultado, então o atacado sai ausente com valor nulo', async () => {
    const service = new TabelasPrecoService(
      fakeRepo({
        buscarNoErp: jest.fn().mockResolvedValue(
          new Map([
            [PRO, { pro_codigo: PRO, pro_descricao: 'X', varejo: 10, atacado_especial: 9, atacado: null }],
          ]),
        ) as never,
      }),
    );

    const dto = await service.precosVigentes(PRO);

    expect(dto.precos.find((p) => p.tabela === 'atacado')).toMatchObject({
      valor: null,
      origem: 'ausente',
    });
  });

  it('dado um código inexistente em ambas as fontes, quando consultado, então lança NotFound', async () => {
    const service = new TabelasPrecoService(
      fakeRepo({
        buscarNoErp: jest.fn().mockResolvedValue(new Map()) as never,
        buscarNoEspelho: jest.fn().mockResolvedValue(new Map()) as never,
      }),
    );

    await expect(service.precosVigentes(99999)).rejects.toBeInstanceOf(NotFoundException);
  });

  it('dado um pro_codigo não numérico, quando consultado, então lança BadRequest antes de tocar as fontes', async () => {
    const repo = fakeRepo();
    const service = new TabelasPrecoService(repo);

    await expect(service.precosVigentes('abc')).rejects.toBeInstanceOf(BadRequestException);
    expect(repo.buscarNoErp).not.toHaveBeenCalled();
  });

  it('dado um lote de códigos, quando consultado, então faz UMA leitura por fonte para o lote inteiro', async () => {
    const repo = fakeRepo();
    const service = new TabelasPrecoService(repo);

    await service.precosVigentesLote([PRO, '38884', PRO]);

    expect(repo.buscarNoErp).toHaveBeenCalledTimes(1);
    expect(repo.buscarNoErp).toHaveBeenCalledWith([PRO, 38884, PRO]);
  });

  it('dado um lote vazio, quando consultado, então lança BadRequest', async () => {
    const service = new TabelasPrecoService(fakeRepo());

    await expect(service.precosVigentesLote([])).rejects.toBeInstanceOf(BadRequestException);
  });
});

describe('TabelasPrecoRepository — leitura do ERP (US-038 / T-021)', () => {
  function repoComOpenQuery(query: jest.Mock): TabelasPrecoRepository {
    return new TabelasPrecoRepository(
      {} as never,
      { query } as never,
    );
  }

  it('dado códigos válidos, quando lê o ERP, então usa OPENQUERY [CONSULTA] em produtos com preco_venda/preco2/preco5', async () => {
    const query = jest.fn().mockResolvedValue([]);

    await repoComOpenQuery(query).buscarNoErp([PRO]);

    const sqlEnviado = String(query.mock.calls[0][0]).replace(/\s+/g, ' ');
    expect(sqlEnviado).toContain('OPENQUERY(CONSULTA,');
    expect(sqlEnviado).toContain('pro.preco_venda AS PRECO_VAREJO');
    expect(sqlEnviado).toContain('pro.preco2 AS PRECO_ATACADO_ESPECIAL');
    expect(sqlEnviado).toContain('pro.preco5 AS PRECO_ATACADO');
  });

  it('dado códigos com lixo, quando monta a lista literal, então só inteiros sanitizados entram no OPENQUERY', async () => {
    const query = jest.fn().mockResolvedValue([]);

    await repoComOpenQuery(query).buscarNoErp([38883, NaN as number, 38884.7]);

    expect(String(query.mock.calls[0][0])).toContain('IN (38883,38884)');
  });

  it('dado o ERP fora do ar, quando lê, então devolve null em vez de propagar o erro', async () => {
    const query = jest.fn().mockRejectedValue(new Error('ECONNREFUSED'));

    await expect(repoComOpenQuery(query).buscarNoErp([PRO])).resolves.toBeNull();
  });

  it('dado colunas em caixa alta do driver, quando mapeia a linha, então rotula os três preços corretamente', async () => {
    const query = jest.fn().mockResolvedValue([
      {
        PRO_CODIGO: '38883',
        PRO_DESCRICAO: 'PASTILHA',
        PRECO_VAREJO: '189.90',
        PRECO_ATACADO_ESPECIAL: '165.50',
        PRECO_ATACADO: '142.30',
      },
    ]);

    const mapa = await repoComOpenQuery(query).buscarNoErp([PRO]);

    expect(mapa?.get(PRO)).toEqual({
      pro_codigo: PRO,
      pro_descricao: 'PASTILHA',
      varejo: 189.9,
      atacado_especial: 165.5,
      atacado: 142.3,
    });
  });

  it.skip('pendente-provisao — amostra de produtos conferida contra o preco5 do ERP real (exige Postgres + linked server CONSULTA)', () => {
    // TESTING.md#gates: perna de integração real fica pendente até a provisão do
    // Postgres de teste e do acesso ao ERP no ambiente de CI.
  });
});
