import { Test, TestingModule } from '@nestjs/testing';
import { PedidoReferenciaService } from './pedido-referencia.service';
import { PedidoRepository } from './pedido.repository';
import { ErpApiService } from '../../../shared/erp-api/erp-api.service';

describe('PedidoReferenciaService', () => {
  let service: PedidoReferenciaService;

  const repo = { findReferenciasEmLote: jest.fn() };
  const erpApi = { habilitado: true, produtoPorReferencia: jest.fn() };

  const item = (pro_codigo: number, for_codigo = 133, extra: Record<string, any> = {}) => ({
    pro_codigo,
    pro_descricao: `PRODUTO ${pro_codigo}`,
    mar_descricao: 'METAGAL',
    referencia: null,
    unidade: 'UN',
    for_codigo,
    quantidade: 10,
    valor_unitario: 5,
    ...extra,
  });

  const produto = (PRO_CODIGO: number, extra: Record<string, any> = {}) => ({
    PRO_CODIGO,
    PRO_DESCRICAO: `ERP ${PRO_CODIGO} `,
    REF_FORNECEDOR: 'RTUM49',
    REFERENCIA: 'RTUM49',
    UNIDADE: 'PC',
    MAR_DESCRICAO: 'METAGAL',
    INATIVO: 'N',
    ...extra,
  });

  beforeEach(async () => {
    jest.clearAllMocks();
    erpApi.habilitado = true;
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PedidoReferenciaService,
        { provide: PedidoRepository, useValue: repo },
        { provide: ErpApiService, useValue: erpApi },
      ],
    }).compile();
    service = module.get(PedidoReferenciaService);
  });

  it('troca o item quando a referência aponta para outro produto', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['11181_133', 'RTUM49']]));
    erpApi.produtoPorReferencia.mockResolvedValue([produto(39179)]);

    const r = await service.substituirPorReferencia([item(11181)], 3);

    expect(erpApi.produtoPorReferencia).toHaveBeenCalledWith('RTUM49', 3);
    expect(r.itens[0]).toMatchObject({
      pro_codigo: 39179,
      pro_descricao: 'ERP 39179',
      referencia: 'RTUM49',
      unidade: 'PC',
      quantidade: 10,
      valor_unitario: 5,
      for_codigo: 133,
    });
    expect(r.substituicoes).toEqual([
      {
        for_codigo: 133,
        referencia: 'RTUM49',
        de: { pro_codigo: 11181, pro_descricao: 'PRODUTO 11181' },
        para: { pro_codigo: 39179, pro_descricao: 'ERP 39179' },
      },
    ]);
    expect(r.avisos).toEqual([]);
  });

  it('mantém o item quando a API não encontra a referência', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['11181_133', 'XPTO']]));
    erpApi.produtoPorReferencia.mockResolvedValue([]);

    const r = await service.substituirPorReferencia([item(11181)], 3);

    expect(r.itens[0].pro_codigo).toBe(11181);
    expect(r.substituicoes).toEqual([]);
    expect(r.avisos[0].motivo).toMatch(/não encontrada/);
  });

  it('mantém o item quando a API falha', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['11181_133', 'RTUM49']]));
    erpApi.produtoPorReferencia.mockRejectedValue(new Error('timeout'));

    const r = await service.substituirPorReferencia([item(11181)], 3);

    expect(r.itens[0].pro_codigo).toBe(11181);
    expect(r.avisos[0].motivo).toMatch(/indisponível/);
  });

  it('mantém o item quando ele próprio já responde pela referência', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['11181_133', 'RTUM49']]));
    erpApi.produtoPorReferencia.mockResolvedValue([produto(11181), produto(39179)]);

    const r = await service.substituirPorReferencia([item(11181)], 3);

    expect(r.itens[0].pro_codigo).toBe(11181);
    expect(r.substituicoes).toEqual([]);
    expect(r.avisos).toEqual([]);
  });

  it('não troca quando a referência é ambígua, mas desempata por marca', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(
      new Map([
        ['1_133', 'AMB'],
        ['2_133', 'MARCA'],
      ]),
    );
    erpApi.produtoPorReferencia.mockImplementation(async (ref: string) =>
      ref === 'AMB'
        ? [produto(100), produto(101)]
        : [produto(200, { MAR_DESCRICAO: 'OUTRA' }), produto(201, { MAR_DESCRICAO: 'metagal ' })],
    );

    const r = await service.substituirPorReferencia([item(1), item(2)], 3);

    expect(r.itens[0].pro_codigo).toBe(1);
    expect(r.avisos[0].motivo).toMatch(/mais de um produto/);
    expect(r.itens[1].pro_codigo).toBe(201);
  });

  it('ignora produtos inativos', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['1_133', 'REF']]));
    erpApi.produtoPorReferencia.mockResolvedValue([produto(100, { INATIVO: 'S' }), produto(101)]);

    const r = await service.substituirPorReferencia([item(1)], 3);

    expect(r.itens[0].pro_codigo).toBe(101);
  });

  it('não troca para um produto que já é outro item do mesmo fornecedor', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['1_133', 'REF']]));
    erpApi.produtoPorReferencia.mockResolvedValue([produto(2)]);

    const r = await service.substituirPorReferencia([item(1), item(2)], 3);

    expect(r.itens.map((i) => i.pro_codigo)).toEqual([1, 2]);
    expect(r.avisos[0].motivo).toMatch(/já está no pedido/);
  });

  it('não consulta a API sem referência gravada ou com a API desabilitada', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(new Map());
    let r = await service.substituirPorReferencia([item(1)], 3);
    expect(erpApi.produtoPorReferencia).not.toHaveBeenCalled();
    expect(r.itens[0].pro_codigo).toBe(1);

    erpApi.habilitado = false;
    repo.findReferenciasEmLote.mockResolvedValue(new Map([['1_133', 'REF']]));
    r = await service.substituirPorReferencia([item(1)], 3);
    expect(erpApi.produtoPorReferencia).not.toHaveBeenCalled();
    expect(r.itens[0].pro_codigo).toBe(1);
  });

  it('consulta cada referência distinta uma única vez', async () => {
    repo.findReferenciasEmLote.mockResolvedValue(
      new Map([
        ['1_133', 'REF'],
        ['2_133', 'REF'],
      ]),
    );
    erpApi.produtoPorReferencia.mockResolvedValue([produto(1)]);

    await service.substituirPorReferencia([item(1), item(2)], 3);

    expect(erpApi.produtoPorReferencia).toHaveBeenCalledTimes(1);
  });
});
