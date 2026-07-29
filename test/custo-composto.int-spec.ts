import { NotFoundException } from '@nestjs/common';
import { CustoService } from '../src/compras/precificacao/custo.service';

/**
 * US-037 / T-020 — custo composto do item da NF.
 *
 * Cobre os três cenários exigidos pelo DoD (frete próprio, frete do fornecedor e
 * nota sem CT-e) contra um duplo de PrismaService: a regra de composição e o
 * rateio são determinísticos e não dependem do Postgres. A perna que precisa de
 * banco real (amostra ≥ 10 itens / ≥ 2 notas conferida contra o lançamento do ERP)
 * fica marcada como `pendente-provisao`, conforme TESTING.md#gates.
 */

const CHAVE = '35260561736732003588550010001915931033927230';
const CHAVE_2 = '35260561736732003588550010001915931033927231';
const PEDIDO_ID = 'ped-001';

/**
 * NF com duas linhas de R$ 1.000,00 (total de mercadoria R$ 2.000,00):
 *  - ABC1: ICMS ST destacado R$ 170,00 (R$ 17,00/un) e IPI R$ 50,00 (R$ 5,00/un);
 *  - ABC2: sem ST e sem IPI.
 */
const XML_NF = `<?xml version="1.0" encoding="UTF-8"?>
<nfeProc><NFe><infNFe>
  <det nItem="1">
    <prod>
      <cProd>ABC1</cProd><uCom>UN</uCom><qCom>10.0000</qCom><vUnCom>100.00</vUnCom>
      <vProd>1000.00</vProd><uTrib>UN</uTrib><qTrib>10.0000</qTrib><vUnTrib>100.00</vUnTrib>
      <vDesc>0.00</vDesc><vOutro>0.00</vOutro>
    </prod>
    <imposto>
      <ICMS><ICMS10><vICMSST>170.00</vICMSST></ICMS10></ICMS>
      <IPI><IPITrib><vIPI>50.00</vIPI></IPITrib></IPI>
    </imposto>
  </det>
  <det nItem="2">
    <prod>
      <cProd>ABC2</cProd><uCom>UN</uCom><qCom>10.0000</qCom><vUnCom>100.00</vUnCom>
      <vProd>1000.00</vProd><uTrib>UN</uTrib><qTrib>10.0000</qTrib><vUnTrib>100.00</vUnTrib>
      <vDesc>0.00</vDesc><vOutro>0.00</vOutro>
    </prod>
    <imposto><ICMS><ICMS00/></ICMS></imposto>
  </det>
</infNFe></NFe></nfeProc>`;

interface CenarioOpts {
  /** null = nenhum CT-e vinculado à nota. */
  tomadorNos: boolean | null;
  /** Guia de ST paga para a chave. */
  guia?: number;
  /** Quantidade de itens do pedido (para o cenário de volume). */
  qtdItens?: number;
}

/** Contador de round-trips ao banco — prova o AC "nota inteira em 1 requisição". */
let chamadas = 0;

function prismaFake(opts: CenarioOpts) {
  const qtdItens = opts.qtdItens ?? 2;
  const conta = <T>(v: T): Promise<T> => {
    chamadas++;
    return Promise.resolve(v);
  };

  const itens = Array.from({ length: qtdItens }, (_, i) => ({
    pro_codigo: 1001 + i,
    pro_descricao: `PRODUTO ${i + 1}`,
    unidade: 'UN',
    quantidade: 10,
    valor_unitario: 100,
  }));

  const itensVinculo = itens.map((it, i) => ({
    pro_codigo: it.pro_codigo,
    // só os dois primeiros produtos existem no XML; os demais caem no fallback
    cprod_xml: i === 0 ? 'ABC1' : i === 1 ? 'ABC2' : `ABC${i + 1}`,
    quantidade_xml: 10,
    quantidade_alocada: 10,
    vuncom_xml: 100,
  }));

  return {
    com_pedido: {
      findUnique: ({ where }: any) =>
        conta(
          where.id === PEDIDO_ID
            ? { id: PEDIDO_ID, for_codigo: 77, ipi_no_valor: false }
            : null,
        ),
    },
    com_pedido_itens: { findMany: () => conta(itens) },
    com_pedido_nfe_vinculo: {
      findMany: ({ select }: any) =>
        conta(
          select?.itens
            ? [{ chave_nfe: CHAVE, itens: itensVinculo }]
            : [{ pedido_id: PEDIDO_ID }],
        ),
    },
    com_nfe_conciliacao: {
      findMany: () =>
        conta([
          { chave_nfe: CHAVE, xml_completo: XML_NF, valor_guia: 0, valor_total: 2000 },
        ]),
    },
    com_pagamento_guia: {
      findMany: () =>
        conta(opts.guia ? [{ chave_nfe: CHAVE, valor: opts.guia }] : []),
    },
    com_nfe_conciliacao_item: {
      findMany: () =>
        conta([
          { chave_nfe: CHAVE, pro_codigo: '1001', possui_icms_st: true },
          { chave_nfe: CHAVE, pro_codigo: '1002', possui_icms_st: false },
        ]),
    },
    $queryRawUnsafe: () =>
      conta(
        opts.tomadorNos === null
          ? []
          : [
              {
                cte_chave: '35260500000000000000570010000000011000000019',
                tomador_nos: opts.tomadorNos,
                valor_frete: 200,
                chaves_nfe: [CHAVE],
              },
            ],
      ),
  } as any;
}

const servico = (opts: CenarioOpts) => new CustoService(prismaFake(opts));
const somaParcelas = (i: any) =>
  i.parcelas.unitario.valor +
  i.parcelas.icms_st.valor +
  i.parcelas.icms_st_complementar.valor +
  i.parcelas.ipi.valor +
  i.parcelas.frete.valor;

beforeEach(() => {
  chamadas = 0;
});

describe('Custo composto do item da NF (US-037)', () => {
  describe('Dado item com ICMS ST apurado e CT-e por nossa conta', () => {
    it('Então devolve as 5 parcelas discriminadas com a soma batendo no custo composto', async () => {
      const r = await servico({ tomadorNos: true, guia: 40 }).custoDoPedido(PEDIDO_ID);
      const item = r.itens[0];

      // 100 (unitário) + 17 (ST) + 4 (ST complementar) + 5 (IPI) + 10 (frete)
      expect(item.parcelas).toMatchObject({
        unitario: { valor: 100, origem: 'xml' },
        icms_st: { valor: 17, origem: 'xml' },
        icms_st_complementar: { valor: 4, origem: 'guia' },
        ipi: { valor: 5, origem: 'xml' },
        frete: { valor: 10, origem: 'cte' },
      });
      expect(Math.abs(item.custo_unitario - somaParcelas(item))).toBeLessThanOrEqual(0.01);
      expect(item.custo_unitario).toBe(136);
    });

    it('Então a guia de ST só rateia sobre os itens que têm ST', async () => {
      const r = await servico({ tomadorNos: true, guia: 40 }).custoDoPedido(PEDIDO_ID);
      expect(r.itens[1].parcelas.icms_st_complementar.valor).toBe(0);
      expect(r.itens[1].parcelas.icms_st.valor).toBe(0);
    });

    it('Então o frete é rateado pelo valor do item sobre a nota inteira', async () => {
      const r = await servico({ tomadorNos: true }).custoDoPedido(PEDIDO_ID);
      // CT-e de R$ 200 sobre R$ 2.000 de mercadoria => R$ 0,10 por real => R$ 10/un.
      expect(r.itens.map((i) => i.parcelas.frete.valor)).toEqual([10, 10]);
      expect(r.totais.frete).toBe(200);
    });
  });

  describe('Dado item cujo CT-e tem tomador_nos = false', () => {
    it('Então a parcela de frete é R$ 0,00', async () => {
      const r = await servico({ tomadorNos: false }).custoDoPedido(PEDIDO_ID);
      expect(r.itens[0].parcelas.frete.valor).toBe(0);
      expect(r.itens[0].parcelas.frete.detalhe).toContain('tomador_nos = false');
    });

    it('Então o custo não varia em relação ao mesmo item sem CT-e', async () => {
      const comCteDoFornecedor = await servico({ tomadorNos: false }).custoDoPedido(PEDIDO_ID);
      const semCte = await servico({ tomadorNos: null }).custoDoPedido(PEDIDO_ID);
      expect(comCteDoFornecedor.itens.map((i) => i.custo_unitario)).toEqual(
        semCte.itens.map((i) => i.custo_unitario),
      );
      expect(semCte.itens[0].custo_unitario).toBe(122); // 100 + 17 + 5, sem frete
    });
  });

  describe('Dada nota sem CT-e vinculado', () => {
    it('Então a parcela de frete fica zerada e declarada como ausente', async () => {
      const r = await servico({ tomadorNos: null }).custoDoPedido(PEDIDO_ID);
      expect(r.itens[0].parcelas.frete).toMatchObject({ valor: 0, origem: 'ausente' });
    });
  });

  describe('Dada NF com centenas de itens', () => {
    it('Então o custo de todos sai em um número constante de idas ao banco', async () => {
      const r = await servico({ tomadorNos: true, qtdItens: 789 }).custoDoPedido(PEDIDO_ID);
      expect(r.itens).toHaveLength(789);
      // 6 consultas fixas (pedido, itens, vínculos, conciliação, guia, flag ST) + CT-e:
      // não cresce com a quantidade de itens.
      expect(chamadas).toBeLessThanOrEqual(8);
    });
  });

  describe('Dado pedido inexistente', () => {
    it('Então responde 404', async () => {
      await expect(servico({ tomadorNos: null }).custoDoPedido('nao-existe')).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  describe('Dada chave de NF-e inválida', () => {
    it('Então responde 404 sem consultar vínculos', async () => {
      await expect(servico({ tomadorNos: null }).custoDaNota('123')).rejects.toThrow(
        NotFoundException,
      );
      expect(chamadas).toBe(0);
    });
  });

  describe('Dada consulta por chave de NF-e', () => {
    it('Então devolve uma composição por pedido vinculado, filtrada pela chave', async () => {
      const r = await servico({ tomadorNos: true }).custoDaNota(CHAVE_2);
      expect(r).toHaveLength(1);
      expect(r[0].chave_nfe).toBe(CHAVE_2);
      expect(r[0].pedido_id).toBe(PEDIDO_ID);
    });
  });

  // Perna de integração: exige Postgres `intranet` provisionado com notas reais.
  // TESTING.md#gates => `skipped (pendente-provisao)`.
  describe.skip('[pendente-provisao] amostra contra o lançamento real no ERP', () => {
    it('Então o custo composto de ≥ 10 itens em ≥ 2 notas bate com o custo lançado', () => {
      // Requer banco real: selecionar 2 chaves conferidas, comparar
      // custo_unitario × quantidade com o valor lançado no ERP (tolerância R$ 0,01).
      expect(true).toBe(true);
    });
  });
});
