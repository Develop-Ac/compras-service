import { Faixa, ItemErp, arredondaParaCima90, calcularItem, custoBase } from './regua';

/** Régua vigente após o SQL de 2026-10-01 (atacado especial 53,8%). */
const FAIXAS: Faixa[] = [
  { tabela_preco: 'varejo', custo_min: 0, custo_max: 15, markup_min_pct: 200 },
  { tabela_preco: 'varejo', custo_min: 15, custo_max: 40, markup_min_pct: 150 },
  { tabela_preco: 'varejo', custo_min: 40, custo_max: 99, markup_min_pct: 130 },
  { tabela_preco: 'varejo', custo_min: 99, custo_max: null, markup_min_pct: 100 },
  { tabela_preco: 'atacado_especial', custo_min: 0, custo_max: null, markup_min_pct: 53.8 },
  { tabela_preco: 'atacado', custo_min: 0, custo_max: null, markup_min_pct: 67 },
];

const item = (p: Partial<ItemErp>): ItemErp => ({
  pro_codigo: 1,
  pro_descricao: 'PARA-BRISA',
  quantidade: 1,
  saldo_anterior: 0,
  preco_custo: 371.94,
  custo_medio: null,
  preco_varejo: 999.9,
  preco_atacado_esp: 999.9,
  preco_atacado: 999.9,
  preco_ant_varejo: null,
  preco_ant_atacado_esp: null,
  preco_ant_atacado: null,
  ...p,
});

describe('regua da conferência de preços', () => {
  it('arredondaParaCima90', () => {
    expect(arredondaParaCima90(572.04)).toBe(572.9);
    expect(arredondaParaCima90(572.95)).toBe(573.9);
    expect(arredondaParaCima90(572.9)).toBe(572.9);
  });

  it('custo base: médio com saldo anterior, custo da NF sem saldo', () => {
    expect(custoBase({ saldo_anterior: 3, custo_medio: 100, preco_custo: 120 })).toEqual({
      custo_base: 100,
      custo_base_origem: 'medio',
    });
    expect(custoBase({ saldo_anterior: 0, custo_medio: 100, preco_custo: 120 })).toEqual({
      custo_base: 120,
      custo_base_origem: 'nf',
    });
    expect(custoBase({ saldo_anterior: null, custo_medio: 100, preco_custo: 120 }).custo_base_origem).toBe('nf');
  });

  it("'abaixo' sem tolerância: custo 371,94 × 1,538 → piso 572,90; 559,90 é abaixo", () => {
    const r = calcularItem(item({ preco_atacado_esp: 559.9 }), FAIXAS);
    expect(r.tabelas.atacado_esp.piso).toBe(572.9);
    expect(r.tabelas.atacado_esp.semaforo).toBe('abaixo');
    expect(r.semaforo_item).toBe('abaixo');
  });

  it('um centavo abaixo do piso já é abaixo; no piso é ok', () => {
    expect(calcularItem(item({ preco_atacado_esp: 572.89 }), FAIXAS).tabelas.atacado_esp.semaforo).toBe('abaixo');
    expect(calcularItem(item({ preco_atacado_esp: 572.9 }), FAIXAS).tabelas.atacado_esp.semaforo).toBe('ok');
  });

  it("'reajuste' quando o preço subiu mais de 15% sobre o anterior (615 → 789,90)", () => {
    const r = calcularItem(item({ preco_atacado_esp: 789.9, preco_ant_atacado_esp: 615 }), FAIXAS);
    expect(r.tabelas.atacado_esp.semaforo).toBe('reajuste');
    expect(r.semaforo_item).toBe('reajuste');
  });

  it('anterior zero não é reajuste (sem histórico)', () => {
    const r = calcularItem(item({ preco_atacado_esp: 789.9, preco_ant_atacado_esp: 0 }), FAIXAS);
    expect(r.tabelas.atacado_esp.semaforo).toBe('ok');
  });

  it('custo nulo → sem_custo nas três tabelas, sem piso', () => {
    const r = calcularItem(item({ preco_custo: null }), FAIXAS);
    expect(r.custo_base).toBeNull();
    expect(r.custo_base_origem).toBe('ausente');
    for (const t of Object.values(r.tabelas)) {
      expect(t.piso).toBeNull();
      expect(t.semaforo).toBe('sem_custo');
    }
    expect(r.semaforo_item).toBe('sem_custo');
  });

  it('preço nulo é abaixo; abaixo vence sem_custo de tabela sem faixa', () => {
    const semAtacado = FAIXAS.filter((f) => f.tabela_preco !== 'atacado');
    const r = calcularItem(item({ preco_varejo: null }), semAtacado);
    expect(r.tabelas.varejo.semaforo).toBe('abaixo');
    expect(r.tabelas.atacado.semaforo).toBe('sem_custo');
    expect(r.semaforo_item).toBe('abaixo');
  });
});
