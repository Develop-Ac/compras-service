/* =============================================================================
   RÉGUA DA CONFERÊNCIA DE PREÇOS — cálculo puro (sem I/O).
   -----------------------------------------------------------------------------
   Recebe o item como a erp-firebird-api devolve (GET /erp/nf-entrada/:nfe/precos)
   e as faixas vigentes de com_precificacao_faixa; devolve custo base, piso e
   semáforo de cada tabela de preço. Regras em docs/conferencia-precos.md.

   Nulo NUNCA vira zero: custo ausente não gera piso (margem sobre zero passaria
   qualquer preço) e preço ausente é "abaixo" (não há preço de venda no ERP).
   Toda comparação de dinheiro é feita em centavos inteiros: 789,90 / 615 em
   ponto flutuante não é confiável na casa do limite.
   ============================================================================= */

export type Semaforo = 'ok' | 'abaixo' | 'reajuste' | 'sem_custo';
export type OrigemCusto = 'nf' | 'medio' | 'ausente';

/** Chaves das tabelas na resposta × valor de `tabela_preco` em com_precificacao_faixa. */
export const TABELAS = {
  varejo: 'varejo',
  atacado_esp: 'atacado_especial',
  atacado: 'atacado',
} as const;
export type Tabela = keyof typeof TABELAS;

export interface Faixa {
  tabela_preco: string;
  custo_min: number;
  /** null = sem limite superior. */
  custo_max: number | null;
  markup_min_pct: number;
}

/** Item da rota de preços da erp-firebird-api (números ou null). */
export interface ItemErp {
  pro_codigo: number;
  pro_descricao: string | null;
  quantidade: number | null;
  estoque_disponivel?: number | null;
  saldo_anterior: number | null;
  saldo_anterior_origem?: string | null;
  preco_custo: number | null;
  custo_medio: number | null;
  preco_varejo: number | null;
  preco_atacado_esp: number | null;
  preco_atacado: number | null;
  preco_ant_varejo: number | null;
  preco_ant_atacado_esp: number | null;
  preco_ant_atacado: number | null;
}

export interface TabelaCalculada {
  preco: number | null;
  anterior: number | null;
  piso: number | null;
  markup_min_pct: number | null;
  semaforo: Semaforo;
}

export interface ItemCalculado {
  pro_codigo: number;
  pro_descricao: string | null;
  quantidade: number | null;
  saldo_anterior: number | null;
  preco_custo: number | null;
  custo_medio: number | null;
  custo_base: number | null;
  custo_base_origem: OrigemCusto;
  tabelas: Record<Tabela, TabelaCalculada>;
  semaforo_item: Semaforo;
  justificativa: string | null;
}

/** Ordem de gravidade do semáforo do item: o pior das três tabelas vence. */
const GRAVIDADE: Record<Semaforo, number> = { abaixo: 3, sem_custo: 2, reajuste: 1, ok: 0 };

/** Acima disto (preço / anterior − 1) o item pede atenção como reajuste. */
const REAJUSTE_PCT = 15;

const centavos = (v: number) => Math.round(v * 100);
const num = (v: unknown): number | null =>
  v === null || v === undefined || v === '' || !Number.isFinite(Number(v)) ? null : Number(v);

/** Menor valor terminado em ,90 que seja >= x (572,04 → 572,90; 572,95 → 573,90). */
export function arredondaParaCima90(x: number): number {
  // −1e-6 absorve o ruído do ponto flutuante (572,90 × 100 = 57289,99999…).
  const c = Math.ceil(x * 100 - 1e-6);
  let alvo = Math.floor(c / 100) * 100 + 90;
  if (alvo < c) alvo += 100;
  return alvo / 100;
}

/** Faixa da tabela que contém o custo: custo_min inclusivo, custo_max exclusivo. */
export function faixaPara(tabela: Tabela, custo: number, faixas: Faixa[]): Faixa | null {
  return (
    faixas.find(
      (f) =>
        f.tabela_preco === TABELAS[tabela] &&
        custo >= f.custo_min &&
        (f.custo_max === null || custo < f.custo_max),
    ) ?? null
  );
}

export function piorSemaforo(lista: Semaforo[]): Semaforo {
  return lista.reduce((pior, s) => (GRAVIDADE[s] > GRAVIDADE[pior] ? s : pior), 'ok' as Semaforo);
}

/**
 * Semáforo de uma tabela. Sem tolerância no piso: um centavo abaixo é "abaixo".
 * Anterior zero ou nulo é "sem histórico", nunca reajuste (nem queda de 100%).
 */
export function semaforoTabela(preco: number | null, anterior: number | null, piso: number | null): Semaforo {
  if (piso === null) return 'sem_custo';
  if (preco === null || centavos(preco) < centavos(piso)) return 'abaixo';
  if (anterior !== null && anterior > 0 && centavos(preco) * 100 > centavos(anterior) * (100 + REAJUSTE_PCT)) {
    return 'reajuste';
  }
  return 'ok';
}

/** Custo que a régua usa: médio quando já havia estoque antes desta NF, senão o da NF. */
export function custoBase(item: Pick<ItemErp, 'saldo_anterior' | 'preco_custo' | 'custo_medio'>): {
  custo_base: number | null;
  custo_base_origem: OrigemCusto;
} {
  const saldo = num(item.saldo_anterior);
  const usarMedio = saldo !== null && saldo > 0;
  const valor = num(usarMedio ? item.custo_medio : item.preco_custo);
  if (valor === null || valor <= 0) return { custo_base: null, custo_base_origem: 'ausente' };
  return { custo_base: valor, custo_base_origem: usarMedio ? 'medio' : 'nf' };
}

/** Calcula custo base, pisos e semáforos de um item contra as faixas vigentes. */
export function calcularItem(item: ItemErp, faixas: Faixa[]): ItemCalculado {
  const { custo_base, custo_base_origem } = custoBase(item);

  const tabelas = {} as Record<Tabela, TabelaCalculada>;
  for (const t of Object.keys(TABELAS) as Tabela[]) {
    const preco = num(item[`preco_${t}`]);
    const anterior = num(item[`preco_ant_${t}`]);
    const faixa = custo_base === null ? null : faixaPara(t, custo_base, faixas);
    const piso = faixa ? arredondaParaCima90(custo_base! * (1 + faixa.markup_min_pct / 100)) : null;
    tabelas[t] = {
      preco,
      anterior,
      piso,
      markup_min_pct: faixa?.markup_min_pct ?? null,
      semaforo: semaforoTabela(preco, anterior, piso),
    };
  }

  return {
    pro_codigo: Number(item.pro_codigo),
    pro_descricao: item.pro_descricao ?? null,
    quantidade: num(item.quantidade),
    saldo_anterior: num(item.saldo_anterior),
    preco_custo: num(item.preco_custo),
    custo_medio: num(item.custo_medio),
    custo_base,
    custo_base_origem,
    tabelas,
    semaforo_item: piorSemaforo(Object.values(tabelas).map((x) => x.semaforo)),
    justificativa: null,
  };
}

/** Contagens gravadas no cabeçalho da fila. */
export function contagens(itens: Pick<ItemCalculado, 'semaforo_item'>[]) {
  return {
    itens_total: itens.length,
    itens_abaixo: itens.filter((i) => i.semaforo_item === 'abaixo').length,
    itens_sem_custo: itens.filter((i) => i.semaforo_item === 'sem_custo').length,
  };
}
