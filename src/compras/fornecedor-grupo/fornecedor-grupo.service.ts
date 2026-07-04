import { Injectable } from '@nestjs/common';
import { FornecedorGrupoRepository, FornecedorErp, raizCnpj } from './fornecedor-grupo.repository';

export type ItemRefEnriquecivel = {
  for_codigo: number | null;
  pro_codigo: number | string | null;
  ref_fornecedor: string | null;
};

@Injectable()
export class FornecedorGrupoService {
  constructor(private readonly repo: FornecedorGrupoRepository) {}

  // ------------------------------ Consultas/CRUD -----------------------------

  /** Lista os grupos com os dados dos fornecedores (do Stage_Fornecedores). */
  async listarGrupos() {
    const mapa = await this.repo.listarGrupos();
    const todosCodigos = [...mapa.values()].flat().map((m) => m.for_codigo);
    const dados = await this.repo.fornecedoresPorCodigo(todosCodigos);
    const porCodigo = new Map(dados.map((d) => [d.for_codigo, d]));

    return [...mapa.entries()].map(([group_id, membros]) => ({
      group_id,
      membros: membros.map((m) => ({
        ...this.fornecedorView(porCodigo.get(m.for_codigo), m.for_codigo),
        principal: m.principal,
      })),
    }));
  }

  async buscarFornecedores(termo: string): Promise<FornecedorErp[]> {
    return this.repo.buscarFornecedores(termo);
  }

  /** Cadastro completo do fornecedor (aba "Fornecedor"). */
  async getFornecedorCompleto(forCodigo: number) {
    return this.repo.fornecedorCompleto(forCodigo);
  }

  /** Cliente de mesmo CNPJ do fornecedor (aba "Garantia" — chaveia a config). null se não houver. */
  async getClienteDoFornecedor(forCodigo: number) {
    return this.repo.clienteDoFornecedor(forCodigo);
  }

  /** Busca CLIENTES no ERP para vincular manualmente quando não há match por CNPJ. */
  async buscarClientes(termo: string) {
    return this.repo.buscarClientes(termo);
  }

  /** Sugere filiais (mesma raiz de CNPJ) a partir de um fornecedor âncora. */
  async sugerirFiliais(forCodigo: number, jaNoGrupo: number[] = []): Promise<FornecedorErp[]> {
    const [ancora] = await this.repo.fornecedoresPorCodigo([forCodigo]);
    const raiz = raizCnpj(ancora?.cpf_cnpj);
    const excluir = [...new Set([forCodigo, ...jaNoGrupo])];
    return this.repo.sugerirFiliais(raiz, excluir);
  }

  async salvarGrupo(params: { groupId?: string | null; membros: number[]; principal?: number | null }) {
    const group_id = await this.repo.salvarGrupo(params);
    return { group_id };
  }

  async removerFornecedor(forCodigo: number) {
    await this.repo.removerFornecedor(forCodigo);
    return { ok: true };
  }

  // ------------------------ Parâmetros de compra -----------------------------

  /**
   * Parâmetros de compra do fornecedor (aba "Parâmetros de Compra").
   * Retorna o registro do próprio for_codigo; se ele não tiver e algum membro
   * do grupo tiver, retorna o do membro (herança de leitura, com herdado_de).
   */
  async getParametros(forCodigo: number) {
    const membros = await this.repo.expandGrupo(forCodigo);
    const rows = await this.repo.parametrosDe(membros);
    const proprio = rows.find((r) => r.for_codigo === forCodigo) ?? null;
    const efetivo = proprio ?? rows[0] ?? null;
    return {
      for_codigo: forCodigo,
      parametros: efetivo
        ? {
            lead_time_dias: efetivo.lead_time_dias,
            tempo_revisao_dias: efetivo.tempo_revisao_dias,
            pedido_minimo_valor:
              efetivo.pedido_minimo_valor == null ? null : Number(efetivo.pedido_minimo_valor),
            updated_at: efetivo.updated_at,
            updated_by: efetivo.updated_by,
            herdado_de: proprio ? null : efetivo.for_codigo,
          }
        : null,
    };
  }

  /**
   * Salva os parâmetros do fornecedor; com aplicarGrupo (padrão da tela),
   * replica os mesmos valores para todos os membros do grupo (matriz e
   * filiais), gravando o for_nome de cada um — o nome é a chave de casamento
   * com o histórico de compra usado pela análise de estoque.
   */
  async salvarParametros(params: {
    forCodigo: number;
    aplicarGrupo: boolean;
    lead_time_dias: number | null;
    tempo_revisao_dias: number | null;
    pedido_minimo_valor: number | null;
    updated_by: string | null;
  }) {
    const alvos = params.aplicarGrupo
      ? await this.repo.expandGrupo(params.forCodigo)
      : [params.forCodigo];
    const dados = await this.repo.fornecedoresPorCodigo(alvos);
    const nomes = new Map<number, string>();
    for (const d of dados) if (d.for_nome) nomes.set(d.for_codigo, d.for_nome);
    const n = await this.repo.salvarParametros({
      forCodigos: alvos,
      nomes,
      lead_time_dias: params.lead_time_dias,
      tempo_revisao_dias: params.tempo_revisao_dias,
      pedido_minimo_valor: params.pedido_minimo_valor,
      updated_by: params.updated_by,
    });
    return { ok: true, fornecedores_atualizados: n };
  }

  async removerGrupo(groupId: string) {
    await this.repo.removerGrupo(groupId);
    return { ok: true };
  }

  private fornecedorView(d: FornecedorErp | undefined, forCodigo: number) {
    return {
      for_codigo: forCodigo,
      for_nome: d?.for_nome ?? null,
      nome_fantasia: d?.nome_fantasia ?? null,
      cpf_cnpj: d?.cpf_cnpj ?? null,
      cidade: d?.cidade ?? null,
      uf: d?.uf ?? null,
      inativo: d?.inativo ?? false,
    };
  }

  // ---------------------- Helpers consumidos por cotação/NF-e ----------------

  /** Todos os for_codigo do grupo (inclui ele mesmo). */
  expandGrupo(forCodigo: number): Promise<number[]> {
    return this.repo.expandGrupo(forCodigo);
  }

  /**
   * True se o fornecedor (via grupo) usa "referência no final da descrição" — o
   * número de referência vem no fim do xProd, não no código (ex.: ARTEB).
   */
  refNaDescricao(forCodigo: number): Promise<boolean> {
    return this.repo.grupoTemRefDescricao(forCodigo);
  }

  /** CNPJs (só dígitos) de todos os fornecedores do grupo do for_codigo (inclui ele mesmo). */
  async cnpjsDoGrupo(forCodigo: number): Promise<string[]> {
    const grupo = await this.repo.expandGrupo(forCodigo);
    const dados = await this.repo.fornecedoresPorCodigo(grupo);
    const set = new Set<string>();
    for (const d of dados) {
      const dig = String(d.cpf_cnpj ?? '').replace(/\D/g, '');
      if (dig) set.add(dig);
    }
    return [...set];
  }

  /**
   * EM LOTE: para um fornecedor e uma lista de produtos, devolve a referência do
   * fornecedor (com_produto_fornecedor_referencia) — a própria do for_codigo se
   * houver, senão a de um relacionado do grupo. Sem entrada no mapa quando não há
   * nenhuma (o chamador usa a referência do cadastro do produto como fallback).
   * 1 query de grupo + 1 de catálogo.
   */
  async referenciasGrupo(
    forCodigo: number,
    proCodigos: Array<string | number>,
  ): Promise<Map<string, string>> {
    const codigos = [...new Set(proCodigos.map((c) => String(c)).filter(Boolean))];
    if (!codigos.length || !Number.isFinite(forCodigo)) return new Map();
    const grupo = await this.repo.expandGrupo(forCodigo);
    const refs = await this.repo.catalogoRefs(grupo, codigos);
    const out = new Map<string, string>();
    for (const codigo of codigos) {
      // própria do fornecedor primeiro, depois de um relacionado do grupo
      let r = refs.get(`${forCodigo}|${codigo}`);
      if (!r) {
        for (const f of grupo) {
          if (f === forCodigo) continue;
          const rr = refs.get(`${f}|${codigo}`);
          if (rr) {
            r = rr;
            break;
          }
        }
      }
      if (r) out.set(codigo, r);
    }
    return out;
  }

  /**
   * Referência do fornecedor para um produto, considerando o grupo:
   * 1) referência própria (mais recente não-vazia); 2) referência de um relacionado; 3) fallback.
   */
  async getReferenciaGrupo(proCodigo: string | number, forCodigo: number, fallback = ''): Promise<string> {
    const codigo = String(proCodigo);
    const grupo = await this.repo.expandGrupo(forCodigo);
    const refs = await this.repo.catalogoRefs(grupo, [codigo]);
    // própria primeiro
    const propria = refs.get(`${forCodigo}|${codigo}`);
    if (propria) return propria;
    // qualquer relacionado
    for (const f of grupo) {
      if (f === forCodigo) continue;
      const r = refs.get(`${f}|${codigo}`);
      if (r) return r;
    }
    return fallback;
  }

  /**
   * Preenche o ref_fornecedor em branco de uma lista de itens usando a referência
   * do grupo (própria ou de relacionado). Usado no match de NF-e. Muta e retorna a lista.
   */
  async enriquecerRefsEmBranco<T extends ItemRefEnriquecivel>(itens: T[]): Promise<T[]> {
    const emBranco = itens.filter(
      (i) => i.for_codigo != null && (i.ref_fornecedor == null || String(i.ref_fornecedor).trim() === ''),
    );
    if (!emBranco.length) return itens;

    const forCodigos = [...new Set(emBranco.map((i) => Number(i.for_codigo)))];
    const proCodigos = [...new Set(emBranco.map((i) => String(i.pro_codigo)))];

    const grupos = await this.repo.gruposDe(forCodigos); // for_codigo -> membros
    const todosForn = [...new Set([...forCodigos, ...[...grupos.values()].flat()])];
    const refs = await this.repo.catalogoRefs(todosForn, proCodigos);

    for (const item of emBranco) {
      const codigo = String(item.pro_codigo);
      const forn = Number(item.for_codigo);
      const membros = grupos.get(forn) ?? [forn];
      // própria primeiro, depois relacionados
      let ref = refs.get(`${forn}|${codigo}`);
      if (!ref) {
        for (const m of membros) {
          if (m === forn) continue;
          const r = refs.get(`${m}|${codigo}`);
          if (r) { ref = r; break; }
        }
      }
      if (ref) item.ref_fornecedor = ref;
    }
    return itens;
  }
}
