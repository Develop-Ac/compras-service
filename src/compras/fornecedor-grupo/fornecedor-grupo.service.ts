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
