import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import cuid from 'cuid';
import { PrismaService } from '../../prisma/prisma.service';
import { OpenQueryService } from '../../shared/database/openquery/openquery.service';

const EMPRESA = 3;

export type FornecedorErp = {
  for_codigo: number;
  for_nome: string | null;
  nome_fantasia: string | null;
  cpf_cnpj: string | null;
  cidade: string | null;
  uf: string | null;
  inativo: boolean;
};

/** Só dígitos do CNPJ/CPF. */
export const soDigitos = (v: any) => String(v ?? '').replace(/\D/g, '');
/** Raiz do CNPJ (8 primeiros dígitos) — identifica matriz/filiais da mesma empresa. */
export const raizCnpj = (v: any) => soDigitos(v).slice(0, 8);

@Injectable()
export class FornecedorGrupoRepository {
  private readonly logger = new Logger(FornecedorGrupoRepository.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly mssql: OpenQueryService,
  ) {}

  // --------------------------- Relacionamento (PG) ---------------------------

  /** Todos os for_codigo do grupo de um fornecedor (inclui ele mesmo). */
  async expandGrupo(forCodigo: number): Promise<number[]> {
    const row = await this.prisma.com_fornecedor_relacionamento.findUnique({
      where: { for_codigo: forCodigo },
      select: { group_id: true },
    });
    if (!row) return [forCodigo];
    const membros = await this.prisma.com_fornecedor_relacionamento.findMany({
      where: { group_id: row.group_id },
      select: { for_codigo: true },
    });
    const set = new Set<number>(membros.map((m) => m.for_codigo));
    set.add(forCodigo);
    return [...set];
  }

  /** Mapa for_codigo -> membros do grupo, para uma lista de fornecedores (1-2 queries). */
  async gruposDe(forCodigos: number[]): Promise<Map<number, number[]>> {
    const out = new Map<number, number[]>();
    const unicos = [...new Set(forCodigos.filter((n) => Number.isFinite(n)))];
    if (!unicos.length) return out;

    const ancoras = await this.prisma.com_fornecedor_relacionamento.findMany({
      where: { for_codigo: { in: unicos } },
      select: { for_codigo: true, group_id: true },
    });
    const groupDeForn = new Map<number, string>();
    for (const a of ancoras) groupDeForn.set(a.for_codigo, a.group_id);

    const groupIds = [...new Set(ancoras.map((a) => a.group_id))];
    const membrosPorGrupo = new Map<string, number[]>();
    if (groupIds.length) {
      const membros = await this.prisma.com_fornecedor_relacionamento.findMany({
        where: { group_id: { in: groupIds } },
        select: { for_codigo: true, group_id: true },
      });
      for (const m of membros) {
        const arr = membrosPorGrupo.get(m.group_id) ?? [];
        arr.push(m.for_codigo);
        membrosPorGrupo.set(m.group_id, arr);
      }
    }

    for (const f of unicos) {
      const g = groupDeForn.get(f);
      out.set(f, g ? membrosPorGrupo.get(g) ?? [f] : [f]);
    }
    return out;
  }

  /** Lista os grupos (group_id + for_codigos + principal). */
  async listarGrupos() {
    const rows = await this.prisma.com_fornecedor_relacionamento.findMany({
      orderBy: [{ group_id: 'asc' }, { principal: 'desc' }, { for_codigo: 'asc' }],
      select: { group_id: true, for_codigo: true, principal: true },
    });
    const mapa = new Map<string, { for_codigo: number; principal: boolean }[]>();
    for (const r of rows) {
      const arr = mapa.get(r.group_id) ?? [];
      arr.push({ for_codigo: r.for_codigo, principal: r.principal });
      mapa.set(r.group_id, arr);
    }
    return mapa;
  }

  /**
   * Define os membros de um grupo numa transação:
   *  - cria group_id se não vier;
   *  - remove do grupo quem saiu;
   *  - move/insere os for_codigo informados para este grupo (for_codigo é único);
   *  - marca o principal (e desmarca os demais).
   */
  async salvarGrupo(params: { groupId?: string | null; membros: number[]; principal?: number | null }) {
    const groupId = params.groupId?.trim() || cuid();
    const membros = [...new Set(params.membros.filter((n) => Number.isFinite(n)))];
    const principal = params.principal ?? null;

    await this.prisma.$transaction(async (tx) => {
      // tira do grupo quem não está mais na lista
      await tx.com_fornecedor_relacionamento.deleteMany({
        where: { group_id: groupId, for_codigo: { notIn: membros.length ? membros : [-1] } },
      });
      for (const for_codigo of membros) {
        const isPrincipal = principal != null && for_codigo === principal;
        await tx.com_fornecedor_relacionamento.upsert({
          where: { for_codigo },
          update: { group_id: groupId, principal: isPrincipal },
          create: { id: cuid(), group_id: groupId, for_codigo, principal: isPrincipal },
        });
      }
      // garante principal único dentro do grupo
      if (principal != null) {
        await tx.com_fornecedor_relacionamento.updateMany({
          where: { group_id: groupId, for_codigo: { not: principal } },
          data: { principal: false },
        });
      }
    });
    return groupId;
  }

  /** Remove um fornecedor de qualquer grupo. */
  async removerFornecedor(forCodigo: number) {
    return this.prisma.com_fornecedor_relacionamento.deleteMany({ where: { for_codigo: forCodigo } });
  }

  /** Dissolve um grupo inteiro. */
  async removerGrupo(groupId: string) {
    return this.prisma.com_fornecedor_relacionamento.deleteMany({ where: { group_id: groupId } });
  }

  // ------------------------- Catálogo de referências -------------------------

  /** Última referência NÃO-vazia por (fornecedor, codigo) para os conjuntos dados. */
  async catalogoRefs(forCodigos: number[], proCodigos: string[]): Promise<Map<string, string>> {
    const out = new Map<string, string>();
    const fs = [...new Set(forCodigos.filter((n) => Number.isFinite(n)))];
    const ps = [...new Set(proCodigos.map((p) => String(p).trim()).filter(Boolean))];
    if (!fs.length || !ps.length) return out;

    const rows = await this.prisma.$queryRaw<{ fornecedor: number; codigo: string; referencia: string }[]>(Prisma.sql`
      SELECT DISTINCT ON (fornecedor, codigo) fornecedor, codigo, referencia
      FROM com_produto_fornecedor_referencia
      WHERE referencia <> '' AND fornecedor = ANY(${fs}::int[]) AND codigo = ANY(${ps}::text[])
      ORDER BY fornecedor, codigo, data DESC
    `);
    for (const r of rows) out.set(`${r.fornecedor}|${r.codigo}`, r.referencia);
    return out;
  }

  // --------------------------- Stage_Fornecedores ----------------------------

  private mapForn(r: any): FornecedorErp {
    return {
      for_codigo: Number(r.FOR_CODIGO),
      for_nome: r.FOR_NOME ?? null,
      nome_fantasia: r.NOME_FANTASIA ?? null,
      cpf_cnpj: r.CPF_CNPJ ?? null,
      cidade: r.CIDADE ?? null,
      uf: r.UF ?? null,
      inativo: String(r.INATIVO ?? '').toUpperCase() === 'S',
    };
  }

  /** Busca fornecedores no Stage_Fornecedores por nome/nome fantasia/CNPJ/código. */
  async buscarFornecedores(termo: string): Promise<FornecedorErp[]> {
    const t = (termo ?? '').trim();
    if (!t) return [];
    const like = `%${t}%`;
    const dig = soDigitos(t);
    const likeDig = dig ? `%${dig}%` : '%@@nope@@%';
    const code = /^\d+$/.test(t) ? Number(t) : -1;

    const rows = await this.mssql.query<any>(
      `SELECT TOP 50 FOR_CODIGO, FOR_NOME, NOME_FANTASIA, CPF_CNPJ, CIDADE, UF, INATIVO
       FROM [BI].[dbo].[Stage_Fornecedores]
       WHERE EMPRESA = @empresa AND (
         FOR_NOME LIKE @like OR NOME_FANTASIA LIKE @like
         OR REPLACE(REPLACE(REPLACE(CPF_CNPJ,'.',''),'/',''),'-','') LIKE @likeDig
         OR FOR_CODIGO = @code
       )
       ORDER BY FOR_NOME`,
      { empresa: EMPRESA, like, likeDig, code },
      { allowZeroRows: true },
    );
    return rows.map((r) => this.mapForn(r));
  }

  /** Dados de fornecedores por código (para exibir membros do grupo). */
  async fornecedoresPorCodigo(codigos: number[]): Promise<FornecedorErp[]> {
    const lista = [...new Set(codigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];
    const rows = await this.mssql.query<any>(
      `SELECT FOR_CODIGO, FOR_NOME, NOME_FANTASIA, CPF_CNPJ, CIDADE, UF, INATIVO
       FROM [BI].[dbo].[Stage_Fornecedores]
       WHERE EMPRESA = @empresa AND FOR_CODIGO IN (${lista.join(',')})`,
      { empresa: EMPRESA },
      { allowZeroRows: true },
    );
    return rows.map((r) => this.mapForn(r));
  }

  /** Filiais candidatas: mesma raiz de CNPJ (8 dígitos), exceto os já informados. */
  async sugerirFiliais(raiz: string, excluir: number[]): Promise<FornecedorErp[]> {
    if (!raiz || raiz.length < 8) return [];
    const rows = await this.mssql.query<any>(
      `SELECT FOR_CODIGO, FOR_NOME, NOME_FANTASIA, CPF_CNPJ, CIDADE, UF, INATIVO
       FROM [BI].[dbo].[Stage_Fornecedores]
       WHERE EMPRESA = @empresa
         AND LEFT(REPLACE(REPLACE(REPLACE(CPF_CNPJ,'.',''),'/',''),'-',''), 8) = @raiz`,
      { empresa: EMPRESA, raiz },
      { allowZeroRows: true },
    );
    const exc = new Set(excluir);
    return rows.map((r) => this.mapForn(r)).filter((f) => !exc.has(f.for_codigo));
  }
}
