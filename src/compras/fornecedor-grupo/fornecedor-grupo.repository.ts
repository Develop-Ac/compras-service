import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import cuid from 'cuid';
import { PrismaService } from '../../prisma/prisma.service';
import { OpenQueryService } from '../../shared/database/openquery/openquery.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';

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
    private readonly erp: ErpApiService,
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

  /**
   * True se o GRUPO do fornecedor está marcado como "referência no final da
   * descrição" (flag ref_descricao em qualquer membro). Usado no match de NF-e
   * para extrair a referência do fim do xProd (ex.: ARTEB).
   */
  async grupoTemRefDescricao(forCodigo: number): Promise<boolean> {
    const row = await this.prisma.com_fornecedor_relacionamento.findUnique({
      where: { for_codigo: forCodigo },
      select: { group_id: true },
    });
    if (!row) return false;
    const n = await this.prisma.com_fornecedor_relacionamento.count({
      where: { group_id: row.group_id, ref_descricao: true },
    });
    return n > 0;
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

  // --------------------- Parâmetros de compra (PG) ---------------------------

  /** Parâmetros de compra (lead time/revisão/pedido mínimo) por for_codigo. */
  async parametrosDe(forCodigos: number[]) {
    const lista = [...new Set(forCodigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];
    return this.prisma.com_fornecedor_parametros.findMany({
      where: { for_codigo: { in: lista } },
    });
  }

  /**
   * Upsert dos parâmetros para uma lista de fornecedores (mesmos valores para
   * todos — herança matriz→filiais do grupo). `nomes` alimenta for_nome, que é
   * a chave de casamento com o histórico de compra usado pela análise.
   */
  async salvarParametros(params: {
    forCodigos: number[];
    nomes: Map<number, string>;
    lead_time_dias: number | null;
    tempo_revisao_dias: number | null;
    pedido_minimo_valor: number | null;
    pedido_minimo_qtd: number | null;
    trabalha_carteira: boolean;
    updated_by: string | null;
  }) {
    const agora = new Date();
    await this.prisma.$transaction(async (tx) => {
      for (const for_codigo of params.forCodigos) {
        const for_nome = params.nomes.get(for_codigo) ?? String(for_codigo);
        await tx.com_fornecedor_parametros.upsert({
          where: { for_codigo },
          update: {
            for_nome,
            lead_time_dias: params.lead_time_dias,
            tempo_revisao_dias: params.tempo_revisao_dias,
            pedido_minimo_valor: params.pedido_minimo_valor,
            pedido_minimo_qtd: params.pedido_minimo_qtd,
            trabalha_carteira: params.trabalha_carteira,
            updated_at: agora,
            updated_by: params.updated_by,
          },
          create: {
            for_codigo,
            for_nome,
            lead_time_dias: params.lead_time_dias,
            tempo_revisao_dias: params.tempo_revisao_dias,
            pedido_minimo_valor: params.pedido_minimo_valor,
            pedido_minimo_qtd: params.pedido_minimo_qtd,
            trabalha_carteira: params.trabalha_carteira,
            updated_at: agora,
            updated_by: params.updated_by,
          },
        });
      }
    });
    return params.forCodigos.length;
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

  // ------------------------------ Cadastro do ERP ----------------------------
  //
  // A fonte é o FORNECEDORES do Firebird, lido pela erp-firebird-api. O Stage do
  // SQL Server continua atrás, como plano B: é uma CÓPIA que o ETL atualiza de
  // tempos em tempos, então fornecedor recém-cadastrado ou corrigido não aparece
  // até a próxima carga — e a tela não tem como distinguir isso de "não existe".
  //
  // Sem ERP_API_URL configurada, todas as leituras seguem pelo Stage e nada muda.

  /**
   * Texto do ERP sem os espaços à direita.
   *
   * Coluna CHAR volta preenchida até o tamanho declarado: INATIVO chega 'N   '
   * e um `=== 'S'` direto dá falso para TODO fornecedor inativo. Vale para os
   * dois caminhos, então a limpeza fica aqui, no ponto único de mapeamento.
   */
  private txt(v: any): string | null {
    const s = String(v ?? '').trim();
    return s === '' ? null : s;
  }

  private mapForn(r: any): FornecedorErp {
    return {
      for_codigo: Number(r.FOR_CODIGO),
      for_nome: this.txt(r.FOR_NOME),
      nome_fantasia: this.txt(r.NOME_FANTASIA),
      cpf_cnpj: this.txt(r.CPF_CNPJ),
      cidade: this.txt(r.CIDADE),
      uf: this.txt(r.UF),
      inativo: (this.txt(r.INATIVO) ?? '').toUpperCase() === 'S',
    };
  }

  /** Busca fornecedores por nome/nome fantasia/CNPJ/código. */
  async buscarFornecedores(termo: string): Promise<FornecedorErp[]> {
    const t = (termo ?? '').trim();
    if (!t) return [];

    return this.erp.comFallback(
      async () => {
        // Menos de 3 caracteres é varredura, e a API recusa por isso. A recusa
        // é decisão dela, não falha dela: barrar aqui evita que a busca curta
        // conte para o breaker e empurre TODAS as leituras para o Stage.
        if (t.length < 3) throw new Error('termo curto: fora do contrato da API');
        // O OU entre as quatro colunas mora na rota nomeada da API: o montador
        // de consulta livre só combina condições com AND.
        const rows = await this.erp.buscarFornecedores(t, EMPRESA, 50);
        return rows.map((r) => this.mapForn(r));
      },
      async () => {
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
      },
    );
  }

  /** Dados de fornecedores por código (para exibir membros do grupo). */
  async fornecedoresPorCodigo(codigos: number[]): Promise<FornecedorErp[]> {
    const lista = [...new Set(codigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];

    return this.erp.comFallback(
      async () => {
        const rows = await this.erp.fornecedoresPorCodigo(lista, EMPRESA);
        return rows.map((r) => this.mapForn(r));
      },
      async () => {
        const rows = await this.mssql.query<any>(
          `SELECT FOR_CODIGO, FOR_NOME, NOME_FANTASIA, CPF_CNPJ, CIDADE, UF, INATIVO
           FROM [BI].[dbo].[Stage_Fornecedores]
           WHERE EMPRESA = @empresa AND FOR_CODIGO IN (${lista.join(',')})`,
          { empresa: EMPRESA },
          { allowZeroRows: true },
        );
        return rows.map((r) => this.mapForn(r));
      },
    );
  }

  /** Filiais candidatas: mesma raiz de CNPJ (8 dígitos), exceto os já informados. */
  async sugerirFiliais(raiz: string, excluir: number[]): Promise<FornecedorErp[]> {
    if (!raiz || raiz.length < 8) return [];

    return this.erp.comFallback(
      async () => {
        const rows = await this.erp.filiaisPorRaiz(raiz, excluir, EMPRESA);
        return rows.map((r) => this.mapForn(r));
      },
      async () => {
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
      },
    );
  }

  /** Cadastro completo do fornecedor — aba "Fornecedor". */
  async fornecedorCompleto(forCodigo: number) {
    if (!Number.isFinite(forCodigo)) return null;

    const r = await this.erp.comFallback(
      () => this.erp.fornecedorCompleto(forCodigo, EMPRESA),
      async () => {
        const rows = await this.mssql.query<any>(
          `SELECT TOP 1 FOR_CODIGO, FOR_NOME, NOME_FANTASIA, CPF_CNPJ, RG_IE, ENDERECO, NUMERO, BAIRRO,
                  CIDADE, UF, CEP, FONE, FAX, CELULAR, EMAIL, CONTATO, INATIVO
           FROM [BI].[dbo].[Stage_Fornecedores]
           WHERE EMPRESA = @empresa AND FOR_CODIGO = @cod`,
          { empresa: EMPRESA, cod: forCodigo },
          { allowZeroRows: true },
        );
        return rows[0] ?? null;
      },
    );

    if (!r) return null;
    return {
      for_codigo: Number(r.FOR_CODIGO),
      for_nome: this.txt(r.FOR_NOME),
      nome_fantasia: this.txt(r.NOME_FANTASIA),
      cpf_cnpj: this.txt(r.CPF_CNPJ),
      rg_ie: this.txt(r.RG_IE),
      endereco: this.txt(r.ENDERECO),
      numero: this.txt(r.NUMERO),
      bairro: this.txt(r.BAIRRO),
      cidade: this.txt(r.CIDADE),
      uf: this.txt(r.UF),
      cep: this.txt(r.CEP),
      fone: this.txt(r.FONE),
      celular: this.txt(r.CELULAR),
      email: this.txt(r.EMAIL),
      contato: this.txt(r.CONTATO),
      inativo: (this.txt(r.INATIVO) ?? '').toUpperCase() === 'S',
    };
  }

  /**
   * Resolve o CLIENTE (ERP ao vivo, Firebird) de mesmo CNPJ do fornecedor.
   * A garantia é contas a receber, então é chaveada pelo cli_codigo. Retorna null se não houver.
   */
  async clienteDoFornecedor(
    forCodigo: number,
  ): Promise<{ cli_codigo: number; cli_nome: string | null; cpf_cnpj: string | null } | null> {
    const [forn] = await this.fornecedoresPorCodigo([forCodigo]);
    const cnpj = String(forn?.cpf_cnpj ?? '').trim();
    if (soDigitos(cnpj).length < 11) return null;

    const r = await this.erp.comFallback(
      // A rota compara os DÍGITOS dos dois lados: o ERP grava o documento
      // pontuado, e um dos lados chegar limpo devolve zero linhas com 200.
      () => this.erp.clientePorDocumento(cnpj, EMPRESA),
      async () => {
        // CLIENTES e FORNECEDORES guardam o CNPJ no mesmo formato
        // (ex.: 61.736.732/0035-88) -> match exato.
        const cnpjFb = cnpj.replace(/'/g, "''"); // escapa p/ o literal Firebird
        const fb = `SELECT FIRST 1 c.cli_codigo, c.cli_nome, c.cpf_cnpj
                    FROM clientes c
                    WHERE c.cpf_cnpj = '${cnpjFb}'`;
        const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${fb.replace(/'/g, "''")}')`;
        const rows = await this.mssql.query<any>(tsql, {}, { allowZeroRows: true, timeout: 60_000 });
        return rows[0] ?? null;
      },
    );

    if (!r) return null;
    return {
      cli_codigo: Number(r.CLI_CODIGO),
      cli_nome: this.txt(r.CLI_NOME),
      cpf_cnpj: this.txt(r.CPF_CNPJ),
    };
  }

  /** Busca CLIENTES no ERP (ao vivo) por nome/CNPJ/código — para vincular manualmente na aba Garantia. */
  async buscarClientes(
    termo: string,
  ): Promise<Array<{ cli_codigo: number; cli_nome: string | null; cpf_cnpj: string | null; cidade: string | null; uf: string | null }>> {
    const t = (termo ?? '').trim();
    if (!t) return [];

    const rows = await this.erp.comFallback(
      async () => {
        // Mesma regra da busca de fornecedor: termo curto é recusa de contrato,
        // não indisponibilidade — não pode contar para o breaker.
        if (t.length < 3) throw new Error('termo curto: fora do contrato da API');
        return this.erp.buscarClientes(t, EMPRESA, 50);
      },
      async () => {
        const esc = t.replace(/'/g, "''"); // escapa p/ o literal Firebird
        const up = esc.toUpperCase();
        const code = /^\d+$/.test(t) ? Number(t) : -1;

        const fb = `SELECT FIRST 50 c.cli_codigo, c.cli_nome, c.cpf_cnpj, c.cidade, c.uf
                    FROM clientes c
                    WHERE c.empresa = ${EMPRESA} AND (
                      UPPER(c.cli_nome) LIKE '%${up}%'
                      OR c.cpf_cnpj LIKE '%${esc}%'
                      OR c.cli_codigo = ${code}
                    )
                    ORDER BY c.cli_nome`;
        const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${fb.replace(/'/g, "''")}')`;
        return this.mssql.query<any>(tsql, {}, { allowZeroRows: true, timeout: 60_000 });
      },
    );

    return rows.map((r) => ({
      cli_codigo: Number(r.CLI_CODIGO),
      cli_nome: this.txt(r.CLI_NOME),
      cpf_cnpj: this.txt(r.CPF_CNPJ),
      cidade: this.txt(r.CIDADE),
      uf: this.txt(r.UF),
    }));
  }
}
