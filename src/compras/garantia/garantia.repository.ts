import { Injectable } from '@nestjs/common';
import { OpenQueryService as MssqlOpenQuery } from '../../shared/database/openquery/openquery.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';

const EMPRESA = 3;

/** Só dígitos (para casar CNPJ em formatos diferentes). */
function soDigitos(s: string | null | undefined): string {
  return String(s ?? '').replace(/\D/g, '');
}

export interface ClienteGarantia {
  cli_codigo: number;
  cli_nome: string | null;
  cpf_cnpj: string | null;
}

export interface TituloGarantia {
  titulo: string; // = NFS
  parcela: number | null;
  tt_codigo: string;
  cli_codigo: number;
  valor: number | null;
  emissao: string | null;
  vencimento: string | null;
  historico: string | null;
}

export interface ProdutoNfs {
  nfs: number;
  pro_codigo: number;
  quantidade: number | null;
  unitario: number | null;
}

/**
 * Repository da Garantia. Prioriza o SQL Server (Stage_*): fornecedores
 * (Stage_Fornecedores), títulos de contas a receber (Stage_ContasReceber_Titulos)
 * e descrição de produtos (Stage_Produtos). Vão ao Firebird via OPENQUERY apenas
 * os 2 pontos que o Stage não cobre: CLIENTES (Stage_Clientes vem com CPF_CNPJ
 * nulo, então o casamento por CNPJ só funciona no ERP) e os itens da NFS
 * (Stage_Vendas cobre só venda de produto, não as notas de garantia IC/RET).
 */
@Injectable()
export class GarantiaRepository {
  constructor(
    private readonly mssql: MssqlOpenQuery,
    private readonly erp: ErpApiService,
  ) {}

  /** CNPJs (só dígitos) dos fornecedores informados — Stage_Fornecedores. */
  async cnpjsDeFornecedores(forCodigos: number[]): Promise<Array<{ for_codigo: number; cnpj: string; cnpjDig: string }>> {
    const lista = [...new Set(forCodigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];
    const rows = await this.mssql.query<any>(
      `SELECT FOR_CODIGO, CPF_CNPJ
       FROM [BI].[dbo].[Stage_Fornecedores]
       WHERE EMPRESA = @empresa AND FOR_CODIGO IN (${lista.join(',')}) AND CPF_CNPJ IS NOT NULL`,
      { empresa: EMPRESA },
      { allowZeroRows: true },
    );
    return rows
      .map((r) => ({ for_codigo: Number(r.FOR_CODIGO), cnpj: String(r.CPF_CNPJ ?? ''), cnpjDig: soDigitos(r.CPF_CNPJ) }))
      .filter((r) => r.cnpjDig.length >= 11);
  }

  /**
   * Clientes de mesmo CNPJ dos fornecedores. Casa por dígitos (ignora máscara).
   *
   * NOTA: usa o CLIENTES do Firebird via OPENQUERY (NÃO Stage_Clientes) porque o
   * Stage NÃO popula CPF_CNPJ dos clientes (vem null) — o casamento por CNPJ só
   * funciona no ERP. É o mesmo motivo pelo qual clienteDoFornecedor/buscarClientes
   * já consultam o Firebird. Retorna o cnpjDig para religar ao for_codigo.
   */
  async clientesPorCnpj(cnpjDigs: string[]): Promise<Array<ClienteGarantia & { cnpjDig: string }>> {
    const lista = [...new Set(cnpjDigs.filter((d) => /^\d{11,}$/.test(d)))];
    if (!lista.length) return [];

    const rows = await this.erp.comFallback<any[]>(
      async () => {
        // A rota nomeada compara os dígitos e resolve UM documento por chamada;
        // o grupo de um fornecedor tem poucos CNPJs, então o leque é pequeno.
        const encontrados = await Promise.all(
          lista.map((doc) => this.erp.clientePorDocumento(doc, EMPRESA)),
        );
        return encontrados.filter(Boolean);
      },
      async () => {
        const inList = lista.map((d) => `'${d}'`).join(',');
        const fb =
          `SELECT c.cli_codigo, c.cli_nome, c.cpf_cnpj ` +
          `FROM clientes c ` +
          `WHERE c.empresa = ${EMPRESA} ` +
          `AND REPLACE(REPLACE(REPLACE(c.cpf_cnpj,'.',''),'/',''),'-','') IN (${inList})`;
        const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${fb.replace(/'/g, "''")}')`;
        return this.mssql.query<any>(tsql, {}, { allowZeroRows: true, timeout: 60_000 });
      },
    );

    return rows.map((r) => ({
      cli_codigo: Number(r.CLI_CODIGO),
      cli_nome: r.CLI_NOME ?? null,
      cpf_cnpj: r.CPF_CNPJ ?? null,
      cnpjDig: soDigitos(r.CPF_CNPJ),
    }));
  }

  /**
   * Títulos de garantia em ABERTO (Stage_ContasReceber_Titulos):
   * STATUS = 0 (Ativo) e DATA_PAGTO nula e TT_CODIGO IN ('IC','RET').
   * IC = Itens para Compra, RET = Retorno.
   */
  async titulosGarantia(cliCodigos: number[]): Promise<TituloGarantia[]> {
    const lista = [...new Set(cliCodigos.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];
    const rows = await this.mssql.query<any>(
      `SELECT TITULO, PARCELA, TT_CODIGO, CLI_CODIGO, VALOR, EMISSAO, VENCIMENTO, HISTORICO
       FROM [BI].[dbo].[Stage_ContasReceber_Titulos]
       WHERE STATUS = 0 AND DATA_PAGTO IS NULL
         AND TT_CODIGO IN ('IC','RET')
         AND CLI_CODIGO IN (${lista.join(',')})
       ORDER BY EMISSAO DESC`,
      {},
      { allowZeroRows: true },
    );
    return rows.map((r) => ({
      titulo: String(r.TITULO ?? '').trim(),
      parcela: r.PARCELA == null ? null : Number(r.PARCELA),
      tt_codigo: String(r.TT_CODIGO ?? '').trim(),
      cli_codigo: Number(r.CLI_CODIGO),
      valor: r.VALOR == null ? null : Number(r.VALOR),
      emissao: r.EMISSAO instanceof Date ? r.EMISSAO.toISOString() : (r.EMISSAO ?? null),
      vencimento: r.VENCIMENTO instanceof Date ? r.VENCIMENTO.toISOString() : (r.VENCIMENTO ?? null),
      historico: r.HISTORICO ?? null,
    }));
  }

  /**
   * Itens das NFS (Firebird via OPENQUERY). O TITULO do contas a receber é o
   * número da NFS. Stage_Vendas NÃO cobre estas notas (garantia), então é o
   * único ponto que consulta o ERP mãe. A descrição vem depois de Stage_Produtos.
   */
  async produtosPorNfs(nfsList: number[]): Promise<ProdutoNfs[]> {
    const lista = [...new Set(nfsList.filter((n) => Number.isFinite(n)))];
    if (!lista.length) return [];

    const rows = await this.erp.comFallback<any[]>(
      () => this.erp.nfsItensPorNotas(lista, EMPRESA),
      async () => {
        const fb = `SELECT ni.nfs, ni.pro_codigo, ni.quantidade, ni.unitario
                FROM nfs_itens ni
                WHERE ni.empresa = ${EMPRESA} AND ni.nfs IN (${lista.join(',')})`;
        const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${fb.replace(/'/g, "''")}')`;
        return this.mssql.query<any>(tsql, {}, { allowZeroRows: true, timeout: 60_000 });
      },
    );

    return rows.map((r) => ({
      nfs: Number(r.NFS),
      pro_codigo: Number(r.PRO_CODIGO),
      quantidade: r.QUANTIDADE == null ? null : Number(r.QUANTIDADE),
      unitario: r.UNITARIO == null ? null : Number(r.UNITARIO),
    }));
  }

  /** Descrições de produtos por código — Stage_Produtos (evita join no Firebird). */
  async descricoesProdutos(proCodigos: number[]): Promise<Map<number, string>> {
    const lista = [...new Set(proCodigos.filter((n) => Number.isFinite(n)))];
    const out = new Map<number, string>();
    if (!lista.length) return out;
    const rows = await this.mssql.query<any>(
      `SELECT PRO_CODIGO, PRO_DESCRICAO
       FROM [BI].[dbo].[Stage_Produtos]
       WHERE EMPRESA = @empresa AND PRO_CODIGO IN (${lista.join(',')})`,
      { empresa: EMPRESA },
      { allowZeroRows: true },
    );
    for (const r of rows) out.set(Number(r.PRO_CODIGO), r.PRO_DESCRICAO ?? '');
    return out;
  }
}
