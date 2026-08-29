import { Injectable } from '@nestjs/common';
import { MssqlService } from '../../common/mssql/mssql.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';

/** Linha crua da Stage_FornecedorSubgrupos (colunas em MAIÚSCULO, como no BI). */
export interface FornecedorSubgrupoRow {
  ID: number;
  DATA_CARGA: Date | null;
  SUBGRP_CODIGO: number;
  SUBGRP_DESCRICAO: string;
  FOR_CODIGO: number;
  FOR_NOME: string;
  FOR_FONE: string | null;
  FOR_CELULAR: string | null;
  FOR_OBS: string | null;
  FOR_UF: string | null;
  ULTIMA_COMPRA: Date | null;
  CARGA_EM: Date | null;
}

/** Última compra do produto por fornecedor — linha crua do OPENQUERY no ERP. */
export interface UltimaCompraProdutoRow {
  FOR_CODIGO: number;
  FOR_NOME: string;
  FOR_UF: string | null;
  PRO_CODIGO: number;
  PRO_DESCRICAO: string | null;
  ULTIMA_COMPRA: Date | null;
  NFE: number | null;
  UNIDADE: string | null;
  QUANTIDADE: number | null;
  UNITARIO: number | null;
  CUSTO_NOTA: number | null;
  FATOR_CONVERSAO: number | null;
}

/** Empresa da matriz — mesma usada no filtro `i.empresa = 3` do OPENQUERY. */
const EMPRESA = 3;

@Injectable()
export class CompraCasadaFornecedoresRepository {
  constructor(
    private readonly mssql: MssqlService,
    private readonly erp: ErpApiService,
  ) {}

  /** SUBGRP_CODIGO de um produto (Stage_Produtos, staging do ERP no BI). */
  async subgrupoDoProduto(proCodigo: number): Promise<number | null> {
    const rows = await this.mssql.query<{ subgrp_codigo: number | null }>(
      `SELECT TOP 1 p.SUBGRP_CODIGO AS subgrp_codigo
       FROM dbo.Stage_Produtos p
       WHERE p.PRO_CODIGO = @pro AND p.EMPRESA = @emp`,
      { pro: proCodigo, emp: EMPRESA },
    );
    if (!rows.length) return null;
    const cod = rows[0].subgrp_codigo;
    return cod == null ? null : Number(cod);
  }

  /** SUBGRP_DESCRICAO a partir do código do subgrupo (Stage_ProdutosSubgrupos). */
  async descricaoDoSubgrupo(subgrpCodigo: number): Promise<string | null> {
    const rows = await this.mssql.query<{ subgrp_descricao: string | null }>(
      `SELECT TOP 1 s.SUBGRP_DESCRICAO AS subgrp_descricao
       FROM dbo.Stage_ProdutosSubgrupos s
       WHERE s.SUBGRP_CODIGO = @sub AND s.EMPRESA = @emp`,
      { sub: subgrpCodigo, emp: EMPRESA },
    );
    return rows[0]?.subgrp_descricao ?? null;
  }

  /**
   * Fornecedores que já venderam produtos do subgrupo, direto da staging do BI
   * (`Stage_FornecedorSubgrupos`) — substitui o OPENQUERY no ERP.
   *
   * A stage é histórica: guarda uma linha por fornecedor A CADA carga, então o
   * ROW_NUMBER mantém só a linha mais recente de cada FOR_CODIGO (senão o
   * fornecedor aparece repetido, uma vez por DATA_CARGA).
   *
   * Ordenado pela última compra (mais recente primeiro).
   */
  async fornecedoresPorSubgrupo(
    subgrpDescricao: string,
  ): Promise<FornecedorSubgrupoRow[]> {
    return this.mssql.query<FornecedorSubgrupoRow>(
      `WITH ultima_carga AS (
           SELECT
               ID,
               DATA_CARGA,
               SUBGRP_CODIGO,
               SUBGRP_DESCRICAO,
               FOR_CODIGO,
               FOR_NOME,
               FOR_FONE,
               FOR_CELULAR,
               FOR_OBS,
               FOR_UF,
               ULTIMA_COMPRA,
               CARGA_EM,
               ROW_NUMBER() OVER (PARTITION BY FOR_CODIGO
                                  ORDER BY DATA_CARGA DESC, CARGA_EM DESC, ID DESC) AS rn
           FROM dbo.Stage_FornecedorSubgrupos
           WHERE SUBGRP_DESCRICAO = @sub
       )
       SELECT
           ID,
           DATA_CARGA,
           SUBGRP_CODIGO,
           SUBGRP_DESCRICAO,
           FOR_CODIGO,
           FOR_NOME,
           FOR_FONE,
           FOR_CELULAR,
           FOR_OBS,
           FOR_UF,
           ULTIMA_COMPRA,
           CARGA_EM
       FROM ultima_carga
       WHERE rn = 1
       ORDER BY ULTIMA_COMPRA DESC`,
      { sub: subgrpDescricao },
    );
  }

  /**
   * Última compra DESTE produto por fornecedor, ao vivo no ERP (linked server
   * `CONSULTA`) — a staging do BI só guarda a última compra do subgrupo.
   *
   * A nota mais recente pode ter mais de um item do mesmo produto; o
   * ROW_NUMBER externo fica com o último item (NFE/ITEM maiores) de cada
   * par (fornecedor, produto).
   *
   * OPENQUERY não aceita parâmetros — o código do produto é interpolado, por
   * isso é forçado a inteiro antes de entrar na string.
   */
  async ultimaCompraProdutoPorFornecedor(
    proCodigo: number,
  ): Promise<UltimaCompraProdutoRow[]> {
    const pro = Math.trunc(Number(proCodigo));
    if (!Number.isFinite(pro)) return [];

    return this.erp.comFallback<UltimaCompraProdutoRow[]>(
      async () => {
        // A rota nomeada faz a agregação (MAX por fornecedor + desempate por
        // NFE/ITEM) do lado da API e já devolve no shape do OPENQUERY antigo.
        const rows = await this.erp.ultimaCompraProduto(pro, EMPRESA);
        return rows.map(
          (r): UltimaCompraProdutoRow => ({
            FOR_CODIGO: Number(r.FOR_CODIGO),
            FOR_NOME: r.FOR_NOME ?? '',
            FOR_UF: r.FOR_UF ?? null,
            PRO_CODIGO: Number(r.PRO_CODIGO),
            PRO_DESCRICAO: r.PRO_DESCRICAO ?? null,
            ULTIMA_COMPRA: r.ULTIMA_COMPRA ? new Date(r.ULTIMA_COMPRA) : null,
            NFE: r.NFE == null ? null : Number(r.NFE),
            UNIDADE: r.UNIDADE ?? null,
            QUANTIDADE: r.QUANTIDADE == null ? null : Number(r.QUANTIDADE),
            UNITARIO: r.UNITARIO == null ? null : Number(r.UNITARIO),
            CUSTO_NOTA: r.CUSTO_NOTA == null ? null : Number(r.CUSTO_NOTA),
            FATOR_CONVERSAO: r.FATOR_CONVERSAO == null ? null : Number(r.FATOR_CONVERSAO),
          }),
        );
      },
      () => this.ultimaCompraProdutoViaOpenquery(pro),
    );
  }

  private async ultimaCompraProdutoViaOpenquery(
    pro: number,
  ): Promise<UltimaCompraProdutoRow[]> {
    const query = `
        SELECT x.FOR_CODIGO, x.FOR_NOME, x.FOR_UF, x.PRO_CODIGO, x.PRO_DESCRICAO,
               x.ULTIMA_COMPRA, x.NFE, x.UNIDADE, x.QUANTIDADE, x.UNITARIO,
               x.CUSTO_NOTA, x.FATOR_CONVERSAO
        FROM (
            SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.FOR_CODIGO, t.PRO_CODIGO
                                           ORDER BY t.NFE DESC, t.ITEM DESC) AS rn
            FROM OPENQUERY(CONSULTA, '
                SELECT
                    u.for_codigo      AS FOR_CODIGO,
                    f.for_nome        AS FOR_NOME,
                    f.UF              AS FOR_UF,
                    u.pro_codigo      AS PRO_CODIGO,
                    p.pro_descricao   AS PRO_DESCRICAO,
                    u.ultima_compra   AS ULTIMA_COMPRA,
                    e.nfe             AS NFE,
                    i.item            AS ITEM,
                    i.unidade         AS UNIDADE,
                    i.quantidade      AS QUANTIDADE,
                    i.unitario        AS UNITARIO,
                    i.custo_nota      AS CUSTO_NOTA,
                    i.fator_conversao AS FATOR_CONVERSAO
                FROM (
                    SELECT e2.for_codigo  AS for_codigo,
                           i2.pro_codigo  AS pro_codigo,
                           MAX(e2.dt_entrada) AS ultima_compra
                    FROM nfe_itens i2
                    JOIN nf_entrada e2 ON e2.empresa = i2.empresa AND e2.nfe = i2.nfe
                    WHERE i2.empresa = ${EMPRESA}
                      AND i2.pro_codigo = ${pro}
                      AND e2.dt_cancelamento IS NULL
                      AND e2.opf_codigo IN (1, 2, 40)
                    GROUP BY e2.for_codigo, i2.pro_codigo
                ) u
                JOIN nf_entrada e ON e.empresa = ${EMPRESA} AND e.for_codigo = u.for_codigo
                                 AND e.dt_entrada = u.ultima_compra
                                 AND e.dt_cancelamento IS NULL AND e.opf_codigo IN (1, 2, 40)
                JOIN nfe_itens    i ON i.empresa = ${EMPRESA} AND i.nfe = e.nfe AND i.pro_codigo = u.pro_codigo
                JOIN produtos     p ON p.empresa = ${EMPRESA} AND p.pro_codigo = u.pro_codigo
                JOIN fornecedores f ON f.empresa = ${EMPRESA} AND f.for_codigo = u.for_codigo
            ') AS t
        ) AS x
        WHERE x.rn = 1
        ORDER BY x.ULTIMA_COMPRA DESC;
    `;

    const result = await this.mssql.query<UltimaCompraProdutoRow>(query);
    return result;
  }
}
