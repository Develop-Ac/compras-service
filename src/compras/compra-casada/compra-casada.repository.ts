import { Injectable } from '@nestjs/common';
import { MssqlService } from '../../common/mssql/mssql.service';

/** Linha crua devolvida pelo OPENQUERY (colunas em MAIÚSCULO, como no ERP). */
export interface FornecedorSubgrupoRow {
  SUBGRP_CODIGO: number;
  SUBGRP_DESCRICAO: string;
  FOR_CODIGO: number;
  FOR_NOME: string;
  QTD_PRODUTOS: number;
  QTD_NOTAS: number;
  QTD_COMPRADA: number;
  VALOR_TOTAL: number;
  PRIMEIRA_COMPRA: Date | null;
  ULTIMA_COMPRA: Date | null;
}

/** Empresa da matriz — mesma usada no filtro `i.empresa = 3` do OPENQUERY. */
const EMPRESA = 3;

@Injectable()
export class CompraCasadaFornecedoresRepository {
  constructor(private readonly mssql: MssqlService) {}

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
   * Fornecedores que já venderam produtos do subgrupo, com métricas de compra
   * (notas de entrada). Vai direto ao ERP via linked server `CONSULTA` — o BI
   * não tem essa granularidade de nfe_itens.
   *
   * OPENQUERY não aceita parâmetros: a query interna é uma string literal T-SQL,
   * então a descrição precisa ser escapada em dobro (aspas simples -> 4 aspas).
   */
  async fornecedoresPorSubgrupo(
    subgrpDescricao: string,
  ): Promise<FornecedorSubgrupoRow[]> {
    const literal = `''${subgrpDescricao.replace(/'/g, "''''")}''`;

    const query = `
      SELECT *
      FROM OPENQUERY(CONSULTA, '
          SELECT
              COALESCE(p.subgrp_codigo, 0)                      AS SUBGRP_CODIGO,
              COALESCE(sg.subgrp_descricao, ''(SEM SUBGRUPO)'') AS SUBGRP_DESCRICAO,
              f.for_codigo                                      AS FOR_CODIGO,
              f.for_nome                                        AS FOR_NOME,
              COUNT(DISTINCT i.pro_codigo)                      AS QTD_PRODUTOS,
              COUNT(DISTINCT e.nfe)                             AS QTD_NOTAS,
              SUM(i.quantidade)                                 AS QTD_COMPRADA,
              SUM(i.total)                                      AS VALOR_TOTAL,
              MIN(e.dt_entrada)                                 AS PRIMEIRA_COMPRA,
              MAX(e.dt_entrada)                                 AS ULTIMA_COMPRA
          FROM nfe_itens i
          JOIN nf_entrada e
            ON e.empresa = i.empresa AND e.nfe = i.nfe
          JOIN fornecedores f
            ON f.empresa = e.empresa AND f.for_codigo = e.for_codigo
          JOIN produtos p
            ON p.empresa = i.empresa AND p.pro_codigo = i.pro_codigo
          LEFT JOIN produtos_subgrupos sg
            ON sg.empresa = p.empresa AND sg.subgrp_codigo = p.subgrp_codigo
          WHERE i.empresa = ${EMPRESA}
            AND e.dt_cancelamento IS NULL
            AND e.opf_codigo IN (1, 2, 40)
            AND f.for_nome NOT LIKE ''%(DEPOSITO)%''
            AND sg.subgrp_descricao = ${literal}
          GROUP BY
              COALESCE(p.subgrp_codigo, 0),
              COALESCE(sg.subgrp_descricao, ''(SEM SUBGRUPO)''),
              f.for_codigo,
              f.for_nome
      ') AS t
      ORDER BY t.SUBGRP_DESCRICAO, t.VALOR_TOTAL DESC
    `;

    return this.mssql.query<FornecedorSubgrupoRow>(query);
  }
}
