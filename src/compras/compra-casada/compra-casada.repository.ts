import { Injectable } from '@nestjs/common';
import { MssqlService } from '../../common/mssql/mssql.service';

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
   * Fornecedores que já venderam produtos do subgrupo, direto da staging do BI
   * (`Stage_FornecedorSubgrupos`) — substitui o OPENQUERY no ERP.
   * Ordenado pela última compra (mais recente primeiro).
   */
  async fornecedoresPorSubgrupo(
    subgrpDescricao: string,
  ): Promise<FornecedorSubgrupoRow[]> {
    return this.mssql.query<FornecedorSubgrupoRow>(
      `SELECT
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
       FROM dbo.Stage_FornecedorSubgrupos
       WHERE SUBGRP_DESCRICAO = @sub
       ORDER BY ULTIMA_COMPRA DESC`,
      { sub: subgrpDescricao },
    );
  }
}
