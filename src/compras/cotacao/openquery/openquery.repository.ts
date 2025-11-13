import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService as MssqlOpenQuery } from '../../../shared/database/openquery/openquery.service';

type PedidoCotacaoRow = {
  pedido_cotacao: number;
  emissao: Date | string | null;
  pro_codigo: number | string | null;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  unidade: string | null;
  quantidade: number | string | null;
  dt_ultima_compra: Date | string | null;
};

type FornecedorRow = {
  for_codigo: number | string | null;
  for_nome: string | null;
  cpf_cnpj: string | null;
  rg_ie: string | null;
  endereco: string | null;
  bairro: string | null;
  numero: string | null;
  cidade: string | null;
  uf: string | null;
  email: string | null;
  fone: string | null;
  contato: string | null;
};

/**
 * Repository que encapsula queries OPENQUERY ao Firebird via Linked Server.
 */
@Injectable()
export class ConsultaOpenqueryRepository {
  private readonly logger = new Logger(ConsultaOpenqueryRepository.name);

  constructor(private readonly mssql: MssqlOpenQuery) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string): string {
    return sql.replace(/'/g, "''");
  }

  /**
   * Busca itens de um pedido de cotação no Firebird.
   */
  async findPedidoItens(empresa: number, pedido: number): Promise<PedidoCotacaoRow[]> {
    const fbSql = `
      SELECT
          orc.pedido_cotacao,
          orc.emissao,
          iorc.pro_codigo,
          pro.pro_descricao,
          mar.mar_descricao,
          pro.referencia,
          pro.unidade,
          iorc.quantidade,
          pro.dt_ultima_compra
      FROM PEDIDOS_COTACOES orc
      LEFT JOIN PEDIDOS_COTACOES_ITENS iorc
             ON iorc.empresa = orc.empresa
            AND iorc.pedido_cotacao = orc.pedido_cotacao
      LEFT JOIN PRODUTOS pro
             ON pro.empresa = orc.empresa
            AND pro.pro_codigo = iorc.pro_codigo
      LEFT JOIN MARCAS mar
             ON mar.empresa = orc.empresa
            AND mar.mar_codigo = pro.mar_codigo
      WHERE orc.empresa = ${empresa}
        AND orc.pedido_cotacao = ${pedido}
    `;

    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<PedidoCotacaoRow>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      return rows || [];
    } catch (err: any) {
      this.logger.error(`[OPENQUERY pedido] ${err?.message || err}`);
      throw err;
    }
  }

  /**
   * Busca dados do fornecedor por for_codigo.
   */
  async findFornecedorByCodigo(empresa: number, forCodigo: number): Promise<FornecedorRow | null> {
    const fbSql = `
      SELECT
        fo.for_codigo,
        fo.for_nome,
        fo.cpf_cnpj,
        fo.rg_ie,
        fo.endereco,
        fo.bairro,
        fo.numero,
        fo.cidade,
        fo.uf,
        fo.email,
        fo.fone,
        fo.contato
      FROM FORNECEDORES fo
      WHERE fo.empresa = ${empresa}
        AND fo.for_codigo = ${forCodigo}
      ROWS 1
    `;

    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<FornecedorRow>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      return rows[0] || null;
    } catch (err: any) {
      this.logger.error(`[OPENQUERY fornecedor] ${err?.message || err}`);
      throw err;
    }
  }
}
