import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService } from '../../shared/database/openquery/openquery.service';

type PedidoCotacaoRow = {
  pedido_cotacao: number;
  emissao: Date | string | null;
  pro_codigo: number | string | null;
  PRO_CODIGO: number | string | null;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  unidade: string | null;
  quantidade: number | string | null;
  dt_ultima_compra: Date | string | null;
  DT_ULTIMA_COMPRA: Date | string | null;
};

@Injectable()
export class ItensRepository {
  private readonly logger = new Logger(ItensRepository.name);

  constructor(private readonly mssql: OpenQueryService) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string): string {
    return sql.replace(/'/g, "''");
  }

  async getUltimaCompra(proCodigo: string | number) {
    // A tabela com_cotacao_itens_for está no SQL Server,
    // portanto NÃO deve ser acessada via OPENQUERY/CONSULTA.
    const fbSql = `
        Select
        pro.dt_ultima_compra,
        pro.pro_codigo
        from produtos pro
        where pro.pro_codigo = ${proCodigo}
    `;

    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<PedidoCotacaoRow>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      console.log(rows);
      // Filtra pelo proCodigo
      const item = rows.find(
        row => String(row.PRO_CODIGO) === String(proCodigo)
      );

      // Pega a data ou null
      const dt = item?.DT_ULTIMA_COMPRA ?? item?.dt_ultima_compra ?? null;
      // Formata para dd/mm/yyyy
      let dtFormatada: string | null = null;
      if (dt) {
        const date = new Date(dt);
        dtFormatada = date.toLocaleDateString('pt-BR');
      }
      return { dt_ultima_compra: dtFormatada };
    } catch (err: any) {
      this.logger.error(
        `[MSSQL ultima-compra] ${err?.message || err}`,
      );
      throw err;
    }
  }
}
