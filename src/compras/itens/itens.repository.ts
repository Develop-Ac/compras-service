import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService } from '../../shared/database/openquery/openquery.service';

/** Cotação é do atacado. O mesmo PRO_CODIGO existe nas 3 empresas do ERP. */
const EMPRESA = 3;

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
    const codigo = Number(proCodigo);
    if (!Number.isInteger(codigo)) return { dt_ultima_compra: null };

    const fbSql = `
        SELECT FIRST 1
               PRO.DT_ULTIMA_COMPRA, PRO.PRO_CODIGO
        FROM PRODUTOS PRO
        WHERE PRO.EMPRESA = ${EMPRESA} AND PRO.PRO_CODIGO = ${codigo}
    `;

    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${this.fbLiteral(fbSql)}')`;

    try {
      const rows = await this.mssql.query<PedidoCotacaoRow>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      const item = rows[0];

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
