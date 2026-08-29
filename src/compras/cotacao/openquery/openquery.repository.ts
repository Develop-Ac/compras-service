import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService as MssqlOpenQuery } from '../../../shared/database/openquery/openquery.service';
import { ErpApiService } from '../../../shared/erp-api/erp-api.service';

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
 * Repository das consultas de cotação/fornecedor no ERP: erp-firebird-api
 * primeiro, OPENQUERY ao Firebird via Linked Server como plano B.
 */
@Injectable()
export class ConsultaOpenqueryRepository {
  private readonly logger = new Logger(ConsultaOpenqueryRepository.name);

  constructor(
    private readonly mssql: MssqlOpenQuery,
    private readonly erp: ErpApiService,
  ) {}

  /** Escapa aspas simples para o literal T-SQL do OPENQUERY */
  private fbLiteral(sql: string): string {
    return sql.replace(/'/g, "''");
  }

  /**
   * Busca itens de um pedido de cotação no ERP.
   *
   * As linhas saem com as chaves em minúsculas E em MAIÚSCULAS: o caminho
   * OPENQUERY devolvia os nomes como o driver mandasse, e há tela lendo cada
   * forma. O dobro de chaves é o preço de não quebrar nenhuma.
   */
  async findPedidoItens(empresa: number, pedido: number): Promise<PedidoCotacaoRow[]> {
    return this.erp.comFallback<PedidoCotacaoRow[]>(
      async () => {
        const rows = await this.erp.cotacaoItens(pedido, empresa);
        return rows.map((r) => {
          const base = {
            pedido_cotacao: r.PEDIDO_COTACAO ?? null,
            emissao: r.EMISSAO ?? null,
            pro_codigo: r.PRO_CODIGO ?? null,
            pro_descricao: r.PRO_DESCRICAO ?? null,
            mar_descricao: r.MAR_DESCRICAO ?? null,
            referencia: r.REFERENCIA ?? null,
            unidade: r.UNIDADE ?? null,
            quantidade: r.QUANTIDADE ?? null,
            dt_ultima_compra: r.DT_ULTIMA_COMPRA ?? null,
          };
          return {
            ...base,
            PEDIDO_COTACAO: base.pedido_cotacao,
            EMISSAO: base.emissao,
            PRO_CODIGO: base.pro_codigo,
            PRO_DESCRICAO: base.pro_descricao,
            MAR_DESCRICAO: base.mar_descricao,
            REFERENCIA: base.referencia,
            UNIDADE: base.unidade,
            QUANTIDADE: base.quantidade,
            DT_ULTIMA_COMPRA: base.dt_ultima_compra,
          } as PedidoCotacaoRow;
        });
      },
      () => this.findPedidoItensViaOpenquery(empresa, pedido),
    );
  }

  private async findPedidoItensViaOpenquery(
    empresa: number,
    pedido: number,
  ): Promise<PedidoCotacaoRow[]> {
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
   * Busca dados do fornecedor por for_codigo — cadastro vivo pela API
   * (fornecedorCompleto), OPENQUERY como plano B. Nos dois caminhos a saída é
   * normalizada para o shape em minúsculas do tipo.
   */
  async findFornecedorByCodigo(empresa: number, forCodigo: number): Promise<FornecedorRow | null> {
    return this.erp.comFallback<FornecedorRow | null>(
      async () => {
        const r = await this.erp.fornecedorCompleto(forCodigo, empresa);
        if (!r) return null;
        return {
          for_codigo: r.FOR_CODIGO ?? forCodigo,
          for_nome: r.FOR_NOME ?? null,
          cpf_cnpj: r.CPF_CNPJ ?? null,
          rg_ie: r.RG_IE ?? null,
          endereco: r.ENDERECO ?? null,
          bairro: r.BAIRRO ?? null,
          numero: r.NUMERO ?? null,
          cidade: r.CIDADE ?? null,
          uf: r.UF ?? null,
          email: r.EMAIL ?? null,
          fone: r.FONE ?? null,
          contato: r.CONTATO ?? null,
        };
      },
      () => this.findFornecedorByCodigoViaOpenquery(empresa, forCodigo),
    );
  }

  private async findFornecedorByCodigoViaOpenquery(
    empresa: number,
    forCodigo: number,
  ): Promise<FornecedorRow | null> {
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
      const rows = await this.mssql.query<Record<string, any>>(tsql, {}, { timeout: 60_000, allowZeroRows: true });
      const row = rows[0];
      if (!row) return null;
      // O OPENQUERY/Firebird devolve os nomes de coluna em MAIÚSCULAS
      // (FOR_NOME, CPF_CNPJ, ...), mas o tipo/consumidores usam minúsculas.
      // Normaliza para o shape declarado (com fallback p/ minúsculas, caso o
      // driver já devolva assim em algum ambiente).
      const pick = (...keys: string[]) => {
        for (const k of keys) if (row[k] != null) return row[k];
        return null;
      };
      return {
        for_codigo: pick('FOR_CODIGO', 'for_codigo'),
        for_nome: pick('FOR_NOME', 'for_nome'),
        cpf_cnpj: pick('CPF_CNPJ', 'cpf_cnpj'),
        rg_ie: pick('RG_IE', 'rg_ie'),
        endereco: pick('ENDERECO', 'endereco'),
        bairro: pick('BAIRRO', 'bairro'),
        numero: pick('NUMERO', 'numero'),
        cidade: pick('CIDADE', 'cidade'),
        uf: pick('UF', 'uf'),
        email: pick('EMAIL', 'email'),
        fone: pick('FONE', 'fone'),
        contato: pick('CONTATO', 'contato'),
      };
    } catch (err: any) {
      this.logger.error(`[OPENQUERY fornecedor] ${err?.message || err}`);
      throw err;
    }
  }
}
