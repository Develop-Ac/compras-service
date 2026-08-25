import { Injectable } from '@nestjs/common';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';
import { ErpApiService } from '../../../shared/erp-api/erp-api.service';

// A NF-e é fiscal: chega e é lançada na matriz.
const EMPRESA_FISCAL = 1;

/**
 * A rota /pendentes da erp-firebird-api exige período de emissão; a fila de
 * pendências, porém, não tem recorte — nota antiga não importada continua
 * pendente. A janela larga reproduz a fila inteira.
 */
const PENDENTES_DESDE = '2020-01-01';

@Injectable()
export class NotaFiscalRepository {
  constructor(
    private readonly openQueryService: OpenQueryService,
    private readonly erp: ErpApiService,
  ) {}

  private hojeMais1(): string {
    const d = new Date(Date.now() + 24 * 60 * 60 * 1000);
    return d.toISOString().slice(0, 10);
  }

  /**
   * Pendências pela API (rota nomeada /pendentes), no shape que os consumidores
   * esperam do caminho OPENQUERY (inclui EMPRESA; SITUACAO_NFE=1).
   */
  private async fetchPendentesViaApi(): Promise<Array<Record<string, any>>> {
    const rows = await this.erp.nfeDistribuicaoPendentes(
      EMPRESA_FISCAL,
      PENDENTES_DESDE,
      this.hojeMais1(),
    );
    return rows
      .filter((r) => Number(r.SITUACAO_NFE) === 1)
      .map((r) => ({
        EMPRESA: EMPRESA_FISCAL,
        CHAVE_NFE: r.CHAVE_NFE,
        CPF_CNPJ_EMITENTE: r.CPF_CNPJ_EMITENTE ?? null,
        NOME_EMITENTE: r.NOME_EMITENTE ?? null,
        RG_IE_EMITENTE: r.RG_IE_EMITENTE ?? null,
        DATA_EMISSAO: r.DATA_EMISSAO ?? null,
        TIPO_OPERACAO: r.TIPO_OPERACAO ?? null,
        TIPO_OPERACAO_DESC: r.TIPO_OPERACAO_DESC ?? null,
        TEM_XML: r.TEM_XML,
      }));
  }

  async fetchNfeDistribuicao() {
    return this.erp.comFallback<Array<Record<string, any>>>(
      async () => {
        // O XML não vem na listagem: busca em lotes só das notas que o têm.
        const pendentes = await this.fetchPendentesViaApi();
        const comXml = pendentes.filter((r) => this.temXml(r));
        const chaves = comXml.map((r) => String(r.CHAVE_NFE ?? '').trim()).filter(Boolean);
        const xmls = await this.erp.xmlPorChaves(chaves, EMPRESA_FISCAL);
        const xmlPorChave = new Map(
          xmls.map((x) => [String(x.CHAVE_NFE ?? '').trim(), x.XML_COMPLETO ?? null]),
        );
        return comXml.map((r) => ({
          ...r,
          XML_COMPLETO: xmlPorChave.get(String(r.CHAVE_NFE ?? '').trim()) ?? null,
        }));
      },
      () =>
        this.openQueryService.query(`
      SELECT *
      FROM OPENQUERY(
        CONSULTA,
        '
        SELECT
          NFD.EMPRESA,
          NFD.CHAVE_NFE,
          NFD.CPF_CNPJ_EMITENTE,
          NFD.NOME_EMITENTE,
          NFD.RG_IE_EMITENTE,
          NFD.DATA_EMISSAO,
          NFD.TIPO_OPERACAO,
          CASE
              WHEN NFD.TIPO_OPERACAO = 0 THEN ''ENTRADA PRÓPRIA''
              WHEN NFD.TIPO_OPERACAO = 1 THEN ''SAÍDA''
              ELSE ''OUTROS''
          END AS TIPO_OPERACAO_DESC,
          X.XML_COMPLETO
        FROM NFE_DISTRIBUICAO NFD
        LEFT JOIN NF_ENTRADA_XML X
               ON X.EMPRESA   = NFD.EMPRESA
              AND X.CHAVE_NFE = NFD.CHAVE_NFE
        WHERE NFD.IMPORTADA   = ''N''
          AND NFD.SITUACAO_NFE = 1
          AND NFD.EMPRESA      = 1
        '
      );
    `),
    );
  }

  /**
   * Versão leve de fetchNfeDistribuicao: lista as NF-e disponíveis
   * (mesmas condições: IMPORTADA='N', SITUACAO_NFE=1, EMPRESA=1) porém
   * SEM o XML_COMPLETO — e só notas que possuem XML.
   *
   * Obs.: o valor total da NF não vem daqui (não há coluna na NFE_DISTRIBUICAO).
   * O service enriquece cada linha com VALOR_TOTAL_NF a partir de com_nfe_conciliacao
   * (Postgres), cruzando pela CHAVE_NFE.
   */
  async fetchNfeDisponiveis() {
    return this.erp.comFallback<Array<Record<string, any>>>(
      async () => (await this.fetchPendentesViaApi()).filter((r) => this.temXml(r)),
      () =>
        this.openQueryService.query(`
      SELECT *
      FROM OPENQUERY(
        CONSULTA,
        '
        SELECT
          NFD.EMPRESA,
          NFD.CHAVE_NFE,
          NFD.CPF_CNPJ_EMITENTE,
          NFD.NOME_EMITENTE,
          NFD.RG_IE_EMITENTE,
          NFD.DATA_EMISSAO,
          NFD.TIPO_OPERACAO,
          CASE
              WHEN NFD.TIPO_OPERACAO = 0 THEN ''ENTRADA PRÓPRIA''
              WHEN NFD.TIPO_OPERACAO = 1 THEN ''SAÍDA''
              ELSE ''OUTROS''
          END AS TIPO_OPERACAO_DESC
        FROM NFE_DISTRIBUICAO NFD
        WHERE NFD.IMPORTADA   = ''N''
          AND NFD.SITUACAO_NFE = 1
          AND NFD.EMPRESA      = 1
          AND EXISTS (
            SELECT 1
            FROM NF_ENTRADA_XML X
            WHERE X.EMPRESA   = NFD.EMPRESA
              AND X.CHAVE_NFE = NFD.CHAVE_NFE
          )
        '
      );
    `),
    );
  }

  /**
   * XML completo de UMA chave — a rota por-chaves não trunca o BLOB, ao
   * contrário do OPENQUERY (que corta XML_COMPLETO em ~11 KB).
   */
  async fetchXmlDaChave(chaveNfe: string): Promise<string | null> {
    const chave = String(chaveNfe ?? '').trim();
    if (!chave) return null;

    return this.erp.comFallback<string | null>(
      async () => {
        const rows = await this.erp.xmlPorChaves([chave], EMPRESA_FISCAL);
        return rows[0]?.XML_COMPLETO ?? null;
      },
      async () => {
        const esc = chave.replace(/'/g, "''");
        const rows = await this.openQueryService.query<any>(
          `SELECT * FROM OPENQUERY(CONSULTA, 'SELECT X.XML_COMPLETO FROM NF_ENTRADA_XML X WHERE X.EMPRESA = 1 AND X.CHAVE_NFE = ''${esc}''')`,
          {},
          { timeout: 120_000, allowZeroRows: true },
        );
        return rows[0]?.XML_COMPLETO ?? null;
      },
    );
  }

  /** TEM_XML chega como flag do outro lado; qualquer forma "verdadeira" vale. */
  private temXml(r: Record<string, any>): boolean {
    const v = r?.TEM_XML;
    if (v == null) return false;
    if (typeof v === 'boolean') return v;
    if (typeof v === 'number') return v !== 0;
    return ['1', 's', 'sim', 'true', 't', 'y'].includes(String(v).trim().toLowerCase());
  }
}
