import { Injectable } from '@nestjs/common';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';

@Injectable()
export class NotaFiscalRepository {
  constructor(private readonly openQueryService: OpenQueryService) {}

  async fetchNfeDistribuicao() {
    const query = `
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
    `;
    return this.openQueryService.query(query);
  }

  /**
   * Versão leve de fetchNfeDistribuicao: lista as NF-e disponíveis
   * (mesmas condições: IMPORTADA='N', SITUACAO_NFE=1, EMPRESA=1) porém
   * SEM o XML_COMPLETO. Em vez do LEFT JOIN com NF_ENTRADA_XML usa
   * WHERE EXISTS, garantindo que só liste notas que possuem XML.
   *
   * Obs.: o valor total da NF não vem daqui (não há coluna na NFE_DISTRIBUICAO).
   * O service enriquece cada linha com VALOR_TOTAL_NF a partir de com_nfe_conciliacao
   * (Postgres), cruzando pela CHAVE_NFE.
   */
  async fetchNfeDisponiveis() {
    const query = `
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
    `;
    return this.openQueryService.query(query);
  }
}