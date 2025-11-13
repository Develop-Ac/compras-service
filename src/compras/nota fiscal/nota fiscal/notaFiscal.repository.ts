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
}