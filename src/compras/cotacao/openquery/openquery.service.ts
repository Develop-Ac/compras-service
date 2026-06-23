import { Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { ConsultaOpenqueryRepository } from './openquery.repository';

type PedidoCotacaoRow = {
  pedido_cotacao: number;
  emissao: Date | string | null;
  pro_codigo: number | string | null;
  pro_descricao: string | null;
  mar_descricao: string | null;
  referencia: string | null;
  unidade: string | null;
  quantidade: number | string | null;
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
 * Serviço com lógica de negócio para consultas via OPENQUERY.
 */
@Injectable()
export class ConsultaOpenqueryService {
  constructor(private readonly repository: ConsultaOpenqueryRepository) {}

  /**
   * Busca itens de um pedido de cotação no Firebird via Linked Server (CONSULTA).
   */
  async buscarPorEmpresaPedido(empresa: number, pedido: number): Promise<PedidoCotacaoRow[]> {
    try {
      const rows = await this.repository.findPedidoItens(empresa, pedido);

      // normaliza emissao para ISO string quando vier Date
      return rows.map((r) => ({
        ...r,
        emissao: r?.emissao instanceof Date ? r.emissao.toISOString() : r?.emissao ?? null,
      }));
    } catch (err: any) {
      throw new InternalServerErrorException('Falha ao buscar pedido de cotação');
    }
  }

  /**
   * Busca dados do fornecedor por for_codigo (empresa fixa = 3).
   */
  async buscarFornecedorPorCodigo(forCodigo: number): Promise<any> {
    const empresa = 3; // conforme sua regra atual

    try {
      const row = await this.repository.findFornecedorByCodigo(empresa, forCodigo);
      if (!row) throw new NotFoundException('Fornecedor não encontrado.');
      // Devolve em minúsculas (shape do repo) E em MAIÚSCULAS (FOR_NOME, ...),
      // pois há telas que leem cada um. O findFornecedorByCodigo foi normalizado
      // p/ minúsculas; estes aliases evitam quebrar quem ainda lê MAIÚSCULAS.
      return {
        ...row,
        FOR_CODIGO: row.for_codigo,
        FOR_NOME: row.for_nome,
        CPF_CNPJ: row.cpf_cnpj,
        RG_IE: row.rg_ie,
        ENDERECO: row.endereco,
        BAIRRO: row.bairro,
        NUMERO: row.numero,
        CIDADE: row.cidade,
        UF: row.uf,
        EMAIL: row.email,
        FONE: row.fone,
        CONTATO: row.contato,
      };
    } catch (err: any) {
      if (err?.status === 404) throw err;
      throw new InternalServerErrorException('Falha ao buscar fornecedor');
    }
  }
}