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
  async buscarFornecedorPorCodigo(forCodigo: number): Promise<FornecedorRow> {
    const empresa = 3; // conforme sua regra atual

    try {
      const row = await this.repository.findFornecedorByCodigo(empresa, forCodigo);
      if (!row) throw new NotFoundException('Fornecedor não encontrado.');
      return row;
    } catch (err: any) {
      if (err?.status === 404) throw err;
      throw new InternalServerErrorException('Falha ao buscar fornecedor');
    }
  }
}