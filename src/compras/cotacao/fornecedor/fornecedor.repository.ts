import { Injectable } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class FornecedorRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async upsertFornecedor(dto: any) {
    return this.prisma.com_cotacao_for.upsert(dto);
  }

    
  async trashFornecedor(pedido_cotacao: number, for_codigo: number) {
    return this.prisma.com_cotacao_for.updateMany({
      where: {
        pedido_cotacao,
        for_codigo,
        trash: 0,
      },
      data: {
        trash: 1,
      },
    });
  }

  async findCotacaoItens(pedido_cotacao: number) {
    return this.prisma.com_cotacao_itens.findMany({
      where: { pedido_cotacao },
      select: {
        emissao: true,
        pro_codigo: true,
        pro_descricao: true,
        mar_descricao: true,
        referencia: true,
        unidade: true,
        quantidade: true,
        qtd_sugerida: true,
      },
    });
  }

  async listarFornecedoresPorPedido(pedido_cotacao: number) {
    return this.prisma.com_cotacao_for.findMany({
      where: { pedido_cotacao },
      select: {
        for_codigo: true,
        for_nome: true,
        cpf_cnpj: true,
      },
      orderBy: { updated_at: 'desc' },
    });
  }
}
