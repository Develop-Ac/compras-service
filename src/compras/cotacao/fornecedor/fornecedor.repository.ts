import { Injectable } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class FornecedorRepository {
  constructor(private readonly prisma: PrismaClient) {}

  async upsertFornecedor(dto: any) {
    return this.prisma.com_cotacao_for.upsert(dto);
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
