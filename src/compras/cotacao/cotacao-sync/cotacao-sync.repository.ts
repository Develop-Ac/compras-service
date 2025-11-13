import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class CotacaoSyncRepository {
  constructor(private readonly prisma: PrismaService) {}

  async upsertFornecedorComItensTx(
    fornecedores: Array<{
      pedido_cotacao: number;
      for_codigo: number;
      for_nome: string;
      cpf_cnpj: string | null;
      itens: Array<{
        emissao: Date | null;
        pro_codigo: number;
        pro_descricao: string;
        mar_descricao: string | null;
        referencia: string | null;
        unidade: string | null;
        quantidade: number;
        valor_unitario: number | null;
      }>;
    }>,
  ) {
    if (!fornecedores?.length) return;
    await this.prisma.$transaction(async (tx) => {
      for (const forn of fornecedores) {
        await tx.com_cotacao_for.upsert({
          where: {
            pedido_for: {
              pedido_cotacao: forn.pedido_cotacao,
              for_codigo: forn.for_codigo,
            },
          },
          update: { for_nome: forn.for_nome, cpf_cnpj: forn.cpf_cnpj },
          create: {
            pedido_cotacao: forn.pedido_cotacao,
            for_codigo: forn.for_codigo,
            for_nome: forn.for_nome,
            cpf_cnpj: forn.cpf_cnpj,
          },
        });

        await tx.com_cotacao_itens_for.deleteMany({
          where: { pedido_cotacao: forn.pedido_cotacao, for_codigo: forn.for_codigo },
        });

        if (forn.itens?.length) {
          const inputs: Prisma.com_cotacao_itens_forCreateManyInput[] = forn.itens.map((i) => ({
            pedido_cotacao: forn.pedido_cotacao,
            for_codigo: forn.for_codigo,
            emissao: i.emissao,
            pro_codigo: i.pro_codigo,
            pro_descricao: i.pro_descricao,
            mar_descricao: i.mar_descricao,
            referencia: i.referencia,
            unidade: i.unidade,
            quantidade: i.quantidade,
            valor_unitario: i.valor_unitario,
          }));
          if (inputs.length) await tx.com_cotacao_itens_for.createMany({ data: inputs });
        }
      }
    });
  }

  async listFornecedoresLocal(pedido_cotacao: number) {
    return this.prisma.com_cotacao_for.findMany({
      where: { pedido_cotacao },
      include: { itens: true },
      orderBy: [{ for_codigo: 'asc' }],
    });
  }
}
