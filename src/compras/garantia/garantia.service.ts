import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import { FornecedorGrupoService } from '../fornecedor-grupo/fornecedor-grupo.service';
import { GarantiaRepository } from './garantia.repository';

const TT_LABEL: Record<string, string> = {
  IC: 'Itens para Compra',
  RET: 'Retorno',
};

export interface ProdutoGarantia {
  pro_codigo: number;
  pro_descricao: string;
  quantidade: number | null;
  unitario: number | null;
}

export interface TituloGarantiaDto {
  nfs: string; // TITULO = número da NFS
  tt_codigo: string;
  tt_label: string;
  parcela: number | null;
  valor: number | null;
  emissao: string | null;
  vencimento: string | null;
  historico: string | null;
  cli_codigo: number;
  cli_nome: string | null;
  for_codigo: number | null;
  cnpj: string | null;
  produtos: ProdutoGarantia[];
}

/** Item do pedido que casa com um produto em garantia (por código ou similar). */
export interface ItemPedidoGarantiaDto {
  pro_codigo: number;
  /** TT_CODIGO(s) da garantia casada — 'IC' e/ou 'RET'. */
  tt_codigos: string[];
  /** Como casou: 'codigo' (mesmo pro_codigo) ou 'grupo' (produto similar). */
  via: 'codigo' | 'grupo';
}

@Injectable()
export class GarantiaService {
  private readonly logger = new Logger(GarantiaService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly grupo: FornecedorGrupoService,
    private readonly repo: GarantiaRepository,
  ) {}

  /**
   * Garantias pendentes ligadas a um pedido: para cada fornecedor do pedido
   * (e seu grupo/filiais), acha o cliente de mesmo CNPJ e lista os títulos de
   * contas a receber IC/RET em aberto, com os produtos de cada NFS.
   *
   * Fontes: Stage_Fornecedores → Stage_Clientes → Stage_ContasReceber_Titulos
   * (tudo no SQL Server); apenas os itens da NFS vêm do Firebird (OPENQUERY),
   * pois não têm cópia em Stage.
   */
  async garantiasDoPedido(pedidoId: string, opts?: { matchItensPedido?: boolean }) {
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { id: true, for_codigo: true },
    });
    if (!pedido) {
      throw new NotFoundException(`Pedido ${pedidoId} não encontrado.`);
    }

    const vazio = {
      pedido_id: pedidoId,
      for_codigo: pedido.for_codigo ?? null,
      tem_garantia: false,
      totais: { titulos: 0, produtos: 0, valor_total: 0 },
      titulos: [] as TituloGarantiaDto[],
      itens_pedido_garantia: [] as ItemPedidoGarantiaDto[],
    };
    if (pedido.for_codigo == null) return vazio;

    // 1) Fornecedor do pedido + grupo (matriz/filiais compartilham CNPJs distintos).
    const forCodigos = await this.grupo.expandGrupo(Number(pedido.for_codigo));

    // 2) CNPJs desses fornecedores (Stage_Fornecedores).
    const cnpjs = await this.repo.cnpjsDeFornecedores(forCodigos);
    if (!cnpjs.length) return vazio;

    // 3) Clientes de mesmo CNPJ (Stage_Clientes). Mapeia cnpjDig -> for_codigo.
    const forPorCnpj = new Map<string, number>();
    for (const c of cnpjs) if (!forPorCnpj.has(c.cnpjDig)) forPorCnpj.set(c.cnpjDig, c.for_codigo);
    const clientes = await this.repo.clientesPorCnpj(cnpjs.map((c) => c.cnpjDig));
    if (!clientes.length) return vazio;

    const clienteInfo = new Map<number, { cli_nome: string | null; cnpj: string | null; for_codigo: number | null }>();
    for (const cli of clientes) {
      clienteInfo.set(cli.cli_codigo, {
        cli_nome: cli.cli_nome,
        cnpj: cli.cpf_cnpj,
        for_codigo: forPorCnpj.get(cli.cnpjDig) ?? null,
      });
    }

    // 4) Títulos IC/RET em aberto desses clientes (Stage_ContasReceber_Titulos).
    const titulos = await this.repo.titulosGarantia([...clienteInfo.keys()]);
    if (!titulos.length) return vazio;

    // 5) Itens das NFS (Firebird OPENQUERY) + descrição via Stage_Produtos.
    const nfsNums = titulos
      .map((t) => Number(t.titulo))
      .filter((n) => Number.isFinite(n));
    const produtos = await this.repo.produtosPorNfs(nfsNums);
    const descricoes = await this.repo.descricoesProdutos(produtos.map((p) => p.pro_codigo));

    const produtosPorNfs = new Map<number, ProdutoGarantia[]>();
    for (const p of produtos) {
      const arr = produtosPorNfs.get(p.nfs) ?? [];
      arr.push({
        pro_codigo: p.pro_codigo,
        pro_descricao: descricoes.get(p.pro_codigo) || `Produto ${p.pro_codigo}`,
        quantidade: p.quantidade,
        unitario: p.unitario,
      });
      produtosPorNfs.set(p.nfs, arr);
    }

    // 6) Monta a resposta por título.
    const out: TituloGarantiaDto[] = titulos.map((t) => {
      const nfsNum = Number(t.titulo);
      const info = clienteInfo.get(t.cli_codigo);
      return {
        nfs: t.titulo,
        tt_codigo: t.tt_codigo,
        tt_label: TT_LABEL[t.tt_codigo] ?? t.tt_codigo,
        parcela: t.parcela,
        valor: t.valor,
        emissao: t.emissao,
        vencimento: t.vencimento,
        historico: t.historico,
        cli_codigo: t.cli_codigo,
        cli_nome: info?.cli_nome ?? null,
        for_codigo: info?.for_codigo ?? null,
        cnpj: info?.cnpj ?? null,
        produtos: produtosPorNfs.get(nfsNum) ?? [],
      };
    });

    const totalProdutos = out.reduce((acc, t) => acc + t.produtos.length, 0);
    const valorTotal = out.reduce((acc, t) => acc + (t.valor ?? 0), 0);

    // Casa os PRODUTOS EM GARANTIA com os ITENS DO PEDIDO (só quando pedido pela
    // tela — evita custo no seed/persistência). Casa por pro_codigo OU por
    // "produto similar" (mesmo group_id em com_relacionamento_itens, o mesmo
    // agrupamento usado pela análise de estoque).
    const itensPedidoGarantia = opts?.matchItensPedido
      ? await this.matchItensPedidoComGarantia(pedidoId, out)
      : [];

    return {
      pedido_id: pedidoId,
      for_codigo: pedido.for_codigo,
      tem_garantia: out.length > 0,
      totais: { titulos: out.length, produtos: totalProdutos, valor_total: valorTotal },
      titulos: out,
      itens_pedido_garantia: itensPedidoGarantia,
    };
  }

  /**
   * Dado os títulos de garantia (com seus produtos), devolve os itens do pedido
   * que "casam" com algum produto em garantia — por pro_codigo direto OU por
   * produto SIMILAR (mesmo group_id em com_relacionamento_itens, o agrupamento
   * mantido pela análise de estoque em /similar/*). Cada casamento traz o(s)
   * TT_CODIGO(s) (IC/RET) para a tag na lista de itens do pedido.
   */
  private async matchItensPedidoComGarantia(
    pedidoId: string,
    titulos: TituloGarantiaDto[],
  ): Promise<ItemPedidoGarantiaDto[]> {
    // pro_codigo (garantia) -> conjunto de TT_CODIGO
    const ttPorProdGarantia = new Map<string, Set<string>>();
    for (const t of titulos) {
      for (const p of t.produtos) {
        const k = String(p.pro_codigo);
        if (!ttPorProdGarantia.has(k)) ttPorProdGarantia.set(k, new Set());
        ttPorProdGarantia.get(k)!.add(t.tt_codigo);
      }
    }
    if (!ttPorProdGarantia.size) return [];

    // Itens do pedido.
    const itens = await this.prisma.com_pedido_itens.findMany({
      where: { pedido_id: pedidoId },
      select: { pro_codigo: true },
    });
    const codigosPedido = [...new Set(itens.map((i) => String(i.pro_codigo)))];
    if (!codigosPedido.length) return [];

    // group_id dos produtos de garantia e dos itens do pedido (similares).
    const codigosGarantia = [...ttPorProdGarantia.keys()];
    const rels = await this.prisma.com_relacionamento_itens.findMany({
      where: { pro_codigo: { in: [...new Set([...codigosGarantia, ...codigosPedido])] } },
      select: { pro_codigo: true, group_id: true },
    });
    const groupPorProd = new Map<string, string>();
    for (const r of rels) groupPorProd.set(String(r.pro_codigo), r.group_id);

    // group_id (garantia) -> conjunto de TT_CODIGO (une os tts dos produtos do grupo).
    const ttPorGrupoGarantia = new Map<string, Set<string>>();
    for (const [cod, tts] of ttPorProdGarantia) {
      const g = groupPorProd.get(cod);
      if (!g) continue;
      if (!ttPorGrupoGarantia.has(g)) ttPorGrupoGarantia.set(g, new Set());
      for (const tt of tts) ttPorGrupoGarantia.get(g)!.add(tt);
    }

    const out: ItemPedidoGarantiaDto[] = [];
    for (const cod of codigosPedido) {
      // 1) casamento direto por pro_codigo
      const direto = ttPorProdGarantia.get(cod);
      if (direto && direto.size) {
        out.push({ pro_codigo: Number(cod), tt_codigos: [...direto], via: 'codigo' });
        continue;
      }
      // 2) casamento por produto similar (mesmo group_id)
      const g = groupPorProd.get(cod);
      const ttsGrupo = g ? ttPorGrupoGarantia.get(g) : undefined;
      if (ttsGrupo && ttsGrupo.size) {
        out.push({ pro_codigo: Number(cod), tt_codigos: [...ttsGrupo], via: 'grupo' });
      }
    }
    return out;
  }

  /**
   * Calcula a garantia do pedido e PERSISTE o resumo em com_pedido (tem_garantia
   * + contagens + valor + data). Usado no backfill (seed) e nos hooks de gerar/
   * salvar pedido. Resiliente: se a consulta ao ERP/Stage falhar, apenas loga e
   * NÃO altera o resumo (evita zerar um flag válido por uma indisponibilidade).
   */
  async recalcularGarantiaPedido(pedidoId: string) {
    let resumo: Awaited<ReturnType<GarantiaService['garantiasDoPedido']>>;
    try {
      resumo = await this.garantiasDoPedido(pedidoId);
    } catch (err: any) {
      this.logger.warn(
        `Garantia do pedido ${pedidoId} não recalculada: ${err?.message || err}`,
      );
      return null;
    }

    await this.prisma.com_pedido.update({
      where: { id: pedidoId },
      data: {
        tem_garantia: resumo.tem_garantia,
        garantia_qtd_titulos: resumo.totais.titulos,
        garantia_qtd_produtos: resumo.totais.produtos,
        garantia_valor: new Prisma.Decimal(resumo.totais.valor_total || 0),
        garantia_verificada_em: new Date(),
      },
    });

    return resumo;
  }
}
