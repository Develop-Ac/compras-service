import {
  BadGatewayException,
  BadRequestException,
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { com_precificacao_conferencia, com_precificacao_conferencia_item } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';
import { ConfirmarConferenciaDto } from './dto/confirmar-conferencia.dto';
import {
  Faixa,
  ItemCalculado,
  ItemErp,
  Semaforo,
  TABELAS,
  Tabela,
  calcularItem,
  contagens,
  faixaPara,
} from './regua';

/** A conferência é do gerencial (empresa 3); empresa 1 está fora do escopo. */
export const EMPRESA = 3;
const STATUS = ['pendente', 'conferida', 'removida'];
/** Itens que só podem ser confirmados com justificativa escrita. */
const EXIGE_JUSTIFICATIVA: Semaforo[] = ['abaixo', 'sem_custo'];
const CACHE_REGUA_MS = 60_000;

const num = (v: any): number | null => (v === null || v === undefined ? null : Number(v));
const dia = (d: Date | null) => (d ? d.toISOString().slice(0, 10) : null);

/**
 * Conferência de preços: o crivo do gerente depois que o comprador precificou
 * a NF no ERP. Lê os preços AO VIVO da erp-firebird-api (a correção é feita no
 * ERP, e a tela precisa refletir na hora) e só grava no Postgres a fila e, na
 * confirmação, a fotografia do que o gerente viu.
 */
@Injectable()
export class ConferenciaPrecosService {
  private readonly logger = new Logger(ConferenciaPrecosService.name);

  /** As faixas mudam raramente; a fila e o detalhe consultam a régua a cada NF. */
  private cacheRegua: { ate: number; faixas: Faixa[] } | null = null;

  constructor(
    private readonly prisma: PrismaService,
    private readonly erp: ErpApiService,
  ) {}

  /* --------------------------------- régua --------------------------------- */

  /**
   * Faixas de com_precificacao_faixa vigentes agora (com cache) ou vigentes num
   * instante passado — a fotografia mostra a régua do dia da confirmação, não
   * a de hoje.
   */
  async regua(em?: Date): Promise<Faixa[]> {
    if (!em && this.cacheRegua && this.cacheRegua.ate > Date.now()) return this.cacheRegua.faixas;

    const linhas = await this.prisma.com_precificacao_faixa.findMany({
      where: em
        ? { vigencia_inicio: { lte: em }, OR: [{ vigencia_fim: null }, { vigencia_fim: { gt: em } }] }
        : { vigencia_fim: null },
      orderBy: [{ tabela_preco: 'asc' }, { custo_min: 'asc' }],
    });
    const faixas: Faixa[] = linhas.map((f) => ({
      tabela_preco: f.tabela_preco,
      custo_min: Number(f.custo_min),
      custo_max: num(f.custo_max),
      markup_min_pct: Number(f.markup_min_pct),
    }));

    if (!em) this.cacheRegua = { ate: Date.now() + CACHE_REGUA_MS, faixas };
    return faixas;
  }

  /* ------------------------------ leitura do ERP ----------------------------- */

  /** Lê a NF na erp-firebird-api e calcula a régua de cada item. */
  async calcularNf(nfe: number): Promise<{ nota: any; itens: ItemCalculado[] }> {
    if (!this.erp.habilitado) {
      throw new ServiceUnavailableException('Leitura do ERP indisponível (ERP_API_URL ausente ou em pausa).');
    }

    let nota: any;
    try {
      nota = await this.erp.nfEntradaPrecos(nfe, EMPRESA);
    } catch (err: any) {
      throw new BadGatewayException(`Falha ao ler a NF ${nfe} no ERP: ${err?.message || err}`);
    }
    if (!nota) throw new NotFoundException(`NF ${nfe} não encontrada no ERP.`);

    // A mesma mercadoria pode vir em duas linhas da NF (CFOP diferente, por
    // exemplo). Preço e custo são do cadastro, então o semáforo é o mesmo; a
    // fotografia tem uma linha por produto (uq conferencia_id + pro_codigo).
    const porProduto = new Map<number, ItemErp>();
    for (const item of (nota.itens ?? []) as ItemErp[]) {
      const pro = Number(item.pro_codigo);
      const ja = porProduto.get(pro);
      if (!ja) porProduto.set(pro, { ...item });
      else if (item.quantidade !== null) ja.quantidade = (ja.quantidade ?? 0) + Number(item.quantidade);
    }

    const faixas = await this.regua();
    const itens = [...porProduto.values()].map((i) => calcularItem(i, faixas));

    for (const i of itens) {
      if (i.custo_base === null) continue;
      const semFaixa = (Object.keys(i.tabelas) as Tabela[]).filter((t) => i.tabelas[t].piso === null);
      if (semFaixa.length) {
        this.logger.warn(
          `NF ${nfe}, produto ${i.pro_codigo}: nenhuma faixa vigente para custo ${i.custo_base} em ${semFaixa.join(', ')} — tratado como sem_custo.`,
        );
      }
    }

    return { nota, itens };
  }

  /* ---------------------------------- fila ---------------------------------- */

  cabecalho(c: com_precificacao_conferencia) {
    return {
      id: c.id,
      nfe: c.nfe,
      nota_fiscal: c.nota_fiscal,
      serie: c.serie,
      chave_nfe: c.chave_nfe,
      for_codigo: c.for_codigo,
      for_nome: c.for_nome,
      opf_codigo: c.opf_codigo,
      dt_entrada: dia(c.dt_entrada),
      total_nota: num(c.total_nota),
      status: c.status,
      itens_total: c.itens_total,
      itens_abaixo: c.itens_abaixo,
      itens_sem_custo: c.itens_sem_custo,
      conferido_por: c.conferido_por,
      conferido_em: c.conferido_em,
      observacao: c.observacao,
    };
  }

  async listar(q: { status?: string; desde?: string; ate?: string }) {
    const status = q.status || 'pendente';
    if (!STATUS.includes(status)) throw new BadRequestException(`status deve ser ${STATUS.join(', ')}.`);

    const data = (v: string | undefined, nome: string) => {
      if (!v) return undefined;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(v)) throw new BadRequestException(`${nome} deve ser YYYY-MM-DD.`);
      return new Date(v);
    };
    const desde = data(q.desde, 'desde');
    const ate = data(q.ate, 'ate');

    const linhas = await this.prisma.com_precificacao_conferencia.findMany({
      where: {
        empresa: EMPRESA,
        status,
        ...(desde || ate ? { dt_entrada: { gte: desde, lte: ate } } : {}),
      },
      orderBy: [{ dt_entrada: 'desc' }, { id: 'desc' }],
    });
    return linhas.map((c) => this.cabecalho(c));
  }

  /* --------------------------------- detalhe -------------------------------- */

  private async buscar(id: number) {
    const c = await this.prisma.com_precificacao_conferencia.findUnique({ where: { id } });
    if (!c) throw new NotFoundException(`Conferência ${id} não encontrada.`);
    return c;
  }

  private reguaSaida(faixas: Faixa[]) {
    return faixas.map((f) => ({
      tabela_preco: f.tabela_preco,
      custo_min: f.custo_min,
      custo_max: f.custo_max,
      markup_min_pct: f.markup_min_pct,
    }));
  }

  /**
   * Pendente/removida: leitura ao vivo do ERP, e as contagens da fila são
   * refeitas com ela. Conferida: devolve a fotografia gravada, sem ir ao ERP.
   */
  async detalhe(id: number) {
    const c = await this.buscar(id);

    if (c.status === 'conferida') {
      const fotos = await this.prisma.com_precificacao_conferencia_item.findMany({
        where: { conferencia_id: id },
        orderBy: { id: 'asc' },
      });
      const faixas = await this.regua(c.conferido_em ?? undefined);
      return {
        conferencia: this.cabecalho(c),
        fonte: 'fotografia' as const,
        regua: this.reguaSaida(faixas),
        itens: fotos.map((f) => this.daFotografia(f, faixas)),
      };
    }

    const { itens } = await this.calcularNf(c.nfe);
    const atualizada = await this.prisma.com_precificacao_conferencia.update({
      where: { id },
      data: { ...contagens(itens), atualizado_em: new Date() },
    });
    return {
      conferencia: this.cabecalho(atualizada),
      fonte: 'erp' as const,
      regua: this.reguaSaida(await this.regua()),
      itens,
    };
  }

  /** Remonta o formato do detalhe a partir da linha gravada na confirmação. */
  private daFotografia(f: com_precificacao_conferencia_item, faixas: Faixa[]): ItemCalculado {
    const custo = num(f.custo_base);
    const tabelas = {} as ItemCalculado['tabelas'];
    for (const t of Object.keys(TABELAS) as Tabela[]) {
      tabelas[t] = {
        preco: num(f[`preco_${t}`]),
        anterior: num(f[`preco_ant_${t}`]),
        piso: num(f[`piso_${t}`]),
        // O markup não é gravado: sai da régua vigente na data da confirmação.
        markup_min_pct: custo === null ? null : (faixaPara(t, custo, faixas)?.markup_min_pct ?? null),
        semaforo: f[`semaforo_${t}`] as Semaforo,
      };
    }
    return {
      pro_codigo: f.pro_codigo,
      pro_descricao: f.pro_descricao,
      quantidade: num(f.quantidade),
      saldo_anterior: num(f.saldo_anterior),
      preco_custo: num(f.preco_custo),
      custo_medio: num(f.custo_medio),
      custo_base: custo,
      custo_base_origem: f.custo_base_origem as ItemCalculado['custo_base_origem'],
      tabelas,
      semaforo_item: f.semaforo_item as Semaforo,
      justificativa: f.justificativa,
    };
  }

  /* ------------------------------- confirmação ------------------------------ */

  /**
   * Confirmação definitiva (não existe reabrir). Relê o ERP em vez de confiar
   * no que o navegador mostrou: o preço pode ter mudado entre a abertura da
   * tela e o clique.
   */
  async confirmar(id: number, body: ConfirmarConferenciaDto, usuario: string | undefined) {
    const userId = String(usuario ?? '').trim();
    if (!userId) throw new BadRequestException('Usuário não identificado (header x-user-id).');
    // A tela mostra "conferida por <nome>"; o header traz só o id de sis_usuarios.
    const u = await this.prisma.sis_usuarios.findUnique({ where: { id: userId }, select: { nome: true } });
    const conferidoPor = (u?.nome?.trim() || userId).slice(0, 120);

    const c = await this.buscar(id);
    if (c.status !== 'pendente') throw new ConflictException(`Conferência ${id} está ${c.status}.`);

    const justificativas = new Map<number, string>();
    for (const j of Array.isArray(body?.justificativas) ? body.justificativas : []) {
      const texto = String(j?.texto ?? '').trim();
      if (texto) justificativas.set(Number(j.pro_codigo), texto);
    }

    const { nota, itens } = await this.calcularNf(c.nfe);
    if (nota.dt_cancelamento || Number(nota.status) !== 1) {
      throw new ConflictException(`NF ${c.nfe} foi cancelada ou não está lançada no ERP.`);
    }

    for (const i of itens) i.justificativa = justificativas.get(i.pro_codigo) ?? null;
    const semJustificativa = itens
      .filter((i) => EXIGE_JUSTIFICATIVA.includes(i.semaforo_item) && !i.justificativa)
      .map((i) => i.pro_codigo);
    if (semJustificativa.length) {
      throw new BadRequestException({
        message: 'Justificativa obrigatória',
        itens_sem_justificativa: semJustificativa,
      });
    }

    const observacao = String(body?.observacao ?? '').trim() || null;
    await this.prisma.$transaction(async (tx) => {
      // Condição no próprio UPDATE: duas confirmações simultâneas não gravam duas fotografias.
      const { count } = await tx.com_precificacao_conferencia.updateMany({
        where: { id, status: 'pendente' },
        data: {
          status: 'conferida',
          conferido_por: conferidoPor,
          conferido_em: new Date(),
          observacao,
          ...contagens(itens),
          atualizado_em: new Date(),
        },
      });
      if (!count) throw new ConflictException(`Conferência ${id} não está mais pendente.`);

      await tx.com_precificacao_conferencia_item.createMany({
        data: itens.map((i) => ({
          conferencia_id: id,
          pro_codigo: i.pro_codigo,
          pro_descricao: i.pro_descricao?.slice(0, 255) ?? null,
          quantidade: i.quantidade,
          saldo_anterior: i.saldo_anterior,
          preco_custo: i.preco_custo,
          custo_medio: i.custo_medio,
          custo_base: i.custo_base,
          custo_base_origem: i.custo_base_origem,
          preco_varejo: i.tabelas.varejo.preco,
          preco_atacado_esp: i.tabelas.atacado_esp.preco,
          preco_atacado: i.tabelas.atacado.preco,
          preco_ant_varejo: i.tabelas.varejo.anterior,
          preco_ant_atacado_esp: i.tabelas.atacado_esp.anterior,
          preco_ant_atacado: i.tabelas.atacado.anterior,
          piso_varejo: i.tabelas.varejo.piso,
          piso_atacado_esp: i.tabelas.atacado_esp.piso,
          piso_atacado: i.tabelas.atacado.piso,
          semaforo_varejo: i.tabelas.varejo.semaforo,
          semaforo_atacado_esp: i.tabelas.atacado_esp.semaforo,
          semaforo_atacado: i.tabelas.atacado.semaforo,
          semaforo_item: i.semaforo_item,
          justificativa: i.justificativa,
        })),
      });
    });

    return this.detalhe(id);
  }
}
