import { ConflictException, Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { ErpApiService } from '../../shared/erp-api/erp-api.service';
import { ConferenciaPrecosService, EMPRESA } from './conferencia-precos.service';
import { contagens } from './regua';

const MAX_EM = 500;
const txt = (v: any): string | null => (v === null || v === undefined ? null : String(v).trim() || null);
const numero = (v: any): number | null => (v === null || v === undefined || v === '' ? null : Number(v));

/**
 * Varredura da fila da conferência de preços.
 *
 * Lê do ERP as NFs de compra para revenda já lançadas e põe na fila as que
 * ainda não estão. É varredura (e não o aviso do fiscal-service) porque pega
 * também NF lançada à mão no ERP. NF pendente que deixou de atender sai da
 * fila como `removida`; NF conferida nunca muda.
 */
@Injectable()
export class ConferenciaPrecosVarreduraService {
  private readonly logger = new Logger(ConferenciaPrecosVarreduraService.name);

  /** Evita varreduras sobrepostas (cron curto + disparo manual). */
  private rodando = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly erp: ErpApiService,
    private readonly conferencia: ConferenciaPrecosService,
  ) {}

  /** Intervalo por env CONFERENCIA_PRECO_CRON (expressão cron), padrão a cada 5 minutos. */
  @Cron(process.env.CONFERENCIA_PRECO_CRON || CronExpression.EVERY_5_MINUTES, {
    name: 'conferencia-precos-varredura',
  })
  async cronVarredura() {
    if (this.rodando) {
      this.logger.warn('Varredura anterior ainda em execução; pulando este disparo.');
      return;
    }
    try {
      await this.varrer();
    } catch (err: any) {
      this.logger.error(`Falha geral na varredura da conferência de preços: ${err?.message || err}`);
    }
  }

  /** Disparo manual (POST /varredura) e do cron, sob a mesma trava. */
  async varrer(): Promise<{ inseridas: number; removidas: number }> {
    if (this.rodando) throw new ConflictException('Varredura já em execução.');
    this.rodando = true;
    try {
      return await this.executar();
    } finally {
      this.rodando = false;
    }
  }

  private async executar(): Promise<{ inseridas: number; removidas: number }> {
    const nada = { inseridas: 0, removidas: 0 };

    // Sem data de corte a varredura traria o histórico inteiro de NFs para a
    // fila; as anteriores ao deploy estão fora do escopo.
    const desde = String(process.env.CONFERENCIA_PRECO_DESDE ?? '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde)) {
      this.logger.warn('CONFERENCIA_PRECO_DESDE ausente ou fora de YYYY-MM-DD; varredura não executada.');
      return nada;
    }
    if (!this.erp.habilitado) {
      this.logger.warn('Leitura do ERP indisponível (ERP_API_URL ausente ou em pausa); varredura não executada.');
      return nada;
    }

    // Operações de compra para revenda: a mesma tabela que a tela de regras
    // fiscais edita. Os códigos lá são texto.
    const opfs = (
      await this.prisma.com_fiscal_opf_destinacao.findMany({
        where: { destinacao: 'COMERCIALIZACAO', ativo: true },
        select: { opf_codigo: true },
      })
    )
      .map((o) => parseInt(o.opf_codigo, 10))
      .filter(Number.isFinite);
    if (!opfs.length) {
      this.logger.warn('Nenhuma OPF ativa com destinação COMERCIALIZACAO; varredura não executada.');
      return nada;
    }

    // exigirCompleto: resultado truncado faria NF válida parecer "sumida" e ser removida.
    const notas = await this.erp.nfEntradaConsulta({
      empresa: EMPRESA,
      campos: [
        'NFE', 'NOTA_FISCAL', 'SERIE', 'CHAVE_NFE', 'FOR_CODIGO', 'fornecedor.FOR_NOME',
        'OPF_CODIGO', 'DT_ENTRADA', 'TOTAL_NOTA',
      ],
      filtros: [
        { campo: 'STATUS', op: 'igual', valor: 1 },
        { campo: 'MODELO_NOTA', op: 'igual', valor: 55 },
        { campo: 'DT_CANCELAMENTO', op: 'nulo' },
        { campo: 'DT_ENTRADA', op: 'maior_igual', valor: desde },
        { campo: 'OPF_CODIGO', op: 'em', valor: opfs },
      ],
      limite: 20_000,
    });
    const nfesLidas = new Set(notas.map((n) => Number(n.NFE)));

    const existentes = await this.prisma.com_precificacao_conferencia.findMany({
      where: { empresa: EMPRESA, OR: [{ status: 'pendente' }, { nfe: { in: [...nfesLidas] } }] },
      select: { id: true, nfe: true, status: true },
    });
    const porNfe = new Map(existentes.map((e) => [e.nfe, e]));

    let inseridas = 0;
    for (const n of notas) {
      const nfe = Number(n.NFE);
      const atual = porNfe.get(nfe);
      if (atual) {
        // Removida que voltou a atender (ex.: estornada e relançada) volta para a fila.
        if (atual.status === 'removida') {
          await this.prisma.com_precificacao_conferencia.updateMany({
            where: { id: atual.id, status: 'removida' },
            data: { status: 'pendente', removida_motivo: null, atualizado_em: new Date() },
          });
          this.logger.log(`NF ${nfe} voltou a atender o critério; reaberta na fila.`);
        }
        continue;
      }

      // Uma NF por vez: a rota de preços faz várias leituras no Firebird por NF.
      // Falha numa NF não derruba a varredura; ela entra no ciclo seguinte.
      try {
        const { itens } = await this.conferencia.calcularNf(nfe);
        await this.prisma.com_precificacao_conferencia.create({
          data: {
            empresa: EMPRESA,
            nfe,
            nota_fiscal: Number(n.NOTA_FISCAL),
            serie: txt(n.SERIE),
            chave_nfe: txt(n.CHAVE_NFE),
            for_codigo: numero(n.FOR_CODIGO),
            for_nome: txt(n.FOR_NOME)?.slice(0, 255) ?? null,
            opf_codigo: numero(n.OPF_CODIGO),
            dt_entrada: new Date(String(n.DT_ENTRADA).slice(0, 10)),
            total_nota: numero(n.TOTAL_NOTA),
            ...contagens(itens),
          },
        });
        inseridas++;
      } catch (err: any) {
        this.logger.error(`NF ${nfe}: não entrou na fila nesta varredura: ${err?.message || err}`);
      }
    }

    const sumidas = existentes.filter((e) => e.status === 'pendente' && !nfesLidas.has(e.nfe));
    const removidas = sumidas.length ? await this.remover(sumidas) : 0;

    if (inseridas || removidas) {
      this.logger.log(`Conferência de preços: ${inseridas} NF(s) inserida(s), ${removidas} removida(s).`);
    }
    return { inseridas, removidas };
  }

  /** Marca como removidas as pendentes que saíram da leitura, com o motivo lido do ERP. */
  private async remover(sumidas: Array<{ id: number; nfe: number }>): Promise<number> {
    const noErp = new Map<number, any>();
    for (let i = 0; i < sumidas.length; i += MAX_EM) {
      const lote = sumidas.slice(i, i + MAX_EM).map((s) => s.nfe);
      const linhas = await this.erp.nfEntradaConsulta({
        empresa: EMPRESA,
        campos: ['NFE', 'STATUS', 'DT_CANCELAMENTO'],
        filtros: [{ campo: 'NFE', op: 'em', valor: lote }],
        limite: lote.length + 1,
      });
      for (const l of linhas) noErp.set(Number(l.NFE), l);
    }

    let removidas = 0;
    for (const s of sumidas) {
      const l = noErp.get(s.nfe);
      const motivo = !l
        ? 'nao_encontrada_no_erp'
        : l.DT_CANCELAMENTO
          ? 'cancelada'
          : Number(l.STATUS) !== 1
            ? 'status<>1'
            : 'fora_do_criterio'; // OPF, modelo ou data deixaram de atender
      // Condição no UPDATE: uma confirmação que chegou no meio da varredura vence.
      const { count } = await this.prisma.com_precificacao_conferencia.updateMany({
        where: { id: s.id, status: 'pendente' },
        data: { status: 'removida', removida_motivo: motivo, atualizado_em: new Date() },
      });
      removidas += count;
    }
    return removidas;
  }
}
