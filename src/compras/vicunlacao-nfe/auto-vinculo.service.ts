import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';
import { NotaFiscalRepository } from '../nota fiscal/nota fiscal/notaFiscal.repository';
import { ConsultaOpenqueryRepository } from '../cotacao/openquery/openquery.repository';
import { FornecedorGrupoService } from '../fornecedor-grupo/fornecedor-grupo.service';

/** Cobertura mínima (pro_codigos distintos vinculados / nº itens do pedido) p/ sugerir. */
const COBERTURA_MINIMA = 0.3;
const EMPRESA = 1;

/** Linha de NF-e disponível (campos vindos do OPENQUERY de fetchNfeDisponiveis). */
interface NfeDisponivel {
  CHAVE_NFE: string;
  CPF_CNPJ_EMITENTE: string | null;
  NOME_EMITENTE: string | null;
  DATA_EMISSAO: Date | string | null;
}

/**
 * Job periódico de vínculo automático SUGERIDO.
 *
 * Varre pedidos abertos, busca NF-e candidatas (emitente com qualquer CNPJ do
 * GRUPO do fornecedor — matriz/filiais — ou nome similar, + data de emissão
 * posterior ao pedido), roda o motor de casamento e,
 * se a cobertura >= 30%, grava uma sugestão (confirmado=false, origem='auto') e
 * marca o pedido como 'Vínculo sugerido'. Nada é confirmado automaticamente.
 */
@Injectable()
export class AutoVinculoService {
  private readonly logger = new Logger(AutoVinculoService.name);

  constructor(
    private readonly vinculacao: VinculacaoNfeService,
    private readonly repo: VinculacaoNfeRepository,
    private readonly notasRepo: NotaFiscalRepository,
    private readonly fornecedorRepo: ConsultaOpenqueryRepository,
    private readonly grupo: FornecedorGrupoService,
  ) {}

  /**
   * Disparo periódico. Intervalo configurável por env AUTOVINCULO_CRON
   * (expressão cron), default a cada 30 minutos.
   */
  @Cron(process.env.AUTOVINCULO_CRON || CronExpression.EVERY_30_MINUTES, {
    name: 'auto-vinculo-nfe',
  })
  async cronVarredura() {
    try {
      await this.executarVarredura();
    } catch (err: any) {
      this.logger.error(`Falha geral na varredura de auto-vínculo: ${err?.message || err}`);
    }
  }

  /**
   * Executa a varredura completa. Retorna um resumo (útil p/ disparo manual).
   */
  async executarVarredura(): Promise<{
    pedidos_varridos: number;
    sugestoes_criadas: number;
    truncado: boolean;
    limite: number;
  }> {
    const limite = this.lerLimite();

    const pedidos = await this.repo.findPedidosAbertosParaAutoVinculo(limite + 1);
    const truncado = pedidos.length > limite;
    const pedidosProcessar = truncado ? pedidos.slice(0, limite) : pedidos;

    if (truncado) {
      this.logger.warn(
        `Varredura truncada: há mais de ${limite} pedidos abertos. Processando ${limite} nesta execução (AUTOVINCULO_LIMITE).`,
      );
    }

    // NF-e disponíveis (1 chamada OPENQUERY p/ toda a varredura).
    let notas: NfeDisponivel[] = [];
    try {
      notas = (await this.notasRepo.fetchNfeDisponiveis()) as unknown as NfeDisponivel[];
    } catch (err: any) {
      this.logger.error(`Não foi possível listar NF-e disponíveis: ${err?.message || err}`);
      return { pedidos_varridos: 0, sugestoes_criadas: 0, truncado, limite };
    }

    let sugestoesCriadas = 0;

    for (const pedido of pedidosProcessar) {
      try {
        sugestoesCriadas += await this.processarPedido(pedido, notas);
      } catch (err: any) {
        this.logger.error(
          `Erro ao processar pedido ${pedido.id} (cotação ${pedido.pedido_cotacao}): ${err?.message || err}`,
        );
      }
    }

    this.logger.log(
      `Auto-vínculo: ${pedidosProcessar.length} pedido(s) varrido(s), ${sugestoesCriadas} sugestão(ões) criada(s)${truncado ? ' (truncado)' : ''}.`,
    );

    return {
      pedidos_varridos: pedidosProcessar.length,
      sugestoes_criadas: sugestoesCriadas,
      truncado,
      limite,
    };
  }

  /** Processa um pedido contra as NF-e disponíveis. Retorna nº de sugestões criadas. */
  private async processarPedido(
    pedido: {
      id: string;
      pedido_cotacao: number;
      for_codigo: number;
      status: string | null;
      created_at: Date;
    },
    notas: NfeDisponivel[],
  ): Promise<number> {
    // nº de itens (pro_codigo distintos) do pedido — denominador da cobertura.
    const totalItens = await this.repo.countProCodigosDoPedido(pedido.id);
    if (totalItens === 0) return 0;

    // Fornecedor do pedido (CNPJ + nome) via OpenQuery.
    const fornecedor = await this.fornecedorRepo.findFornecedorByCodigo(
      EMPRESA,
      pedido.for_codigo,
    );
    const cnpjForn = this.soDigitos(fornecedor?.cpf_cnpj ?? null);
    const nomeForn = fornecedor?.for_nome ?? null;

    // CNPJs do GRUPO do fornecedor (matriz/filiais relacionadas). Inclui o próprio.
    // Permite vincular NF emitida por um relacionado (ex.: pedido p/ CNPJ X, NF do CNPJ Y do mesmo grupo).
    const cnpjsGrupo = new Set<string>(await this.grupo.cnpjsDoGrupo(pedido.for_codigo));
    if (cnpjForn) cnpjsGrupo.add(cnpjForn);

    // Candidatas: emitente é do grupo (qualquer CNPJ do grupo) OU nome similar E data de emissão > pedido.
    const candidatas = notas.filter((n) => {
      const dataEmissao = this.toDate(n.DATA_EMISSAO);
      if (!dataEmissao || dataEmissao <= pedido.created_at) return false;

      const cnpjNf = this.soDigitos(n.CPF_CNPJ_EMITENTE);
      const cnpjBate = !!cnpjNf && cnpjsGrupo.has(cnpjNf);
      const nomeBate = this.nomeSimilar(nomeForn, n.NOME_EMITENTE);
      return cnpjBate || nomeBate;
    });

    let criadas = 0;

    for (const nf of candidatas) {
      const chave = String(nf.CHAVE_NFE ?? '').trim();
      if (!chave) continue;

      // Evita duplicar: já existe vínculo (qualquer) p/ esse par pedido+chave.
      if (await this.repo.existeVinculoParaPar(pedido.id, chave)) continue;

      let resultado: Awaited<ReturnType<VinculacaoNfeService['vincular']>>;
      try {
        resultado = await this.vinculacao.vincular(pedido.pedido_cotacao, chave);
      } catch (err: any) {
        this.logger.warn(
          `Casamento falhou p/ pedido ${pedido.id} x chave ${chave}: ${err?.message || err}`,
        );
        continue;
      }

      // Cobertura = pro_codigos distintos vinculados / nº itens do pedido.
      const proCodigosVinculados = new Set(
        resultado.vinculados
          .map((v) => v.pro_codigo)
          .filter((c) => c != null)
          .map((c) => String(c)),
      );
      const cobertura = proCodigosVinculados.size / totalItens;
      if (cobertura < COBERTURA_MINIMA) continue;

      await this.vinculacao.salvarSugestao({
        pedido_id: pedido.id,
        pedido_cotacao: pedido.pedido_cotacao,
        for_codigo: pedido.for_codigo,
        chave_nfe: chave,
        emitente: nf.NOME_EMITENTE ?? null,
        data_emissao: this.toDate(nf.DATA_EMISSAO),
        valor_total: null,
        resultado,
      });

      await this.repo.marcarPedidoVinculoSugerido(pedido.id);
      criadas++;

      this.logger.log(
        `Sugestão criada: pedido ${pedido.id} (cotação ${pedido.pedido_cotacao}) x NF ${chave} — cobertura ${(cobertura * 100).toFixed(0)}%.`,
      );
    }

    return criadas;
  }

  // ------------------------------- Helpers -----------------------------------

  private lerLimite(): number {
    const raw = Number(process.env.AUTOVINCULO_LIMITE);
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 100;
  }

  private soDigitos(s: string | null): string {
    return s == null ? '' : String(s).replace(/\D/g, '');
  }

  private toDate(v: Date | string | null): Date | null {
    if (v == null) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  /**
   * Normaliza um nome de empresa: maiúsculas, sem acento, sem sufixos
   * societários (LTDA/ME/EPP/SA/EIRELI/MEI) e sem pontuação.
   */
  private normalizarNome(s: string | null): string {
    if (!s) return '';
    let t = String(s)
      .normalize('NFKD')
      .replace(/[̀-ͯ]/g, '')
      .toUpperCase();
    t = t.replace(/[^A-Z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    const sufixos = new Set([
      'LTDA',
      'ME',
      'EPP',
      'SA',
      'S',
      'A',
      'EIRELI',
      'MEI',
      'CIA',
      'COMPANHIA',
    ]);
    return t
      .split(' ')
      .filter((tk) => tk && !sufixos.has(tk))
      .join(' ')
      .trim();
  }

  /**
   * Nomes "similares": após normalização, um contém o outro OU a interseção de
   * tokens é >= 3.
   */
  private nomeSimilar(a: string | null, b: string | null): boolean {
    const na = this.normalizarNome(a);
    const nb = this.normalizarNome(b);
    if (!na || !nb) return false;
    if (na === nb) return true;
    if (na.includes(nb) || nb.includes(na)) return true;

    const ta = new Set(na.split(' ').filter(Boolean));
    const tb = new Set(nb.split(' ').filter(Boolean));
    let inter = 0;
    for (const tk of ta) if (tb.has(tk)) inter++;
    return inter >= 3;
  }
}
