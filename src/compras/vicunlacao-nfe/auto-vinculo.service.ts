import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';
import { NotaFiscalRepository } from '../nota fiscal/nota fiscal/notaFiscal.repository';
import { ConsultaOpenqueryRepository } from '../cotacao/openquery/openquery.repository';
import { FornecedorGrupoService } from '../fornecedor-grupo/fornecedor-grupo.service';

/** Cobertura mínima (pro_codigos distintos vinculados / nº itens do pedido) p/ sugerir. */
const COBERTURA_MINIMA = 0.3;
// Empresa GERENCIAL (pedido/cotação/fornecedor). O cadastro de fornecedor é o
// mesmo nas empresas (1=fiscal, 3=gerencial) e os códigos são iguais; usamos a
// gerencial (3), igual ao resto do sistema. A NF-e, fiscal, segue na empresa 1.
const EMPRESA = 3;

/**
 * Diferença MÁXIMA (em dias) entre a data do pedido e a data de emissão da NF
 * para considerar a NF candidata ao auto-vínculo. Configurável por AUTOVINCULO_MAX_DIAS.
 */
const MAX_DIAS_DIFERENCA =
  Number(process.env.AUTOVINCULO_MAX_DIAS) > 0 ? Number(process.env.AUTOVINCULO_MAX_DIAS) : 60;
const MS_POR_DIA = 24 * 60 * 60 * 1000;

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

  /** Evita varreduras sobrepostas quando o intervalo é curto (ex.: a cada minuto). */
  private rodando = false;

  /**
   * Disparo periódico. Intervalo configurável por env AUTOVINCULO_CRON
   * (expressão cron), default a cada 1 minuto.
   */
  @Cron(process.env.AUTOVINCULO_CRON || CronExpression.EVERY_MINUTE, {
    name: 'auto-vinculo-nfe',
  })
  async cronVarredura() {
    if (this.rodando) {
      this.logger.warn('Varredura anterior ainda em execução; pulando este disparo.');
      return;
    }
    this.rodando = true;
    try {
      await this.executarVarredura();
    } catch (err: any) {
      this.logger.error(`Falha geral na varredura de auto-vínculo: ${err?.message || err}`);
    } finally {
      this.rodando = false;
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

  /**
   * Sugestão sob demanda para UM pedido (botão "Sugerir vínculo de NF" na tela do
   * pedido). Usa exatamente o mesmo motor da varredura automática
   * (`processarPedido`: mesmos critérios de fornecedor/grupo, janela de data,
   * saldo e cobertura mínima), porém filtrado a este pedido. Retorna o nº de
   * sugestões NOVAS criadas e o total de sugestões pendentes do pedido.
   */
  async sugerirParaPedido(pedidoId: string): Promise<{
    pedido_id: string;
    sugestoes_criadas: number;
    sugestoes_pendentes: number;
  }> {
    const pedido = await this.repo.findPedidoParaAutoVinculo(pedidoId);
    if (!pedido) {
      throw new NotFoundException(`Pedido ${pedidoId} não encontrado.`);
    }

    // Fonte de NF-e SOB DEMANDA: a conciliação (Postgres), por janela de data de
    // emissão a partir do pedido. Inclui NF JÁ LANÇADA no ERP — diferente do cron,
    // que só enxerga NF não importada. Assim o botão reconcilia também pedidos
    // cujas notas já entraram (caso comum ao validar manualmente).
    const dataMax = new Date(pedido.created_at.getTime() + MAX_DIAS_DIFERENCA * MS_POR_DIA);
    let notas: NfeDisponivel[] = [];
    try {
      notas = (await this.repo.findConciliacaoCandidatas(
        pedido.created_at,
        dataMax,
      )) as unknown as NfeDisponivel[];
    } catch (err: any) {
      this.logger.error(
        `Não foi possível listar NF-e candidatas (pedido ${pedidoId}): ${err?.message || err}`,
      );
      throw err;
    }

    const sugestoesCriadas = await this.processarPedido(pedido, notas);
    const sugestoesPendentes = await this.repo.countSugestoesPendentesDoPedido(pedidoId);

    this.logger.log(
      `Sugestão sob demanda: pedido ${pedidoId} (cotação ${pedido.pedido_cotacao}) — ` +
        `${sugestoesCriadas} nova(s), ${sugestoesPendentes} pendente(s).`,
    );

    return {
      pedido_id: pedidoId,
      sugestoes_criadas: sugestoesCriadas,
      sugestoes_pendentes: sugestoesPendentes,
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

    // CNPJ do fornecedor do pedido (via OpenQuery).
    const fornecedor = await this.fornecedorRepo.findFornecedorByCodigo(
      EMPRESA,
      pedido.for_codigo,
    );
    const cnpjForn = this.soDigitos(fornecedor?.cpf_cnpj ?? null);

    // CNPJs do GRUPO do fornecedor (matriz/filiais relacionadas). Inclui o próprio.
    // Permite vincular NF emitida por um relacionado (ex.: pedido p/ CNPJ X, NF do CNPJ Y do mesmo grupo).
    const cnpjsGrupo = new Set<string>(await this.grupo.cnpjsDoGrupo(pedido.for_codigo));
    if (cnpjForn) cnpjsGrupo.add(cnpjForn);

    // Candidatas: emitente tem CNPJ do fornecedor OU de um relacionado do grupo,
    // E data de emissão posterior ao pedido (dentro de MAX_DIAS_DIFERENCA dias).
    // O casamento por NOME foi removido de propósito: gerava muitos falsos
    // positivos (razões sociais genéricas), deixando o vínculo lento e impreciso.
    let candidatas = notas.filter((n) => {
      const dataEmissao = this.toDate(n.DATA_EMISSAO);
      // NF deve ser emitida DEPOIS do pedido e dentro de MAX_DIAS_DIFERENCA dias dele.
      if (!dataEmissao || dataEmissao <= pedido.created_at) return false;
      const diffDias = (dataEmissao.getTime() - pedido.created_at.getTime()) / MS_POR_DIA;
      if (diffDias > MAX_DIAS_DIFERENCA) return false;

      const cnpjNf = this.soDigitos(n.CPF_CNPJ_EMITENTE);
      return !!cnpjNf && cnpjsGrupo.has(cnpjNf);
    });

    // Não sugere NF sem saldo (totalmente consumida por vínculos confirmados).
    if (candidatas.length) {
      const semSaldo = await this.repo.chavesSemSaldo(
        candidatas.map((n) => String(n.CHAVE_NFE ?? '').trim()).filter(Boolean),
      );
      if (semSaldo.size) {
        candidatas = candidatas.filter((n) => !semSaldo.has(String(n.CHAVE_NFE ?? '').trim()));
      }
    }

    let criadas = 0;

    // Itens da cotação são os MESMOS para todas as NF-e candidatas deste pedido:
    // busca uma única vez (Firebird + Postgres + enriquecimento) e reusa em cada
    // vincular(), em vez de refazer por NF.
    let itensCotacaoPedido:
      | Awaited<ReturnType<VinculacaoNfeService['carregarItensCotacao']>>
      | undefined;

    // "Referência no final da descrição" (ex.: ARTEB) — 1x por pedido.
    const refDescricao = await this.grupo.refNaDescricao(pedido.for_codigo);

    // Grupo do fornecedor (matriz/filiais) p/ o método 1 (relacionamento validado
    // PRODUTOS_FORNECEDOR_NFE) — expandido 1x por pedido e reusado em cada NF.
    const forCodigosGrupo = await this.grupo.expandGrupo(pedido.for_codigo);

    for (const nf of candidatas) {
      const chave = String(nf.CHAVE_NFE ?? '').trim();
      if (!chave) continue;

      // Não mexe em vínculos CONFIRMADOS nem em REJEIÇÕES. Sugestões pendentes
      // são reprocessadas (refresh) — assim, mudanças na lógica de casamento
      // corrigem sozinhas as sugestões antigas, sem precisar apagar à mão.
      const estado = await this.repo.findVinculoEstadoParaPar(pedido.id, chave);
      if (estado && (estado.confirmado || estado.rejeitado)) continue;

      let resultado: Awaited<ReturnType<VinculacaoNfeService['vincular']>>;
      try {
        if (!itensCotacaoPedido) {
          itensCotacaoPedido = await this.vinculacao.carregarItensCotacao(pedido.pedido_cotacao);
        }
        resultado = await this.vinculacao.vincular(
          pedido.pedido_cotacao,
          chave,
          pedido.for_codigo,
          { itensCotacao: itensCotacaoPedido, refDescricao, forCodigosGrupo },
        );
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

      // Re-checa logo antes de salvar: se durante o cálculo o usuário confirmou ou
      // rejeitou este par, não sobrescreve.
      const estadoAgora = await this.repo.findVinculoEstadoParaPar(pedido.id, chave);
      if (estadoAgora && (estadoAgora.confirmado || estadoAgora.rejeitado)) continue;

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

      // Conta/loga só sugestões NOVAS; refresh de sugestões pendentes é silencioso
      // (acontece a cada ciclo e poluiria o log).
      if (!estado) {
        criadas++;
        this.logger.log(
          `Sugestão criada: pedido ${pedido.id} (cotação ${pedido.pedido_cotacao}) x NF ${chave} — cobertura ${(cobertura * 100).toFixed(0)}%.`,
        );
      }
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

}
