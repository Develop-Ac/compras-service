import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { TabelaPreco } from './dto/preco-vigente.dto';

/** Uma linha de `com_precificacao_faixa`, já com os `Decimal` convertidos. */
export interface FaixaReguaRow {
  id: number;
  tabela: TabelaPreco;
  /** Custo mínimo da faixa — INCLUSIVO. */
  custo_min: number;
  /** Custo máximo — EXCLUSIVO; `null` = sem limite (última faixa). */
  custo_max: number | null;
  /** Piso inviolável de markup, em %. */
  markup_min_pct: number;
  /** Teto de markup, em %. */
  markup_max_pct: number;
  vigencia_inicio: Date;
  vigencia_fim: Date | null;
  criado_por: string;
  criado_em: Date;
  encerrado_por: string | null;
  encerrado_em: Date | null;
  motivo: string | null;
}

/** Dados de uma nova versão de faixa (a antiga é encerrada, nunca alterada). */
export interface NovaFaixaRegua {
  tabela: TabelaPreco;
  custo_min: number;
  custo_max: number | null;
  markup_min_pct: number;
  markup_max_pct: number;
  autor: string;
  motivo: string | null;
}

/**
 * Acesso a dados da régua de margem (US-040 / T-024).
 *
 * Fonte única: `com_precificacao_faixa` no Postgres `intranet` (nativa, dono de
 * escrita é ESTE serviço — não é espelho de ERP). DDL manual em
 * `sql/2026-07-30_precificacao_faixas.sql`; o Prisma só gera o client.
 *
 * **Versionamento append-only:** `substituirFaixa` nunca faz UPDATE dos limites da
 * faixa. Ela encerra a linha vigente (`vigencia_fim`, `encerrado_por`,
 * `encerrado_em`) e insere uma nova, dentro de UMA transação — a tabela é o próprio
 * histórico auditável exigido pelo DoD da US-040 ("quem alterou, quando"). Como
 * consequência, `(tabela_preco, custo_min)` repete entre linhas encerradas: a
 * unicidade é um índice PARCIAL (`WHERE vigencia_fim IS NULL`) e **nunca** se usa
 * `findUnique` por essa combinação.
 */
@Injectable()
export class ReguaRepository {
  private readonly logger = new Logger(ReguaRepository.name);

  constructor(private readonly prisma: PrismaService) {}

  /** Todas as faixas VIGENTES, de todas as tabelas, ordenadas por (tabela, custo). */
  async faixasVigentes(): Promise<FaixaReguaRow[]> {
    const linhas = await this.prisma.com_precificacao_faixa.findMany({
      where: { vigencia_fim: null },
      orderBy: [{ tabela_preco: 'asc' }, { custo_min: 'asc' }],
    });
    return linhas.map((l) => this.mapear(l));
  }

  /**
   * Trilha de auditoria: vigentes + encerradas, mais recente primeiro. Sem filtro,
   * devolve a régua inteira; com `tabela`, só a tabela pedida.
   */
  async historico(tabela?: TabelaPreco): Promise<FaixaReguaRow[]> {
    const linhas = await this.prisma.com_precificacao_faixa.findMany({
      where: tabela ? { tabela_preco: tabela } : {},
      orderBy: [{ vigencia_inicio: 'desc' }, { id: 'desc' }],
    });
    return linhas.map((l) => this.mapear(l));
  }

  /**
   * Para cada faixa recebida: encerra a vigente de `(tabela, custo_min)` — quando
   * existir — e insere a nova versão. **Tudo em UMA transação**, porque dividir uma
   * faixa em duas exige mexer em duas linhas e uma aplicação parcial deixaria a
   * régua com buraco de cobertura (o que o AC3 proíbe).
   *
   * `agora` entra por parâmetro (e não `new Date()` aqui dentro) para que o instante
   * do encerramento e o do início da nova vigência sejam **o mesmo** em todas as
   * linhas do lote: sem isso a trilha teria um vão de milissegundos em que nenhuma
   * faixa vale.
   */
  async substituirFaixas(novas: NovaFaixaRegua[], agora: Date): Promise<FaixaReguaRow[]> {
    const criadas = await this.prisma.$transaction(async (tx) => {
      const out: Record<string, unknown>[] = [];
      for (const nova of novas) {
        const encerradas = await tx.com_precificacao_faixa.updateMany({
          where: {
            tabela_preco: nova.tabela,
            custo_min: nova.custo_min,
            vigencia_fim: null,
          },
          data: {
            vigencia_fim: agora,
            encerrado_por: nova.autor,
            encerrado_em: agora,
          },
        });

        const inserida = await tx.com_precificacao_faixa.create({
          data: {
            tabela_preco: nova.tabela,
            custo_min: nova.custo_min,
            custo_max: nova.custo_max,
            markup_min_pct: nova.markup_min_pct,
            markup_max_pct: nova.markup_max_pct,
            vigencia_inicio: agora,
            criado_por: nova.autor,
            criado_em: agora,
            motivo: nova.motivo,
          },
        });

        this.logger.log(
          `[regua] faixa ${nova.tabela} custo_min=${nova.custo_min} ` +
            `${encerradas.count ? 'substituída' : 'criada'} por ${nova.autor} ` +
            `(markup ${nova.markup_min_pct}%–${nova.markup_max_pct}%).`,
        );
        out.push(inserida as Record<string, unknown>);
      }
      return out;
    });

    return criadas.map((l) => this.mapear(l));
  }

  // ---------------------------------------------------------------------------
  // Utilitários
  // ---------------------------------------------------------------------------

  private mapear(l: Record<string, unknown>): FaixaReguaRow {
    return {
      id: Number(l.id),
      tabela: String(l.tabela_preco) as TabelaPreco,
      custo_min: this.decimal(l.custo_min) ?? 0,
      custo_max: this.decimal(l.custo_max),
      markup_min_pct: this.decimal(l.markup_min_pct) ?? 0,
      markup_max_pct: this.decimal(l.markup_max_pct) ?? 0,
      vigencia_inicio: l.vigencia_inicio as Date,
      vigencia_fim: (l.vigencia_fim as Date | null) ?? null,
      criado_por: String(l.criado_por ?? ''),
      criado_em: l.criado_em as Date,
      encerrado_por: (l.encerrado_por as string | null) ?? null,
      encerrado_em: (l.encerrado_em as Date | null) ?? null,
      motivo: (l.motivo as string | null) ?? null,
    };
  }

  /** `Decimal` do Prisma, string ou number — tudo vira number|null. */
  private decimal(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).replace(',', '.').trim());
    return Number.isFinite(n) ? n : null;
  }
}
