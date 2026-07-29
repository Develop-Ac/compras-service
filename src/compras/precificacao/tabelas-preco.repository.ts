import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { OpenQueryService } from '../../shared/database/openquery/openquery.service';

/** Uma linha de `produtos` do ERP com as três tabelas de preço vigentes. */
export interface PrecosErpRow {
  pro_codigo: number;
  pro_descricao: string | null;
  /** `pro.preco_venda` — varejo. */
  varejo: number | null;
  /** `pro.preco2` — atacado especial. */
  atacado_especial: number | null;
  /** `pro.preco5` — atacado. NÃO é espelhado em `com_fifo_completo`. */
  atacado: number | null;
}

/** Recorte do espelho `com_fifo_completo` — preços + idade do espelhamento. */
export interface PrecosEspelhoRow {
  pro_codigo: number;
  pro_descricao: string | null;
  varejo: number | null;
  atacado_especial: number | null;
  /** `data_processamento` da linha mais recente do produto. */
  atualizado_em: Date | null;
}

/**
 * Acesso a dados das três tabelas de preço (US-038).
 *
 * Duas fontes, ambas SOMENTE LEITURA:
 * 1. **ERP** (`produtos` via `OpenQueryService` → OPENQUERY `[CONSULTA]`, conforme
 *    `CODING_STANDARDS.md#convencoes-backend`) — fonte da verdade das três tabelas,
 *    incluindo o **atacado (`preco5`), que o ETL do `analise-estoque-service` não
 *    espelha**. Ler o ERP aqui é o que garante os ACs "nenhum nulo quando o ERP tem
 *    valor cadastrado" e "atacado disponível para 100% dos produtos que o têm no ERP":
 *    o espelho só cobre produtos ativos e comercializáveis (`inativo='N'`,
 *    `comercializavel='S'`).
 * 2. **Espelho** `com_fifo_completo` (Postgres, via Prisma) — dono de escrita é o ETL
 *    do `analise-estoque-service`; aqui é lido para (a) informar a idade do dado
 *    espelhado e (b) servir de fallback degradado quando o ERP não responde.
 */
@Injectable()
export class TabelasPrecoRepository {
  private readonly logger = new Logger(TabelasPrecoRepository.name);

  /** Empresa do grupo usada em todas as leituras do ERP (mesma do ETL e do cotacao-sync). */
  private readonly empresa = 3;

  constructor(
    private readonly prisma: PrismaService,
    private readonly openQuery: OpenQueryService,
  ) {}

  /**
   * Lê as três tabelas de preço no ERP para os códigos informados.
   *
   * Devolve `null` (e não lança) quando o ERP está indisponível: o serviço degrada
   * para o espelho em vez de derrubar a consulta. `null` ≠ `Map` vazio — vazio
   * significa "ERP respondeu e não conhece esses códigos".
   */
  async buscarNoErp(codigos: number[]): Promise<Map<number, PrecosErpRow> | null> {
    const unicos = this.sanitizarCodigos(codigos);
    if (!unicos.length) return new Map();

    // Lista literal só com inteiros já sanitizados — parâmetros do driver não
    // atravessam o literal do OPENQUERY (mesma restrição do ETL e do cotacao-sync).
    const inList = unicos.join(',');
    const texto = `
      SELECT * FROM OPENQUERY(CONSULTA, '
          SELECT
              pro.pro_codigo,
              pro.pro_descricao,
              pro.preco_venda AS PRECO_VAREJO,
              pro.preco2      AS PRECO_ATACADO_ESPECIAL,
              pro.preco5      AS PRECO_ATACADO
          FROM produtos pro
          WHERE pro.empresa = ${this.empresa}
            AND pro.pro_codigo IN (${inList})
      ')
    `;

    try {
      const linhas = await this.openQuery.query<Record<string, unknown>>(
        texto,
        {},
        { allowZeroRows: true },
      );
      const out = new Map<number, PrecosErpRow>();
      for (const l of linhas) {
        const cod = this.inteiro(this.campo(l, 'pro_codigo'));
        if (cod == null) continue;
        out.set(cod, {
          pro_codigo: cod,
          pro_descricao: this.texto(this.campo(l, 'pro_descricao')),
          varejo: this.decimal(this.campo(l, 'preco_varejo')),
          atacado_especial: this.decimal(this.campo(l, 'preco_atacado_especial')),
          atacado: this.decimal(this.campo(l, 'preco_atacado')),
        });
      }
      return out;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(
        `[tabelas-preco] ERP indisponível (${msg}); degradando para o espelho com_fifo_completo — o atacado (preco5) ficará ausente.`,
      );
      return null;
    }
  }

  /**
   * Lê varejo e atacado especial no espelho `com_fifo_completo`, junto da
   * `data_processamento` da materialização mais recente de cada produto.
   */
  async buscarNoEspelho(codigos: number[]): Promise<Map<number, PrecosEspelhoRow>> {
    const out = new Map<number, PrecosEspelhoRow>();
    const unicos = this.sanitizarCodigos(codigos);
    if (!unicos.length) return out;

    const linhas = await this.prisma.com_fifo_completo.findMany({
      where: { pro_codigo: { in: unicos.map((c) => String(c)) } },
      select: {
        pro_codigo: true,
        pro_descricao: true,
        preco_venda_1: true,
        preco_venda_2: true,
        data_processamento: true,
      },
      orderBy: { data_processamento: 'desc' },
    });

    // `orderBy desc` + "primeira ocorrência vence" = materialização mais recente
    // por produto (a tabela guarda uma linha por processamento).
    for (const l of linhas) {
      const cod = this.inteiro(l.pro_codigo);
      if (cod == null || out.has(cod)) continue;
      out.set(cod, {
        pro_codigo: cod,
        pro_descricao: this.texto(l.pro_descricao),
        varejo: this.decimal(l.preco_venda_1),
        atacado_especial: this.decimal(l.preco_venda_2),
        atualizado_em: l.data_processamento ?? null,
      });
    }
    return out;
  }

  // ---------------------------------------------------------------------------
  // Utilitários
  // ---------------------------------------------------------------------------

  /** Só inteiros finitos, deduplicados — barra qualquer injeção na lista literal. */
  private sanitizarCodigos(codigos: number[]): number[] {
    return Array.from(
      new Set(
        (codigos ?? [])
          .map((c) => Number(c))
          .filter((n) => Number.isFinite(n))
          .map((n) => Math.trunc(n)),
      ),
    );
  }

  /** Acesso case-insensitive: o driver pode devolver a coluna em qualquer caixa. */
  private campo(linha: Record<string, unknown>, nome: string): unknown {
    if (nome in linha) return linha[nome];
    const alvo = nome.toLowerCase();
    for (const [k, v] of Object.entries(linha)) {
      if (k.toLowerCase() === alvo) return v;
    }
    return undefined;
  }

  private inteiro(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).trim());
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }

  /** `Decimal` do Prisma, string do driver ou number — tudo vira number|null. */
  private decimal(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).replace(',', '.').trim());
    return Number.isFinite(n) ? n : null;
  }

  private texto(v: unknown): string | null {
    if (v == null) return null;
    const s = String(v).trim();
    return s.length ? s : null;
  }
}
