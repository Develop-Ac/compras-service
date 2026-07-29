import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';

/** Vínculo vivo produto → grupo de itens iguais (`com_relacionamento_itens`). */
export interface VinculoGrupoRow {
  pro_codigo: number;
  group_id: string;
}

/** Recorte de giro da materialização mais recente de um produto em `com_fifo_completo`. */
export interface GiroMaterializadoRow {
  pro_codigo: number;
  pro_descricao: string | null;
  /** `data_processamento` da linha mais recente — a idade do dado sai daqui. */
  materializado_em: Date | null;

  /** Grupo REFLETIDO na materialização (pode estar atrás de `com_relacionamento_itens`). */
  group_id: string | null;
  grupo_chave: string | null;
  grupo_qtd_itens: number | null;

  /** Giro real / esperado no escopo do PRODUTO. */
  demanda_media_dia: number | null;
  demanda_planejamento_dia: number | null;

  /** Giro real / esperado no escopo do GRUPO, já agregados pelo ETL. */
  grp_demanda_media_dia: number | null;
  grupo_demanda_dia: number | null;

  tempo_medio_estoque: number | null;
  fator_tendencia: number | null;
  tendencia_label: string | null;
  vendas_ult_12m: number | null;
  vendas_12m_ant: number | null;
  grp_vendas_ult_12m: number | null;
  grp_vendas_12m_ant: number | null;
}

/**
 * Acesso a dados do sinal de giro (US-039).
 *
 * **Contrato de leitura escolhido: SQL direto (Prisma) sobre o Postgres `intranet`
 * compartilhado, SOMENTE LEITURA** — `com_relacionamento_itens` (vínculo produto ↔ grupo)
 * e `com_fifo_completo` (materialização do ETL). A justificativa completa está no
 * `giro.service.ts` e em `ARCHITECTURE.md#lacunas-e-perguntas`. O dono de escrita das duas
 * tabelas continua sendo o ETL do `analise-estoque-service`; aqui nada é escrito.
 *
 * Duas leituras, de propósito:
 * 1. `com_relacionamento_itens` é o vínculo **vivo** — responde "tem grupo?" (AC 2) mesmo
 *    quando o ETL ainda não rodou depois do agrupamento;
 * 2. `com_fifo_completo` traz os **agregados já calculados** e a `data_processamento` que
 *    dá a idade do dado (AC 1 e AC 3).
 */
@Injectable()
export class GiroRepository {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Vínculo produto → grupo. `pro_codigo` é UNIQUE em `com_relacionamento_itens`, então
   * cada produto aparece no máximo uma vez. Ausência da chave = produto sem grupo.
   */
  async buscarVinculosDeGrupo(codigos: number[]): Promise<Map<number, string>> {
    const out = new Map<number, string>();
    const unicos = this.sanitizarCodigos(codigos);
    if (!unicos.length) return out;

    const linhas = await this.prisma.com_relacionamento_itens.findMany({
      where: { pro_codigo: { in: unicos.map((c) => String(c)) } },
      select: { pro_codigo: true, group_id: true },
    });

    for (const l of linhas) {
      const cod = this.inteiro(l.pro_codigo);
      const grupo = this.texto(l.group_id);
      if (cod == null || grupo == null) continue;
      out.set(cod, grupo);
    }
    return out;
  }

  /**
   * Recorte de giro da materialização **mais recente** de cada produto.
   *
   * `com_fifo_completo` guarda uma linha por processamento; `orderBy data_processamento
   * desc` + "primeira ocorrência vence" isola a última — mesma técnica de
   * `tabelas-preco.repository.ts#buscarNoEspelho`.
   */
  async buscarMaterializacao(codigos: number[]): Promise<Map<number, GiroMaterializadoRow>> {
    const out = new Map<number, GiroMaterializadoRow>();
    const unicos = this.sanitizarCodigos(codigos);
    if (!unicos.length) return out;

    const linhas = await this.prisma.com_fifo_completo.findMany({
      where: { pro_codigo: { in: unicos.map((c) => String(c)) } },
      select: {
        pro_codigo: true,
        pro_descricao: true,
        data_processamento: true,
        group_id: true,
        grupo_chave: true,
        grupo_qtd_itens: true,
        demanda_media_dia: true,
        demanda_planejamento_dia: true,
        grp_demanda_media_dia: true,
        grupo_demanda_dia: true,
        tempo_medio_estoque: true,
        fator_tendencia: true,
        tendencia_label: true,
        vendas_ult_12m: true,
        vendas_12m_ant: true,
        grp_vendas_ult_12m: true,
        grp_vendas_12m_ant: true,
      },
      orderBy: { data_processamento: 'desc' },
    });

    for (const l of linhas) {
      const cod = this.inteiro(l.pro_codigo);
      if (cod == null || out.has(cod)) continue;
      out.set(cod, {
        pro_codigo: cod,
        pro_descricao: this.texto(l.pro_descricao),
        materializado_em: l.data_processamento ?? null,
        group_id: this.texto(l.group_id),
        grupo_chave: this.texto(l.grupo_chave),
        grupo_qtd_itens: this.inteiro(l.grupo_qtd_itens),
        demanda_media_dia: this.decimal(l.demanda_media_dia),
        demanda_planejamento_dia: this.decimal(l.demanda_planejamento_dia),
        grp_demanda_media_dia: this.decimal(l.grp_demanda_media_dia),
        grupo_demanda_dia: this.decimal(l.grupo_demanda_dia),
        tempo_medio_estoque: this.decimal(l.tempo_medio_estoque),
        fator_tendencia: this.decimal(l.fator_tendencia),
        tendencia_label: this.texto(l.tendencia_label),
        vendas_ult_12m: this.decimal(l.vendas_ult_12m),
        vendas_12m_ant: this.decimal(l.vendas_12m_ant),
        grp_vendas_ult_12m: this.decimal(l.grp_vendas_ult_12m),
        grp_vendas_12m_ant: this.decimal(l.grp_vendas_12m_ant),
      });
    }
    return out;
  }

  // ---------------------------------------------------------------------------
  // Utilitários (mesmas regras de coerção de `tabelas-preco.repository.ts`)
  // ---------------------------------------------------------------------------

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
