import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import type {
  ExecucaoTriagemDto,
  RegistroTriagemDto,
  SerieTriagemDto,
  TriagemNotaDto,
} from './dto/triagem-nota.dto';

/** Teto de linhas devolvidas pela série — a consulta é de acompanhamento, não de exportação. */
const LIMITE_SERIE_PADRAO = 200;
const LIMITE_SERIE_MAX = 1000;

/** Arquivo da DDL manual que sustenta a série — citado no motivo da indisponibilidade. */
export const DDL_TRIAGEM = 'sql/2026-07-30_precificacao_triagem_execucao.sql';

/**
 * Série histórica da taxa de exceção (US-043 / T-030).
 *
 * ---
 * ## Por que a escrita é best-effort, e não um pré-requisito
 *
 * DDL neste projeto é **manual** e sua aplicação é ato do responsável
 * (CODING_STANDARDS#do). A régua da T-024 mostrou o preço de amarrar a função à DDL: sem
 * `com_precificacao_faixa` aplicada, a régua responde 503 e a proposta propaga o 503 — a
 * funcionalidade fica **meio-desligada** até alguém rodar o `.sql`.
 *
 * A triagem não repete isso. Classificar a nota e medir a taxa **não dependem** desta tabela:
 * a taxa da execução corrente sai sempre na resposta. O que a tabela acrescenta é a **série**
 * — comparar a taxa de hoje com a de semanas atrás —, e essa é a única coisa que fica
 * pendente até a DDL ser aplicada. Enquanto isso, `RegistroTriagemDto.indisponivel_motivo`
 * diz exatamente o que falta, em vez de a rota falhar ou, pior, fingir que gravou.
 *
 * O erro é capturado e **logado como warn, nunca engolido em silêncio** — mas não sobe: o
 * comprador não perde a triagem porque a série não está provisionada.
 */
@Injectable()
export class TriagemRepository {
  private readonly log = new Logger(TriagemRepository.name);

  constructor(private readonly prisma: PrismaService) {}

  /** Grava UMA execução na série. Nunca lança. */
  async registrar(dto: TriagemNotaDto): Promise<RegistroTriagemDto> {
    try {
      const linha = await this.prisma.com_precificacao_triagem_execucao.create({
        data: {
          pedido_id: dto.pedido_id,
          chave_nfe: dto.chave_nfe,
          for_codigo: dto.for_codigo,
          itens: dto.taxa.itens,
          automaticos: dto.taxa.automaticos,
          excecoes: dto.taxa.excecoes,
          taxa_excecao_pct: dto.taxa.taxa_excecao_pct,
          meta_pct: dto.taxa.meta_pct,
          dentro_da_meta: dto.taxa.dentro_da_meta,
          excecoes_sem_proposta: dto.taxa.excecoes_sem_proposta,
          por_motivo: dto.taxa.por_motivo as unknown as object,
          triado_em: new Date(dto.triado_em),
        },
        select: { id: true },
      });
      return { persistido: true, execucao_id: linha.id, indisponivel_motivo: null };
    } catch (erro) {
      const motivo = this.motivoIndisponivel(erro);
      this.log.warn(`Taxa de exceção não registrada na série: ${motivo}`);
      return { persistido: false, execucao_id: null, indisponivel_motivo: motivo };
    }
  }

  /** Lê a série, da execução mais recente para a mais antiga. Nunca lança. */
  async serie(limite?: number): Promise<SerieTriagemDto> {
    const take = Math.min(
      Math.max(Number.isFinite(Number(limite)) ? Number(limite) : LIMITE_SERIE_PADRAO, 1),
      LIMITE_SERIE_MAX,
    );
    const lidoEm = new Date().toISOString();

    try {
      const linhas = await this.prisma.com_precificacao_triagem_execucao.findMany({
        orderBy: { triado_em: 'desc' },
        take,
      });
      return {
        disponivel: true,
        indisponivel_motivo: null,
        execucoes: (linhas ?? []).map((l) => this.mapear(l)),
        lido_em: lidoEm,
      };
    } catch (erro) {
      const motivo = this.motivoIndisponivel(erro);
      this.log.warn(`Série de triagem indisponível: ${motivo}`);
      return {
        disponivel: false,
        indisponivel_motivo: motivo,
        execucoes: [],
        lido_em: lidoEm,
      };
    }
  }

  // ---------------------------------------------------------------------------

  /** Coerção de `Decimal`/`Date` do Prisma para os tipos do contrato. */
  private mapear(l: Record<string, unknown>): ExecucaoTriagemDto {
    return {
      id: Number(l.id),
      pedido_id: String(l.pedido_id),
      chave_nfe: l.chave_nfe == null ? null : String(l.chave_nfe),
      for_codigo: l.for_codigo == null ? null : Number(l.for_codigo),
      itens: Number(l.itens),
      excecoes: Number(l.excecoes),
      taxa_excecao_pct: Number(l.taxa_excecao_pct),
      meta_pct: Number(l.meta_pct),
      dentro_da_meta: Boolean(l.dentro_da_meta),
      triado_em:
        l.triado_em instanceof Date ? l.triado_em.toISOString() : String(l.triado_em ?? ''),
    };
  }

  /**
   * Traduz a falha para uma linha que o responsável consiga agir.
   *
   * O caso esperado — tabela ainda não criada — é o `42P01` do Postgres; qualquer outro erro
   * é devolvido como veio, sem mascarar de "DDL pendente" um problema que não é esse.
   */
  private motivoIndisponivel(erro: unknown): string {
    const texto = erro instanceof Error ? erro.message : String(erro);
    if (/42P01|does not exist|não existe/i.test(texto)) {
      return `DDL de ${DDL_TRIAGEM} ainda não aplicada — a série começa quando o responsável rodá-la.`;
    }
    return texto.split('\n')[0].slice(0, 300);
  }
}
