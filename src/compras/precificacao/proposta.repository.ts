import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { PrecoVigenteDto } from './dto/preco-vigente.dto';
import { SinalGiroDto } from './dto/sinal-giro.dto';
import { GiroRepository } from './giro.repository';
import { GiroService } from './giro.service';
import { TabelasPrecoService } from './tabelas-preco.service';

/** Teto por consulta em lote aos serviços de preço/giro (`MAX_ITENS_LOTE` deles é 500). */
const LOTE_MAX = 400;

/** Linha de `com_fifo_completo` que sustenta o custo ANTERIOR e o estoque do item. */
export interface EstoqueFifoRow {
  pro_codigo: number;
  custo_unitario: number | null;
  custo_fonte: string | null;
  estoque_disponivel: number | null;
  tempo_medio_saldo_atual: number | null;
}

/** Nunca coalesce ausência para 0 — NULL entra e NULL sai (mesma regra da T-025). */
export function numeroOuNulo(v: unknown): number | null {
  if (v == null) return null;
  const n = Number(String(v).replace(',', '.').trim());
  return Number.isFinite(n) ? n : null;
}

function textoOuNulo(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function inteiroOuNulo(v: unknown): number | null {
  const n = numeroOuNulo(v);
  if (n == null) return null;
  const i = Math.trunc(n);
  return i > 0 ? i : null;
}

/**
 * Acesso a dados da proposta de preço (US-041 / T-026, extraído pela T-027).
 *
 * A T-026 deixou as três leituras dentro do `PropostaService` com o desvio de convenção
 * declarado no cabeçalho daquela classe — os `## Outputs` daquela task não incluíam
 * repositório algum. A T-027 quita a dívida: o container manda acesso a dados em
 * `*.repository.ts` (CODING_STANDARDS#convencoes-backend-compras-service), e é aqui que ele
 * passa a morar. **Refactor puro**: nenhum comportamento mudou, só o endereço do código.
 *
 * `com_fifo_completo` é lida **somente leitura** — o dono da escrita é o ETL do
 * `analise-estoque-service`. As outras duas leituras são de fatias irmãs do próprio módulo
 * (`TabelasPrecoService` da US-038 e `GiroService` da US-039); moram aqui porque compartilham
 * o mesmo fatiamento em blocos de `LOTE_MAX` e porque o service passa a ter uma única porta de
 * leitura, e não três de origens diferentes.
 */
@Injectable()
export class PropostaRepository {
  constructor(
    private readonly prisma: PrismaService,
    private readonly tabelasPreco: TabelasPrecoService,
    private readonly giro: GiroService,
    private readonly giroRepositorio: GiroRepository,
  ) {}

  /**
   * Vínculo **vivo** produto → grupo (`com_relacionamento_itens`), delegado ao
   * `GiroRepository` — um ativo, dois usos (US-039 / US-042). Não é reimplementado aqui para
   * não criar uma segunda verdade sobre o agrupamento; a porta de leitura do `PropostaService`
   * continua sendo uma só.
   */
  async lerGruposDosItens(codigos: number[]): Promise<Map<number, string>> {
    if (!codigos?.length) return new Map();
    return this.giroRepositorio.buscarVinculosDeGrupo(codigos);
  }

  /**
   * Membros de cada grupo (`com_relacionamento_itens`, **somente leitura**) — a enumeração que
   * a herança de markup da US-042 precisa para achar os similares de um item sem histórico.
   *
   * Devolve `group_id → pro_codigos`, em blocos de `LOTE_MAX` como as demais leituras. Grupo
   * sem linha utilizável simplesmente não aparece no mapa (ausência, nunca lista vazia
   * inventada).
   */
  async lerMembrosDoGrupo(groupIds: string[]): Promise<Map<string, number[]>> {
    const out = new Map<string, number[]>();
    const unicos = Array.from(
      new Set((groupIds ?? []).map((g) => textoOuNulo(g)).filter((g): g is string => g != null)),
    );
    if (!unicos.length) return out;

    for (let i = 0; i < unicos.length; i += LOTE_MAX) {
      const bloco = unicos.slice(i, i + LOTE_MAX);
      const linhas = await this.prisma.com_relacionamento_itens.findMany({
        where: { group_id: { in: bloco } },
        select: { pro_codigo: true, group_id: true },
      });
      for (const l of linhas) {
        const grupo = textoOuNulo(l.group_id);
        const cod = inteiroOuNulo(l.pro_codigo);
        if (grupo == null || cod == null) continue;
        const atual = out.get(grupo);
        if (atual) {
          if (!atual.includes(cod)) atual.push(cod);
        } else {
          out.set(grupo, [cod]);
        }
      }
    }
    return out;
  }

  /**
   * Custo ANTERIOR e estoque do item, de `com_fifo_completo` (**somente leitura**; dono da
   * escrita é o ETL do `analise-estoque-service`).
   */
  async lerEstoqueFifo(codigos: number[]): Promise<Map<number, EstoqueFifoRow>> {
    const out = new Map<number, EstoqueFifoRow>();
    if (!codigos.length) return out;

    for (const bloco of this.blocos(codigos)) {
      const linhas = await this.prisma.com_fifo_completo.findMany({
        where: { pro_codigo: { in: bloco.map((c) => String(c)) } },
        select: {
          pro_codigo: true,
          custo_unitario: true,
          custo_fonte: true,
          estoque_disponivel: true,
          tempo_medio_saldo_atual: true,
          data_processamento: true,
        },
        orderBy: { data_processamento: 'desc' },
      });
      for (const l of linhas) {
        const cod = inteiroOuNulo(l.pro_codigo);
        if (cod == null || out.has(cod)) continue;
        out.set(cod, {
          pro_codigo: cod,
          custo_unitario: numeroOuNulo(l.custo_unitario),
          custo_fonte: textoOuNulo(l.custo_fonte),
          estoque_disponivel: numeroOuNulo(l.estoque_disponivel),
          tempo_medio_saldo_atual: numeroOuNulo(l.tempo_medio_saldo_atual),
        });
      }
    }
    return out;
  }

  async lerPrecos(codigos: number[]): Promise<Map<number, PrecoVigenteDto>> {
    const out = new Map<number, PrecoVigenteDto>();
    for (const bloco of this.blocos(codigos)) {
      for (const dto of await this.tabelasPreco.precosVigentesLote(bloco)) {
        out.set(Number(dto.pro_codigo), dto);
      }
    }
    return out;
  }

  async lerSinais(codigos: number[]): Promise<Map<number, SinalGiroDto>> {
    const out = new Map<number, SinalGiroDto>();
    for (const bloco of this.blocos(codigos)) {
      for (const dto of await this.giro.sinalGiroLote(bloco)) {
        out.set(Number(dto.pro_codigo), dto);
      }
    }
    return out;
  }

  private blocos(codigos: number[]): number[][] {
    const out: number[][] = [];
    for (let i = 0; i < codigos.length; i += LOTE_MAX) {
      out.push(codigos.slice(i, i + LOTE_MAX));
    }
    return out;
  }
}
