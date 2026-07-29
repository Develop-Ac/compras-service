import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import {
  OrigemPreco,
  PrecoTabelaDto,
  PrecoVigenteDto,
  TABELAS_PRECO,
  TabelaPreco,
} from './dto/preco-vigente.dto';
import {
  PrecosEspelhoRow,
  PrecosErpRow,
  TabelasPrecoRepository,
} from './tabelas-preco.repository';

/** Teto de códigos por consulta em lote — evita literal gigante no OPENQUERY. */
const MAX_CODIGOS_LOTE = 500;

/**
 * Disponibiliza ao `compras-service` as **três** tabelas de preço vigentes de um
 * produto, rotuladas por tabela de negócio (US-038):
 *
 * | tabela            | ERP                 | espelho `com_fifo_completo` |
 * |-------------------|---------------------|-----------------------------|
 * | varejo            | `pro.preco_venda`   | `preco_venda_1`             |
 * | atacado especial  | `pro.preco2`        | `preco_venda_2`             |
 * | atacado           | `pro.preco5`        | **não espelhado**           |
 *
 * **Via escolhida para o `preco5`:** leitura direta do ERP a partir do
 * `compras-service`, via `OpenQueryService`. Motivos:
 * 1. `CODING_STANDARDS.md#convencoes-backend` já declara a via canônica —
 *    "ERP: sempre via `OpenQueryService` (pool `mssql` → OPENQUERY `[CONSULTA]`),
 *    somente leitura" — e o `cotacao-sync.service.ts` é o precedente vivo no
 *    mesmo container. Nenhuma fronteira entre serviços se move: o
 *    `analise-estoque-service` continua o **dono único de escrita** de
 *    `com_fifo_completo` (aqui não se escreve nada), e o ERP segue read-only.
 * 2. Estender o ETL do `analise-estoque-service` acoplaria a latência do preço ao
 *    ciclo do batch (o preço chegaria com a idade do último processamento) e faria
 *    o AC "atacado disponível para 100% dos produtos que o têm no ERP" depender do
 *    filtro do ETL (`inativo='N' AND comercializavel='S'`), que exclui produtos que
 *    ainda assim têm preço cadastrado.
 * 3. A leitura direta cobre as três tabelas com a MESMA idade (o instante da
 *    consulta) — comparar preços de tabelas com frescor diferente seria enganoso.
 *
 * O espelho continua sendo lido, para dois fins: informar **a idade do dado
 * espelhado** (AC 3) e servir de **fallback degradado** quando o ERP não responde —
 * caso em que o atacado sai `origem: 'ausente'`, nunca um número inventado.
 */
@Injectable()
export class TabelasPrecoService {
  constructor(private readonly repo: TabelasPrecoRepository) {}

  /** Preços vigentes de UM produto. 404 quando o código não existe em nenhuma fonte. */
  async precosVigentes(proCodigo: number | string): Promise<PrecoVigenteDto> {
    const cod = this.normalizarCodigo(proCodigo);
    if (cod == null) {
      throw new BadRequestException(`pro_codigo inválido: "${proCodigo}".`);
    }
    const [dto] = await this.montar([cod]);
    if (!dto) {
      throw new NotFoundException(
        `Produto ${cod} não encontrado no ERP nem no espelho com_fifo_completo.`,
      );
    }
    return dto;
  }

  /**
   * Preços vigentes de VÁRIOS produtos numa requisição — uma ida ao ERP e uma ao
   * espelho para o lote inteiro. Códigos desconhecidos simplesmente não voltam.
   */
  async precosVigentesLote(codigos: Array<number | string>): Promise<PrecoVigenteDto[]> {
    const cods = (codigos ?? [])
      .map((c) => this.normalizarCodigo(c))
      .filter((c): c is number => c != null);
    if (!cods.length) {
      throw new BadRequestException('Informe ao menos um pro_codigo válido.');
    }
    if (cods.length > MAX_CODIGOS_LOTE) {
      throw new BadRequestException(
        `Máximo de ${MAX_CODIGOS_LOTE} códigos por consulta (recebidos ${cods.length}).`,
      );
    }
    return this.montar(cods);
  }

  // ---------------------------------------------------------------------------
  // Composição
  // ---------------------------------------------------------------------------

  private async montar(codigos: number[]): Promise<PrecoVigenteDto[]> {
    const [erp, espelho] = await Promise.all([
      this.repo.buscarNoErp(codigos),
      this.repo.buscarNoEspelho(codigos),
    ]);

    const erpDisponivel = erp !== null;
    const lidoEm = new Date();
    const out: PrecoVigenteDto[] = [];

    for (const cod of codigos) {
      const linhaErp = erp?.get(cod) ?? null;
      const linhaEspelho = espelho.get(cod) ?? null;
      if (!linhaErp && !linhaEspelho) continue;

      const espelhoEm = linhaEspelho?.atualizado_em ?? null;
      out.push({
        pro_codigo: cod,
        pro_descricao: linhaErp?.pro_descricao ?? linhaEspelho?.pro_descricao ?? null,
        precos: TABELAS_PRECO.map((meta) =>
          this.montarPreco(meta, linhaErp, linhaEspelho, lidoEm, espelhoEm),
        ),
        lido_em: lidoEm.toISOString(),
        espelho_atualizado_em: espelhoEm ? espelhoEm.toISOString() : null,
        espelho_idade_horas: this.idadeHoras(espelhoEm, lidoEm),
        erp_disponivel: erpDisponivel,
      });
    }
    return out;
  }

  /**
   * Um preço rotulado. Precedência: ERP (fonte da verdade) → espelho (fallback) →
   * ausente. O atacado não tem fallback: só existe no ERP.
   */
  private montarPreco(
    meta: (typeof TABELAS_PRECO)[number],
    erp: PrecosErpRow | null,
    espelho: PrecosEspelhoRow | null,
    lidoEm: Date,
    espelhoEm: Date | null,
  ): PrecoTabelaDto {
    const doErp = erp ? this.valorPorTabela(meta.tabela, erp) : null;
    const doEspelho =
      meta.campo_espelho && espelho ? this.valorEspelhoPorTabela(meta.tabela, espelho) : null;

    let valor: number | null = null;
    let origem: OrigemPreco = 'ausente';
    let atualizadoEm: string | null = null;

    if (doErp != null) {
      valor = doErp;
      origem = 'erp';
      atualizadoEm = lidoEm.toISOString();
    } else if (doEspelho != null) {
      valor = doEspelho;
      origem = 'espelho';
      atualizadoEm = espelhoEm ? espelhoEm.toISOString() : null;
    }

    return {
      tabela: meta.tabela,
      rotulo: meta.rotulo,
      campo_erp: meta.campo_erp,
      campo_espelho: meta.campo_espelho,
      valor,
      origem,
      atualizado_em: atualizadoEm,
    };
  }

  private valorPorTabela(tabela: TabelaPreco, erp: PrecosErpRow): number | null {
    if (tabela === 'varejo') return erp.varejo;
    if (tabela === 'atacado_especial') return erp.atacado_especial;
    return erp.atacado;
  }

  private valorEspelhoPorTabela(
    tabela: TabelaPreco,
    espelho: PrecosEspelhoRow,
  ): number | null {
    if (tabela === 'varejo') return espelho.varejo;
    if (tabela === 'atacado_especial') return espelho.atacado_especial;
    // Atacado nunca vem do espelho — a coluna não existe em com_fifo_completo.
    return null;
  }

  private idadeHoras(espelhoEm: Date | null, agora: Date): number | null {
    if (!espelhoEm) return null;
    const horas = (agora.getTime() - espelhoEm.getTime()) / 3_600_000;
    if (!Number.isFinite(horas)) return null;
    return Math.round(Math.max(0, horas) * 100) / 100;
  }

  private normalizarCodigo(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).trim());
    if (!Number.isFinite(n)) return null;
    const i = Math.trunc(n);
    return i > 0 ? i : null;
  }
}
