import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import {
  EscopoGiro,
  FrescorGiro,
  JANELA_FRESCURA_HORAS,
  METRICAS_GIRO,
  MetricaGiroDto,
  SinalGiroDto,
} from './dto/sinal-giro.dto';
import { GiroMaterializadoRow, GiroRepository } from './giro.repository';

/** Teto de códigos por consulta em lote — mesmo limite de `tabelas-preco.service.ts`. */
const MAX_CODIGOS_LOTE = 500;

/**
 * Sinal de performance de um produto (US-039): **giro real** e **giro esperado**
 * calculados sobre o **grupo de itens iguais**, mais a **idade da materialização** que
 * os originou.
 *
 * ---
 * ## Contrato de leitura escolhido: SQL direto sobre a tabela compartilhada
 *
 * O ADR-0001 deixou aberto: o `compras-service` lê `com_fifo_completo` direto ou chama o
 * `analise-estoque-service` por HTTP? **Escolhido: leitura direta**, via `PrismaService`,
 * somente leitura. Três razões:
 *
 * 1. **Não existe endpoint HTTP que sirva este sinal.** A API do `analise-estoque-service`
 *    expõe `/analise` (listagem paginada da tela de análise, modelo `AnaliseItem`),
 *    `/similar/group`, `/similar/auto-group`, `/similar/ungroup`, `/similar/recalc`
 *    (POST/DELETE — **mutam** o agrupamento) e `/subgroups` (lista de filtro). O
 *    `AnaliseItem` **não carrega** `demanda_real_dia`, `demanda_planejamento_dia` nem
 *    `grupo_demanda_dia` — exatamente as colunas que constituem o giro esperado. Escolher
 *    HTTP significaria **criar um endpoint novo no serviço do outro dono**, ou seja, mover
 *    uma fronteira — e um endpoint de leitura que apenas reprojetaria a mesma tabela.
 * 2. **Nenhuma fronteira se move na leitura direta**, mesmo critério aplicado pela T-021
 *    (`tabelas-preco.repository.ts`) e registrado no `DATA_MODEL.md`: o
 *    `analise-estoque-service` permanece **dono único de escrita** de `com_fifo_completo` e
 *    de `com_relacionamento_itens`; aqui não se escreve nada. O acoplamento é ao **schema**
 *    de uma tabela compartilhada que o `compras-service` já mapeia no seu
 *    `schema.prisma` e já lê em produção — não é acoplamento novo.
 * 3. **HTTP acrescentaria disponibilidade a uma leitura que não precisa dela.** O sinal é
 *    dado materializado em lote, não um recurso vivo: passar por HTTP colocaria o uptime do
 *    `analise-estoque-service` no caminho crítico da precificação sem entregar nada mais
 *    fresco — do outro lado, o `/subgroups` também lê `com_fifo_completo` por SQL.
 *
 * **Sem ADR novo**, pelo mesmo critério da T-021: nenhuma fronteira entre serviços se move
 * e o ADR-0001 permanece válido. A decisão fecha a lacuna "contrato de leitura do sinal de
 * giro" em `ARCHITECTURE.md`, onde também fica declarada a janela de frescura.
 *
 * ---
 * ## Janela de frescura: 72 horas (corrigida na T-025)
 *
 * `JANELA_FRESCURA_HORAS = 72`, e não 24 como a T-022 entregou. A cadência real do ETL foi
 * **medida** em ~3 dias entre execuções (runs de 2026-07-26 14:06 e 2026-07-29 15:25), então a
 * janela de 24h marcava **todo** produto como obsoleto entre dois ciclos — alerta que dispara
 * sempre não informa nada. Detalhe da decisão e do limite inclusivo (`idade ≥ janela` já é
 * obsoleto) em `sinal-giro.dto.ts`.
 *
 * Acima da janela o sinal **continua sendo devolvido** — nunca se esconde o dado nem se inventa
 * número —, marcado `frescor: 'obsoleto'` e `dado_desatualizado: true`, com
 * `idade_horas`/`idade_dias` explícitos e `janela_frescura_horas` declarado no próprio retorno.
 *
 * ---
 * ## Tendência removida do contrato (T-025)
 *
 * O objeto `tendencia` saiu do `SinalGiroDto`: três dos seus quatro campos
 * (`fator_tendencia`, `tendencia_label`, `vendas_12m_ant`) vieram `null` nos 36 produtos da
 * demo da sprint-04 porque o ETL não os calcula, embora as colunas existam. Sobrou
 * `vendas_ult_12m`, promovido a campo próprio. Justificativa completa e alternativa descartada
 * em `sinal-giro.dto.ts`. O `giro.repository.ts` **continua** selecionando as colunas — o
 * recorte de leitura não é contrato de API, e mexer nele estaria fora do escopo da T-025.
 *
 * ---
 * ## Escopo do número
 *
 * O grupo vem do vínculo **vivo** (`com_relacionamento_itens`), a agregação vem da
 * **materialização** (`com_fifo_completo`). Quando o produto tem grupo mas o agregado ainda
 * não foi materializado (agrupamento mais novo que o último ciclo do ETL), o retorno cai
 * para o escopo `produto` com `agrupamento_materializado: false` — em vez de devolver
 * `null` ou fingir escopo de grupo. Produto sem grupo devolve `sem_grupo: true`,
 * explicitamente, para o motor tratar como exceção.
 */
@Injectable()
export class GiroService {
  constructor(private readonly repo: GiroRepository) {}

  /** Sinal de giro de UM produto. 404 quando o código não existe em nenhuma das fontes. */
  async sinalGiro(proCodigo: number | string): Promise<SinalGiroDto> {
    const cod = this.normalizarCodigo(proCodigo);
    if (cod == null) {
      throw new BadRequestException(`pro_codigo inválido: "${proCodigo}".`);
    }
    const [dto] = await this.montar([cod]);
    if (!dto) {
      throw new NotFoundException(
        `Produto ${cod} não encontrado em com_fifo_completo nem em com_relacionamento_itens.`,
      );
    }
    return dto;
  }

  /**
   * Sinal de giro de VÁRIOS produtos numa requisição — uma ida a cada tabela para o lote
   * inteiro. Códigos desconhecidos simplesmente não voltam.
   */
  async sinalGiroLote(codigos: Array<number | string>): Promise<SinalGiroDto[]> {
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

  private async montar(codigos: number[]): Promise<SinalGiroDto[]> {
    const [vinculos, materializacao] = await Promise.all([
      this.repo.buscarVinculosDeGrupo(codigos),
      this.repo.buscarMaterializacao(codigos),
    ]);

    const lidoEm = new Date();
    const out: SinalGiroDto[] = [];

    for (const cod of codigos) {
      const grupoVivo = vinculos.get(cod) ?? null;
      const linha = materializacao.get(cod) ?? null;
      if (!grupoVivo && !linha) continue;

      // Grupo do produto: o vínculo vivo manda; a materialização é o fallback quando o
      // vínculo não foi lido (ex.: linha órfã no espelho).
      const groupId = grupoVivo ?? linha?.group_id ?? null;
      const semGrupo = groupId == null;

      // Escopo de grupo só quando o agregado do grupo está DE FATO materializado.
      const agrupamentoMaterializado =
        !semGrupo && linha != null && linha.group_id != null && linha.group_id === groupId;
      const escopo: EscopoGiro = agrupamentoMaterializado ? 'grupo' : 'produto';

      const materializadoEm = linha?.materializado_em ?? null;
      const idadeHoras = this.idadeHoras(materializadoEm, lidoEm);
      const frescor = this.frescor(idadeHoras);

      out.push({
        pro_codigo: cod,
        pro_descricao: linha?.pro_descricao ?? null,
        escopo,
        sem_grupo: semGrupo,
        group_id: groupId,
        grupo_chave: agrupamentoMaterializado ? (linha?.grupo_chave ?? null) : null,
        grupo_qtd_itens: agrupamentoMaterializado ? (linha?.grupo_qtd_itens ?? null) : null,
        agrupamento_materializado: agrupamentoMaterializado,
        giro: METRICAS_GIRO.map((meta) => this.montarMetrica(meta, escopo, linha)),
        vendas_ult_12m: this.vendasUlt12m(escopo, linha),
        tempo_medio_estoque_dias: linha?.tempo_medio_estoque ?? null,
        materializado_em: materializadoEm ? materializadoEm.toISOString() : null,
        idade_horas: idadeHoras,
        idade_dias: idadeHoras == null ? null : Math.round((idadeHoras / 24) * 100) / 100,
        frescor,
        janela_frescura_horas: JANELA_FRESCURA_HORAS,
        dado_desatualizado: frescor !== 'fresco',
        lido_em: lidoEm.toISOString(),
      });
    }
    return out;
  }

  /**
   * Uma métrica de giro no escopo efetivo. Sem materialização, o valor sai `null` com o
   * campo de origem do escopo — nunca um número inferido.
   */
  private montarMetrica(
    meta: (typeof METRICAS_GIRO)[number],
    escopo: EscopoGiro,
    linha: GiroMaterializadoRow | null,
  ): MetricaGiroDto {
    const campoOrigem = escopo === 'grupo' ? meta.campo_grupo : meta.campo_produto;
    return {
      metrica: meta.metrica,
      rotulo: meta.rotulo,
      valor: linha ? this.valorPorMetrica(meta.metrica, escopo, linha) : null,
      unidade: 'un/dia',
      escopo,
      campo_origem: campoOrigem,
    };
  }

  private valorPorMetrica(
    metrica: (typeof METRICAS_GIRO)[number]['metrica'],
    escopo: EscopoGiro,
    linha: GiroMaterializadoRow,
  ): number | null {
    if (metrica === 'giro_real') {
      return escopo === 'grupo' ? linha.grp_demanda_media_dia : linha.demanda_media_dia;
    }
    return escopo === 'grupo' ? linha.grupo_demanda_dia : linha.demanda_planejamento_dia;
  }

  /**
   * Vendas 12m no escopo efetivo — o que sobrou do antigo objeto `tendencia`, por ser o único
   * campo do grupo que o ETL de fato preenche.
   */
  private vendasUlt12m(escopo: EscopoGiro, linha: GiroMaterializadoRow | null): number | null {
    if (!linha) return null;
    return escopo === 'grupo' ? linha.grp_vendas_ult_12m : linha.vendas_ult_12m;
  }

  private idadeHoras(materializadoEm: Date | null, agora: Date): number | null {
    if (!materializadoEm) return null;
    const horas = (agora.getTime() - materializadoEm.getTime()) / 3_600_000;
    if (!Number.isFinite(horas)) return null;
    return Math.round(Math.max(0, horas) * 100) / 100;
  }

  /**
   * Limite **inclusivo**: `idade ≥ JANELA_FRESCURA_HORAS` é obsoleto. Atingir a janela cheia
   * significa que o ciclo esperado do ETL (~72h medidas) fechou sem dado novo.
   */
  private frescor(idadeHoras: number | null): FrescorGiro {
    if (idadeHoras == null) return 'sem-materializacao';
    return idadeHoras >= JANELA_FRESCURA_HORAS ? 'obsoleto' : 'fresco';
  }

  private normalizarCodigo(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).trim());
    if (!Number.isFinite(n)) return null;
    const i = Math.trunc(n);
    return i > 0 ? i : null;
  }
}
