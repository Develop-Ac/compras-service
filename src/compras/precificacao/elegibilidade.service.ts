import { BadRequestException, Injectable } from '@nestjs/common';
import {
  AvaliacaoElegibilidadeInput,
  ClassificacaoSaldo,
  CORTE_IDADE_SALDO_DIAS,
  CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
  CUSTO_FONTE_CONFIAVEL,
  DECLARACAO_ANCORA,
  ElegibilidadeDto,
  MOTIVO_CUSTO_NAO_CONFIAVEL,
  OrigemAncoraCusto,
  TravaElegibilidadeDto,
} from './dto/elegibilidade.dto';

/** Teto de itens por avaliação em lote — mesmo limite de `giro.service.ts`. */
const MAX_ITENS_LOTE = 500;

/**
 * Travas de elegibilidade e sinal de performance por idade do saldo (US-041 / T-025).
 *
 * Responde **"este item pode receber proposta automática, e qual é o sinal de performance
 * dele?"**. É insumo da T-026, que propõe o preço — aqui **não** se propõe preço, não se lê
 * faixa e não se aplica piso.
 *
 * ---
 * ## Por que este serviço não tem repositório
 *
 * Serviço de **política pura**: recebe os valores já lidos e devolve a decisão. Três razões:
 *
 * 1. **Os dados vêm de duas fontes que já têm dono.** Custo/idade do estoque estão em
 *    `com_fifo_completo` (lida por `giro.repository.ts`, somente leitura) e as `chaves_nfe`
 *    vêm do custo composto da nota (`CustoItemDto`, US-037/T-020). Um repositório novo aqui
 *    seria uma terceira leitura da mesma tabela, com uma terceira coerção de `Decimal`.
 * 2. **A decisão é determinística.** Nada aqui depende de I/O — logo o teste da regra não
 *    depende de banco, e o teste de banco (que o ETL preenche as colunas) já é da T-023.
 * 3. **A T-026 é a orquestradora.** Ela já vai ter custo, giro e régua em mão para propor o
 *    preço; passar os valores é mais barato que reconsultar.
 *
 * ---
 * ## As duas travas de custo (US-041, decisão do responsável de 2026-07-29)
 *
 * Ambas devolvem o **mesmo motivo** — `custo nao confiavel` — e ambas mandam o item para
 * exceção humana, mas olham coisas diferentes:
 *
 * | trava | olha | dispara quando |
 * |---|---|---|
 * | `custo_estoque_nao_confiavel` | custo do **estoque existente** | nenhuma âncora aceita (ver `origemAncora`) **ou** `custo_unitario` NULL |
 * | `custo_nota_sem_xml` | custo da **nota entrando** (US-037) | `chaves_nfe` vazio ⇒ composto parcial |
 *
 * ### A fronteira da âncora andou um degrau (US-047 / T-028, 2026-07-30)
 *
 * A trava da sprint-05 exigia `custo_fonte = camada_viva`. **Camada viva implica estoque** —
 * é a média ponderada das camadas FIFO residuais —, então todo item sem saldo caía em
 * `ultima_entrada` e virava inelegível. Só que os cenários 2 e 3 da US-041 pressupõem
 * exatamente **estoque zerado**: os dois ramos estavam implementados e verdes nos testes da
 * T-026, mas **nenhum item real os alcançava** — código morto por contradição de
 * especificação. Decisão do responsável no gate de review da sprint-05: `ultima_entrada` **é**
 * âncora legítima quando não há estoque, porque é o único custo que existe para aquele item e
 * é custo real de compra, não estimativa.
 *
 * A fronteira anda **um degrau, e só quando `estoque_disponivel = 0`**: com estoque presente a
 * trava continua valendo onde foi desenhada para valer, e `cadastro`/ausente seguem barrados
 * sempre. A origem da âncora sobe declarada no retorno (`custo_ancora_origem` /
 * `custo_ancora_declaracao`) — o aprovador precisa saber que aquele custo é de uma compra
 * passada, não do estoque presente.
 *
 * `custo_unitario` NULL **nunca** é tratado como zero: NULL é ausência de custo no ERP
 * (6.975 produtos mudaram de `0,00` para NULL na correção de 2026-07-29). Coalescer para 0
 * produziria markup infinito e preço calculado sobre o nada.
 *
 * O caso vivo da trava da nota é o par exposto pela demo da sprint-04: o produto `33091` veio
 * com `chaves_nfe: []`, unitário `origem: "pedido"` (R$ 15,77) e ST/IPI/frete `ausente`,
 * contra R$ 40,48 do par `33090` **com** XML — aplicar o piso sobre R$ 15,77 precificaria uma
 * fração do custo real.
 *
 * **Elegibilidade e classificação são independentes.** Uma trava suprime a **proposta**; ela
 * não apaga o sinal de performance, que continua sendo devolvido para o extrato de decisão.
 *
 * ---
 * ## Idade do saldo: o corte é 365, pela coluna numérica
 *
 * Item **com** estoque e **sem** giro não é "sinal faltando" — é classificado por
 * `tempo_medio_saldo_atual` (dias): `≥ 365` ⇒ performance **ruim** (elegível, candidato a
 * redução respeitando o piso); `< 365` ⇒ **sinal ausente** (exceção humana). Sem essa regra,
 * 8.930 itens (59% do estoque) chegariam ao motor indistinguíveis — item morto e item novo
 * produzem o mesmo "não vendeu".
 *
 * **`categoria_saldo_atual` não entra na conta** (e nem sequer é campo de entrada): os buckets
 * param em 240 (`Obsoleto` > 240) e não têm degrau em 365 — classificar por eles marcaria como
 * "ruim" um item de 250 dias que a regra manda tratar como sinal ausente.
 *
 * ### Idade negativa (anomalia conhecida, mínimo medido −150 dias)
 *
 * Idade negativa não existe: é `data_compra` no futuro ou erro de sinal no ETL do
 * `analise-estoque-service`. O tratamento é **explícito e conservador**: a classificação
 * continua acontecendo (nada estoura, nada vira `null`), o item cai em `ausente` — jamais em
 * `ruim`, porque `ruim` autoriza **redução de preço** e não se corta preço com base num número
 * impossível — e a bandeira `idade_saldo_anomala: true` sobe no retorno junto com o valor
 * original, para o aprovador ver o número cru. Nada é silenciosamente descartado nem
 * "corrigido" para zero.
 */
@Injectable()
export class ElegibilidadeService {
  /** Avalia UM item. */
  avaliar(input: AvaliacaoElegibilidadeInput): ElegibilidadeDto {
    const [dto] = this.avaliarLote([input]);
    return dto;
  }

  /** Avalia VÁRIOS itens — a decisão é local a cada item, sem I/O nem estado compartilhado. */
  avaliarLote(inputs: AvaliacaoElegibilidadeInput[]): ElegibilidadeDto[] {
    const lista = inputs ?? [];
    if (!lista.length) {
      throw new BadRequestException('Informe ao menos um item para avaliar.');
    }
    if (lista.length > MAX_ITENS_LOTE) {
      throw new BadRequestException(
        `Máximo de ${MAX_ITENS_LOTE} itens por avaliação (recebidos ${lista.length}).`,
      );
    }
    const avaliadoEm = new Date().toISOString();
    return lista.map((i) => this.avaliarItem(i, avaliadoEm));
  }

  // ---------------------------------------------------------------------------
  // Decisão
  // ---------------------------------------------------------------------------

  private avaliarItem(
    input: AvaliacaoElegibilidadeInput,
    avaliadoEm: string,
  ): ElegibilidadeDto {
    const cod = this.normalizarCodigo(input?.pro_codigo);
    if (cod == null) {
      throw new BadRequestException(`pro_codigo inválido: "${input?.pro_codigo}".`);
    }

    const custoUnitario = this.numero(input.custo_unitario);
    const custoFonte = this.texto(input.custo_fonte);
    const estoque = this.numero(input.estoque_disponivel);
    const comEstoque = estoque != null && estoque > 0;
    const estoqueZerado = estoque === 0;

    const ancoraOrigem = this.origemAncora(custoFonte, custoUnitario, estoqueZerado);
    const custoEstoqueConfiavel = ancoraOrigem != null;

    const travas: TravaElegibilidadeDto[] = [];
    if (!custoEstoqueConfiavel) {
      travas.push({
        trava: 'custo_estoque_nao_confiavel',
        motivo: MOTIVO_CUSTO_NAO_CONFIAVEL,
        detalhe: this.detalheTravaCusto(custoFonte, custoUnitario, estoque),
        campo_origem:
          custoUnitario == null
            ? 'com_fifo_completo.custo_unitario'
            : 'com_fifo_completo.custo_fonte',
      });
    }

    // Ausência de nota ≠ nota sem XML: só se avalia a trava quando a nota foi informada.
    const vinculadoXml = this.vinculoXml(input.chaves_nfe);
    if (vinculadoXml === false) {
      travas.push({
        trava: 'custo_nota_sem_xml',
        motivo: MOTIVO_CUSTO_NAO_CONFIAVEL,
        detalhe:
          'chaves_nfe vazio — item de pedido sem vínculo de XML; o custo composto é parcial ' +
          '(unitário do pedido, ST/IPI/frete ausentes)',
        campo_origem: 'CustoItemDto.chaves_nfe',
      });
    }

    const giroReal = this.numero(input.giro_real);
    const comGiro = giroReal != null && giroReal > 0;

    const idadeSaldo = this.numero(input.tempo_medio_saldo_atual);
    const idadeAnomala = idadeSaldo != null && idadeSaldo < 0;

    return {
      pro_codigo: cod,
      elegivel: travas.length === 0,
      motivos: Array.from(new Set(travas.map((t) => t.motivo))),
      travas,
      custo_estoque_confiavel: custoEstoqueConfiavel,
      custo_ancora_origem: ancoraOrigem,
      custo_ancora_declaracao: ancoraOrigem == null ? null : DECLARACAO_ANCORA[ancoraOrigem],
      custo_unitario: custoUnitario,
      custo_fonte: custoFonte,
      custo_nota_vinculado_xml: vinculadoXml,
      com_estoque: comEstoque,
      com_giro: comGiro,
      classificacao_saldo: this.classificarSaldo(comEstoque, comGiro, idadeSaldo, idadeAnomala),
      idade_saldo_dias: idadeSaldo,
      idade_saldo_anomala: idadeAnomala,
      corte_idade_saldo_dias: CORTE_IDADE_SALDO_DIAS,
      avaliado_em: avaliadoEm,
    };
  }

  /**
   * Qual âncora de custo sustenta a proposta — ou `null` quando nenhuma sustenta (US-047).
   *
   * A fronteira anda **um degrau, e só com estoque zerado**:
   *
   * | `custo_fonte` | `estoque_disponivel = 0` | `> 0`, NULL ou negativo |
   * |---|---|---|
   * | `camada_viva`    | âncora `camada_viva` | âncora `camada_viva` |
   * | `ultima_entrada` | âncora `ultima_entrada_sem_estoque` | **trava** |
   * | `cadastro` / ausente | **trava** | **trava** |
   *
   * `custo_unitario` NULL trava **em qualquer degrau e qualquer estoque** — NULL é ausência de
   * custo no ERP, jamais custo zero, e coalescer produziria markup infinito.
   *
   * `estoqueZerado` é o **zero literal**: NULL (não materializado) e negativo (anomalia de
   * ERP) não destravam a última entrada. Estender a exceção a um número ausente ou impossível
   * seria abrir preço automático justamente onde o dado não sustenta a decisão.
   */
  private origemAncora(
    custoFonte: string | null,
    custoUnitario: number | null,
    estoqueZerado: boolean,
  ): OrigemAncoraCusto {
    if (custoUnitario == null) return null;
    if (custoFonte === CUSTO_FONTE_CONFIAVEL) return 'camada_viva';
    if (custoFonte === CUSTO_FONTE_ANCORA_SEM_ESTOQUE && estoqueZerado) {
      return 'ultima_entrada_sem_estoque';
    }
    return null;
  }

  /** Explica qual valor concreto derrubou a âncora, com o estoque que decidiu o caso. */
  private detalheTravaCusto(
    custoFonte: string | null,
    custoUnitario: number | null,
    estoque: number | null,
  ): string {
    if (custoUnitario == null) {
      return 'custo_unitario ausente (NULL) — ausência de custo no ERP, nunca custo zero';
    }
    if (custoFonte === CUSTO_FONTE_ANCORA_SEM_ESTOQUE) {
      return (
        `custo_fonte = "${CUSTO_FONTE_ANCORA_SEM_ESTOQUE}" com estoque_disponivel = ` +
        `${estoque ?? 'ausente (NULL)'} — a última entrada só ancora item com estoque ` +
        'zerado (US-047)'
      );
    }
    return (
      `custo_fonte = "${custoFonte ?? 'ausente'}" (esperado "${CUSTO_FONTE_CONFIAVEL}", ou ` +
      `"${CUSTO_FONTE_ANCORA_SEM_ESTOQUE}" com estoque_disponivel = 0)`
    );
  }

  /**
   * Classificação por idade do saldo. Só decide para item **com** estoque e **sem** giro —
   * nos outros casos a performance vem do par giro real × esperado (US-039).
   *
   * Idade anômala (negativa) e idade não materializada caem em `ausente`: exceção humana, e
   * jamais `ruim`, que autorizaria redução de preço.
   */
  private classificarSaldo(
    comEstoque: boolean,
    comGiro: boolean,
    idadeSaldoDias: number | null,
    idadeAnomala: boolean,
  ): ClassificacaoSaldo {
    if (!comEstoque || comGiro) return 'nao-aplicavel';
    if (idadeSaldoDias == null || idadeAnomala) return 'ausente';
    return idadeSaldoDias >= CORTE_IDADE_SALDO_DIAS ? 'ruim' : 'ausente';
  }

  /**
   * `true` com pelo menos uma chave; `false` com lista vazia (o caso do produto `33091`);
   * `null` quando nenhuma nota foi informada — aí a trava da nota não se aplica.
   */
  private vinculoXml(chaves: string[] | null | undefined): boolean | null {
    if (chaves == null) return null;
    return chaves.some((c) => this.texto(c) != null);
  }

  // ---------------------------------------------------------------------------
  // Utilitários (mesmas regras de coerção de `giro.repository.ts`)
  // ---------------------------------------------------------------------------

  /** Nunca coalesce ausência para 0 — NULL entra e NULL sai. */
  private numero(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).replace(',', '.').trim());
    return Number.isFinite(n) ? n : null;
  }

  private texto(v: unknown): string | null {
    if (v == null) return null;
    const s = String(v).trim();
    return s.length ? s : null;
  }

  private normalizarCodigo(v: unknown): number | null {
    if (v == null) return null;
    const n = Number(String(v).trim());
    if (!Number.isFinite(n)) return null;
    const i = Math.trunc(n);
    return i > 0 ? i : null;
  }
}
