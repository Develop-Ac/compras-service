import { BadRequestException } from '@nestjs/common';
import {
  AvaliacaoElegibilidadeInput,
  CORTE_IDADE_SALDO_DIAS,
  CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
  CUSTO_FONTE_CONFIAVEL,
  ElegibilidadeDto,
  MOTIVO_CUSTO_NAO_CONFIAVEL,
} from '../src/compras/precificacao/dto/elegibilidade.dto';
import { JANELA_FRESCURA_HORAS } from '../src/compras/precificacao/dto/sinal-giro.dto';
import { ElegibilidadeService } from '../src/compras/precificacao/elegibilidade.service';
import {
  GiroMaterializadoRow,
  GiroRepository,
} from '../src/compras/precificacao/giro.repository';
import { GiroService } from '../src/compras/precificacao/giro.service';

/**
 * US-041 / T-025 — travas de elegibilidade do item e correções do sinal de performance.
 *
 * A decisão é determinística e sem I/O (`ElegibilidadeService` é política pura), então os
 * testes exercitam a regra diretamente. A perna que exige Postgres real — conferir que o ETL
 * de fato preenche `custo_fonte`/`tempo_medio_saldo_atual` nas proporções medidas — fica
 * `pendente-provisao`, conforme TESTING.md#gates.
 *
 * As duas últimas suítes cobrem as correções feitas em `giro.service.ts` /
 * `sinal-giro.dto.ts` (janela 24h → 72h e remoção da tendência sempre nula). Elas moram aqui
 * porque `test/sinal-giro.int-spec.ts` não está nos `## Outputs` da T-025 e não foi tocado —
 * ele assere contra a **constante** `JANELA_FRESCURA_HORAS`, logo não prova o valor 72.
 */

// Par vivo exposto pela demo da sprint-04.
const PRO_COM_XML = 33090; // R$ 40,48, com chave de NF-e
const PRO_SEM_XML = 33091; // R$ 15,77, chaves_nfe: [] — custo composto parcial
const CHAVE_NFE = '35260712345678901234550010000123451234567890';

/** Item saudável: custo confiável, com estoque, com giro. */
function item(over: Partial<AvaliacaoElegibilidadeInput> = {}): AvaliacaoElegibilidadeInput {
  return {
    pro_codigo: PRO_COM_XML,
    custo_unitario: 40.48,
    custo_fonte: CUSTO_FONTE_CONFIAVEL,
    estoque_disponivel: 12,
    giro_real: 3.42,
    tempo_medio_saldo_atual: 47.5,
    chaves_nfe: [CHAVE_NFE],
    ...over,
  };
}

describe('ElegibilidadeService (US-041 / T-025)', () => {
  const service = () => new ElegibilidadeService();

  // Trava de custo do ESTOQUE ---------------------------------------------------
  // O `item()` base tem `estoque_disponivel: 12` — logo esta suíte inteira é a NÃO-REGRESSÃO
  // exigida pela US-047: com estoque presente, a trava continua valendo onde foi desenhada.
  describe('Dado um item COM estoque cujo custo_fonte não é camada_viva', () => {
    it('Então o item é inelegível para proposta automática', () => {
      const dto = service().avaliar(item({ custo_fonte: 'ultima_entrada' }));

      expect(dto.elegivel).toBe(false);
    });

    it('E o motivo registrado é "custo nao confiavel"', () => {
      const dto = service().avaliar(item({ custo_fonte: 'ultima_entrada' }));

      expect(dto.motivos).toEqual([MOTIVO_CUSTO_NAO_CONFIAVEL]);
    });

    it('E a trava identificada é a do custo do estoque, com a coluna de origem', () => {
      const dto = service().avaliar(item({ custo_fonte: 'cadastro' }));

      expect(dto.travas).toEqual([
        expect.objectContaining({
          trava: 'custo_estoque_nao_confiavel',
          campo_origem: 'com_fifo_completo.custo_fonte',
        }),
      ]);
    });

    it('E custo_fonte ausente (NULL) também derruba a elegibilidade', () => {
      const dto = service().avaliar(item({ custo_fonte: null }));

      expect(dto).toMatchObject({ elegivel: false, custo_estoque_confiavel: false });
    });
  });

  // Âncora da última entrada sem estoque (US-047 / T-028) ----------------------
  describe('Dado um item SEM estoque com custo_fonte "ultima_entrada" (US-047)', () => {
    const semEstoque = (over: Partial<AvaliacaoElegibilidadeInput> = {}) =>
      item({
        custo_fonte: CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
        estoque_disponivel: 0,
        custo_unitario: 44,
        ...over,
      });

    it('Então o item é elegível para proposta automática', () => {
      const dto = service().avaliar(semEstoque());

      expect(dto).toMatchObject({ elegivel: true, motivos: [], travas: [] });
    });

    it('E a proposta declara que a âncora veio da última entrada, não de camada viva', () => {
      const dto = service().avaliar(semEstoque());

      expect(dto.custo_ancora_origem).toBe('ultima_entrada_sem_estoque');
      expect(dto.custo_ancora_declaracao).toMatch(/ÚLTIMA ENTRADA/);
      expect(dto.custo_ancora_declaracao).toMatch(/compra passada, não do estoque presente/);
    });

    it('E o item com camada viva declara a outra origem — as duas não se confundem', () => {
      const dto = service().avaliar(item());

      expect(dto).toMatchObject({ elegivel: true, custo_ancora_origem: 'camada_viva' });
      expect(dto.custo_ancora_declaracao).toMatch(/camadas FIFO vivas/);
    });

    it('E o custo continua saindo cru no retorno, sem coerção', () => {
      const dto = service().avaliar(semEstoque());

      expect(dto).toMatchObject({
        custo_unitario: 44,
        custo_fonte: CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
        com_estoque: false,
      });
    });

    it('E a trava da NOTA continua independente — item sem estoque e sem XML segue barrado', () => {
      const dto = service().avaliar(semEstoque({ chaves_nfe: [] }));

      expect(dto.elegivel).toBe(false);
      expect(dto.travas.map((t) => t.trava)).toEqual(['custo_nota_sem_xml']);
    });
  });

  describe('Dado que a fronteira anda UM degrau e SÓ com estoque zerado', () => {
    const comFonte = (fonte: string | null, estoque: number | null) =>
      service().avaliar(item({ custo_fonte: fonte, estoque_disponivel: estoque }));

    it('Então "ultima_entrada" COM estoque permanece inelegível (não-regressão da trava)', () => {
      const dto = comFonte(CUSTO_FONTE_ANCORA_SEM_ESTOQUE, 12);

      expect(dto).toMatchObject({
        elegivel: false,
        custo_estoque_confiavel: false,
        custo_ancora_origem: null,
        custo_ancora_declaracao: null,
        motivos: [MOTIVO_CUSTO_NAO_CONFIAVEL],
      });
    });

    it('E a trava explica que a última entrada só ancora item com estoque zerado', () => {
      const dto = comFonte(CUSTO_FONTE_ANCORA_SEM_ESTOQUE, 12);

      expect(dto.travas[0].detalhe).toMatch(/estoque_disponivel = 12/);
      expect(dto.travas[0].detalhe).toMatch(/só ancora item com estoque zerado/);
    });

    it('E "cadastro" segue barrado em QUALQUER estoque — a fronteira não anda dois degraus', () => {
      for (const estoque of [0, 12, null]) {
        expect(comFonte('cadastro', estoque).elegivel).toBe(false);
      }
    });

    it('E custo_fonte ausente (NULL) segue barrado em QUALQUER estoque', () => {
      for (const estoque of [0, 12, null]) {
        expect(comFonte(null, estoque).elegivel).toBe(false);
      }
    });

    it('E estoque NULL NÃO é estoque zero — não destrava a última entrada', () => {
      const dto = comFonte(CUSTO_FONTE_ANCORA_SEM_ESTOQUE, null);

      expect(dto).toMatchObject({ elegivel: false, custo_ancora_origem: null });
    });

    it('E estoque NEGATIVO (anomalia de ERP) também não destrava a última entrada', () => {
      const dto = comFonte(CUSTO_FONTE_ANCORA_SEM_ESTOQUE, -3);

      expect(dto).toMatchObject({ elegivel: false, custo_ancora_origem: null });
    });

    it('E "camada_viva" segue elegível com estoque zerado — nada foi retirado', () => {
      const dto = comFonte(CUSTO_FONTE_CONFIAVEL, 0);

      expect(dto).toMatchObject({ elegivel: true, custo_ancora_origem: 'camada_viva' });
    });

    it('E custo_unitario NULL barra a última entrada sem estoque — NULL nunca vira zero', () => {
      const dto = service().avaliar(
        item({
          custo_fonte: CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
          estoque_disponivel: 0,
          custo_unitario: null,
        }),
      );

      expect(dto).toMatchObject({
        elegivel: false,
        custo_ancora_origem: null,
        custo_unitario: null,
      });
      expect(dto.custo_unitario).not.toBe(0);
    });
  });

  describe('Dado um item cujo custo_unitario é NULL', () => {
    const semCusto = () => item({ custo_unitario: null });

    it('Então o item é inelegível, mesmo com custo_fonte camada_viva', () => {
      const dto = service().avaliar(semCusto());

      expect(dto).toMatchObject({
        elegivel: false,
        custo_estoque_confiavel: false,
        motivos: [MOTIVO_CUSTO_NAO_CONFIAVEL],
      });
    });

    it('E o custo NULL NUNCA é tratado como zero — sai null no retorno', () => {
      const dto = service().avaliar(semCusto());

      expect(dto.custo_unitario).toBeNull();
      expect(dto.custo_unitario).not.toBe(0);
    });

    it('E custo_unitario zero é caso DIFERENTE de NULL — zero é valor, não ausência', () => {
      const dto = service().avaliar(item({ custo_unitario: 0 }));

      expect(dto.custo_unitario).toBe(0);
      expect(dto.travas[0]).toBeUndefined();
    });
  });

  // Trava do custo da NOTA ENTRANDO --------------------------------------------
  describe('Dado o item 33091 da demo, de pedido sem vínculo de XML', () => {
    const semXml = () =>
      item({ pro_codigo: PRO_SEM_XML, custo_unitario: 15.77, chaves_nfe: [] });

    it('Então o item é inelegível com o mesmo motivo "custo nao confiavel"', () => {
      const dto = service().avaliar(semXml());

      expect(dto).toMatchObject({
        pro_codigo: PRO_SEM_XML,
        elegivel: false,
        motivos: [MOTIVO_CUSTO_NAO_CONFIAVEL],
      });
    });

    it('E a trava identificada é a da nota, distinta da trava do custo do estoque', () => {
      const dto = service().avaliar(semXml());

      expect(dto.travas).toEqual([
        expect.objectContaining({
          trava: 'custo_nota_sem_xml',
          campo_origem: 'CustoItemDto.chaves_nfe',
        }),
      ]);
    });

    it('E o retorno declara que a nota não tem vínculo de XML', () => {
      const dto = service().avaliar(semXml());

      expect(dto.custo_nota_vinculado_xml).toBe(false);
    });

    it('E o par 33090, com XML e custo camada_viva, permanece elegível', () => {
      const dto = service().avaliar(item());

      expect(dto).toMatchObject({
        pro_codigo: PRO_COM_XML,
        elegivel: true,
        motivos: [],
        travas: [],
        custo_nota_vinculado_xml: true,
      });
    });
  });

  describe('Dado um item avaliado sem nota nenhuma (só estoque)', () => {
    it('Então a trava da nota NÃO dispara — ausência de nota não é nota sem XML', () => {
      const dto = service().avaliar(item({ chaves_nfe: undefined }));

      expect(dto).toMatchObject({ elegivel: true, custo_nota_vinculado_xml: null });
    });
  });

  describe('Dado um item que dispara as DUAS travas', () => {
    it('Então as duas voltam listadas, com o motivo deduplicado numa única linha', () => {
      const dto = service().avaliar(item({ custo_unitario: null, chaves_nfe: [] }));

      expect(dto.travas.map((t) => t.trava)).toEqual([
        'custo_estoque_nao_confiavel',
        'custo_nota_sem_xml',
      ]);
      expect(dto.motivos).toEqual([MOTIVO_CUSTO_NAO_CONFIAVEL]);
    });
  });

  // Idade do saldo -------------------------------------------------------------
  describe('Dado um item COM estoque e SEM giro', () => {
    const comSaldoSemGiro = (idade: number | null) =>
      item({ estoque_disponivel: 30, giro_real: null, tempo_medio_saldo_atual: idade });

    it('Então idade ≥ 365 dias classifica performance como ruim', () => {
      const dto = service().avaliar(comSaldoSemGiro(1893));

      expect(dto.classificacao_saldo).toBe('ruim');
    });

    it('E performance ruim NÃO derruba a elegibilidade — é candidato a redução', () => {
      const dto = service().avaliar(comSaldoSemGiro(1893));

      expect(dto.elegivel).toBe(true);
    });

    it('E exatamente 365 dias já é performance ruim (corte inclusivo)', () => {
      const dto = service().avaliar(comSaldoSemGiro(CORTE_IDADE_SALDO_DIAS));

      expect(dto.classificacao_saldo).toBe('ruim');
    });

    it('E idade < 365 dias é sinal ausente, para exceção humana', () => {
      const dto = service().avaliar(comSaldoSemGiro(176));

      expect(dto.classificacao_saldo).toBe('ausente');
    });

    it('E 364 dias ainda é sinal ausente', () => {
      const dto = service().avaliar(comSaldoSemGiro(CORTE_IDADE_SALDO_DIAS - 1));

      expect(dto.classificacao_saldo).toBe('ausente');
    });

    it('E o corte usado é declarado no retorno, não implícito', () => {
      const dto = service().avaliar(comSaldoSemGiro(176));

      expect(dto.corte_idade_saldo_dias).toBe(365);
    });

    it('E 300 dias — bucket "Obsoleto" (>240) — permanece AUSENTE: o corte é a coluna numérica', () => {
      const dto = service().avaliar(comSaldoSemGiro(300));

      expect(dto.classificacao_saldo).toBe('ausente');
    });

    it('E idade não materializada (NULL) é sinal ausente, nunca ruim', () => {
      const dto = service().avaliar(comSaldoSemGiro(null));

      expect(dto).toMatchObject({ classificacao_saldo: 'ausente', idade_saldo_dias: null });
    });
  });

  describe('Dado um item cuja idade de saldo é NEGATIVA (anomalia conhecida, −150 dias)', () => {
    const anomalo = () =>
      item({ estoque_disponivel: 30, giro_real: null, tempo_medio_saldo_atual: -150 });

    it('Então a classificação acontece normalmente — nada estoura nem volta nulo', () => {
      const dto = service().avaliar(anomalo());

      expect(dto.classificacao_saldo).toBe('ausente');
    });

    it('E a anomalia é sinalizada explicitamente, sem exceção silenciosa', () => {
      const dto = service().avaliar(anomalo());

      expect(dto.idade_saldo_anomala).toBe(true);
    });

    it('E o valor cru é preservado no retorno, para o aprovador ver o número impossível', () => {
      const dto = service().avaliar(anomalo());

      expect(dto.idade_saldo_dias).toBe(-150);
    });

    it('E a idade negativa NUNCA vira performance ruim — não se corta preço com número impossível', () => {
      const dto = service().avaliar(anomalo());

      expect(dto.classificacao_saldo).not.toBe('ruim');
    });

    it('E idade válida não levanta a bandeira de anomalia', () => {
      const dto = service().avaliar(
        item({ estoque_disponivel: 30, giro_real: null, tempo_medio_saldo_atual: 400 }),
      );

      expect(dto).toMatchObject({ idade_saldo_anomala: false, classificacao_saldo: 'ruim' });
    });
  });

  describe('Dado um item COM giro', () => {
    it('Então a idade do saldo não decide — a performance vem do par giro real × esperado', () => {
      const dto = service().avaliar(item({ giro_real: 3.42, tempo_medio_saldo_atual: 2000 }));

      expect(dto).toMatchObject({ com_giro: true, classificacao_saldo: 'nao-aplicavel' });
    });
  });

  describe('Dado um item SEM estoque', () => {
    it('Então a idade do saldo não se aplica', () => {
      const dto = service().avaliar(
        item({ estoque_disponivel: 0, giro_real: null, tempo_medio_saldo_atual: 2000 }),
      );

      expect(dto).toMatchObject({ com_estoque: false, classificacao_saldo: 'nao-aplicavel' });
    });
  });

  // Bordas ---------------------------------------------------------------------
  describe('Dado uma entrada inválida', () => {
    it('Então pro_codigo não numérico é rejeitado com 400', () => {
      expect(() => service().avaliar(item({ pro_codigo: 'abc' }))).toThrow(BadRequestException);
    });

    it('E um lote vazio é rejeitado', () => {
      expect(() => service().avaliarLote([])).toThrow(BadRequestException);
    });

    it('E o lote avalia cada item de forma independente', () => {
      const lista = service().avaliarLote([
        item(),
        item({ pro_codigo: PRO_SEM_XML, chaves_nfe: [] }),
      ]);

      expect(lista.map((d) => d.elegivel)).toEqual([true, false]);
    });
  });

  // Ramos 2 e 3 da US-041 alcançáveis (US-047) ---------------------------------
  //
  // `proposta.service.ts` decide o ramo assim (transcrição literal das linhas 194 e 438-484):
  //
  //   elegivel = elegibilidade.elegivel && custoAtual > 0
  //   se !elegivel                                 ⇒ 'inelegivel'
  //   direcao === 'caiu' && comEstoque             ⇒ AC1 'custo_caiu_com_estoque'
  //   direcao === 'caiu' && sinal.posicao === 'alta'  ⇒ AC2 '..._sem_estoque_performance_boa'
  //   direcao === 'caiu' && sinal.posicao === 'baixa' ⇒ AC3 '..._sem_estoque_performance_ruim'
  //   direcao === 'caiu'                           ⇒ '..._sem_estoque_sinal_ausente'
  //
  // e `posicao` vem de `sinalPerformance` (linhas 659-680): com par de giro positivo,
  // `giro_real ≥ giro_esperado ⇒ 'alta'`, senão `'baixa'`.
  //
  // `test/proposta.int-spec.ts` já assere os ramos 2 e 3 ponta a ponta, mas com fixtures de
  // `camada_viva` + estoque 0 — combinação que **não existe em item real**, porque camada viva
  // é a média das camadas FIFO residuais e portanto implica estoque. Item real sem estoque tem
  // `ultima_entrada`, e era filtrado no gate antes de chegar à árvore. O que estas asserções
  // provam é exatamente isso: com a T-028, o item REAL passa o gate e o predicado de cada ramo
  // fica satisfeito. `ramoAlcancado` é espelho declarado da condição acima, não o serviço.
  describe('Dado a amostra real de itens com estoque zerado e custo da última entrada', () => {
    type Ramo =
      | 'inelegivel'
      | 'custo_caiu_com_estoque'
      | 'custo_caiu_sem_estoque_performance_boa'
      | 'custo_caiu_sem_estoque_performance_ruim'
      | 'custo_caiu_sem_estoque_sinal_ausente';

    /** Espelho da árvore de `proposta.service.ts` para custo em queda. */
    function ramoAlcancado(
      dto: ElegibilidadeDto,
      giroReal: number | null,
      giroEsperado: number | null,
      custoAtual: number,
    ): Ramo {
      if (!dto.elegivel || !(custoAtual > 0)) return 'inelegivel';
      if (dto.com_estoque) return 'custo_caiu_com_estoque';
      const temPar = giroReal != null && giroReal > 0 && giroEsperado != null && giroEsperado > 0;
      if (temPar) {
        return giroReal >= giroEsperado
          ? 'custo_caiu_sem_estoque_performance_boa'
          : 'custo_caiu_sem_estoque_performance_ruim';
      }
      if (dto.classificacao_saldo === 'ruim') {
        return 'custo_caiu_sem_estoque_performance_ruim';
      }
      return 'custo_caiu_sem_estoque_sinal_ausente';
    }

    // Amostra com a forma do dado real: sem saldo ⇒ sem camada viva ⇒ `ultima_entrada`.
    // Custo anterior 44 e custo atual 40,48 — custo caiu.
    const CUSTO_ATUAL = 40.48;
    const GIRO_ESPERADO = 2.1;
    const amostra = [
      { pro_codigo: 33090, giro_real: 3.42 }, // giro ≥ esperado ⇒ ramo 2
      { pro_codigo: 33091, giro_real: 0.4 }, // giro < esperado ⇒ ramo 3
    ];

    const avaliada = () =>
      service()
        .avaliarLote(
          amostra.map((a) =>
            item({
              pro_codigo: a.pro_codigo,
              custo_unitario: 44,
              custo_fonte: CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
              estoque_disponivel: 0,
              giro_real: a.giro_real,
              tempo_medio_saldo_atual: null,
            }),
          ),
        )
        .map((dto, i) =>
          ramoAlcancado(dto, amostra[i].giro_real, GIRO_ESPERADO, CUSTO_ATUAL),
        );

    it('Então ao menos 1 item alcança o ramo 2 — custo caiu, estoque zerado, giro ≥ esperado', () => {
      expect(avaliada()).toContain('custo_caiu_sem_estoque_performance_boa');
    });

    it('E ao menos 1 item alcança o ramo 3 — custo caiu, estoque zerado, giro < esperado', () => {
      expect(avaliada()).toContain('custo_caiu_sem_estoque_performance_ruim');
    });

    it('E NENHUM item da amostra é filtrado como inelegível antes da árvore', () => {
      expect(avaliada()).not.toContain('inelegivel');
    });

    it('E antes da T-028 a MESMA amostra morria no gate — os dois ramos eram inalcançáveis', () => {
      // Reconstitui a trava da sprint-05: só `camada_viva` ancorava.
      const gateAntigo = (i: AvaliacaoElegibilidadeInput) =>
        i.custo_fonte === CUSTO_FONTE_CONFIAVEL && i.custo_unitario != null;

      const itensReais = amostra.map((a) =>
        item({
          pro_codigo: a.pro_codigo,
          custo_unitario: 44,
          custo_fonte: CUSTO_FONTE_ANCORA_SEM_ESTOQUE,
          estoque_disponivel: 0,
        }),
      );

      expect(itensReais.map(gateAntigo)).toEqual([false, false]);
      expect(avaliada()).toEqual([
        'custo_caiu_sem_estoque_performance_boa',
        'custo_caiu_sem_estoque_performance_ruim',
      ]);
    });
  });

  // Perna de banco real --------------------------------------------------------
  // eslint-disable-next-line jest/no-disabled-tests
  it.skip('pendente-provisao: proporções medidas de custo_fonte e idade do saldo no Postgres real', () => {
    // Requer Postgres `intranet` provisionado para teste (TESTING.md#gates).
  });
});

// ---------------------------------------------------------------------------
// Correções do sinal de giro (T-025 sobre a entrega da T-022)
// ---------------------------------------------------------------------------

const AGORA = new Date('2026-07-29T12:00:00.000Z');

function linhaGiro(over: Partial<GiroMaterializadoRow> = {}): GiroMaterializadoRow {
  return {
    pro_codigo: PRO_COM_XML,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    materializado_em: new Date('2026-07-29T06:00:00.000Z'), // 6h
    group_id: 'GRP-000123',
    grupo_chave: 'PASTILHA FREIO DIANT / GOL G5',
    grupo_qtd_itens: 4,
    demanda_media_dia: 0.85,
    demanda_planejamento_dia: 0.9,
    grp_demanda_media_dia: 3.42,
    grupo_demanda_dia: 3.75,
    tempo_medio_estoque: 47.5,
    // Os três campos que vieram null nos 36 produtos da demo — o ETL não os calcula.
    fator_tendencia: null,
    tendencia_label: null,
    vendas_12m_ant: null,
    vendas_ult_12m: 310,
    grp_vendas_ult_12m: 1240.5,
    grp_vendas_12m_ant: null,
    ...over,
  };
}

function giroService(linha: GiroMaterializadoRow): GiroService {
  const repo = {
    buscarVinculosDeGrupo: jest.fn().mockResolvedValue(new Map([[linha.pro_codigo, 'GRP-000123']])),
    buscarMaterializacao: jest.fn().mockResolvedValue(new Map([[linha.pro_codigo, linha]])),
  } as unknown as GiroRepository;
  return new GiroService(repo);
}

describe('Janela de frescura corrigida para 72h (US-039, retro-04)', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(AGORA);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it('Dado a cadência real do ETL (~3 dias), Então a janela declarada é 72 horas', () => {
    expect(JANELA_FRESCURA_HORAS).toBe(72);
  });

  it('E o valor da janela volta declarado no retorno, não implícito no consumidor', async () => {
    const dto = await giroService(linhaGiro()).sinalGiro(PRO_COM_XML);

    expect(dto.janela_frescura_horas).toBe(72);
  });

  it('E materialização de 48h — antes marcada obsoleta pela janela de 24h — permanece fresca', async () => {
    const dto = await giroService(
      linhaGiro({ materializado_em: new Date('2026-07-27T12:00:00.000Z') }),
    ).sinalGiro(PRO_COM_XML);

    expect(dto).toMatchObject({ idade_horas: 48, frescor: 'fresco', dado_desatualizado: false });
  });

  it('E materialização de 72h atinge a janela e já é obsoleta (limite inclusivo)', async () => {
    const dto = await giroService(
      linhaGiro({ materializado_em: new Date('2026-07-26T12:00:00.000Z') }),
    ).sinalGiro(PRO_COM_XML);

    expect(dto).toMatchObject({ idade_horas: 72, frescor: 'obsoleto', dado_desatualizado: true });
  });
});

describe('Tendência sempre nula removida do contrato (T-025)', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(AGORA);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  it('Dado que os três campos vinham null, Então o objeto `tendencia` não existe mais no retorno', async () => {
    const dto = await giroService(linhaGiro()).sinalGiro(PRO_COM_XML);

    expect(dto).not.toHaveProperty('tendencia');
  });

  it('E nenhum campo do contrato permanece exposto e sempre nulo', async () => {
    const dto = await giroService(linhaGiro()).sinalGiro(PRO_COM_XML);

    for (const removido of ['fator_tendencia', 'tendencia_label', 'rotulo', 'vendas_12m_ant']) {
      expect(dto).not.toHaveProperty(removido);
    }
  });

  it('E vendas_ult_12m — o único campo que o ETL preenche — sobe para o próprio sinal, no escopo efetivo', async () => {
    const dto = await giroService(linhaGiro()).sinalGiro(PRO_COM_XML);

    expect(dto).toMatchObject({ escopo: 'grupo', vendas_ult_12m: 1240.5 });
  });

  it('E no escopo produto vendas_ult_12m vem da coluna do produto', async () => {
    const dto = await giroService(
      linhaGiro({ group_id: null, grp_demanda_media_dia: null, grupo_demanda_dia: null }),
    ).sinalGiro(PRO_COM_XML);

    expect(dto).toMatchObject({ escopo: 'produto', vendas_ult_12m: 310 });
  });
});
