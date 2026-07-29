import { BadRequestException, NotFoundException } from '@nestjs/common';
import { JANELA_FRESCURA_HORAS } from '../src/compras/precificacao/dto/sinal-giro.dto';
import {
  GiroMaterializadoRow,
  GiroRepository,
} from '../src/compras/precificacao/giro.repository';
import { GiroService } from '../src/compras/precificacao/giro.service';

/**
 * US-039 / T-022 — sinal de giro real e esperado do grupo de itens iguais.
 *
 * Cobre os três ACs contra duplos das duas fontes lidas (`com_relacionamento_itens` e
 * `com_fifo_completo`, ambas via Prisma, somente leitura): a escolha de escopo
 * grupo × produto, o "sem grupo" explícito e o cálculo da idade contra a janela de
 * frescura são determinísticos e não dependem de banco.
 *
 * A perna que exige Postgres real (conferir, sobre uma amostra, que o agregado do grupo
 * bate com a soma dos membros na materialização corrente) fica `pendente-provisao`,
 * conforme TESTING.md#gates.
 */

const PRO_COM_GRUPO = 38883;
const PRO_SEM_GRUPO = 41207;
const PRO_DESATUALIZADO = 50011;
const GROUP_ID = 'GRP-000123';

const AGORA = new Date('2026-07-29T12:00:00.000Z');
const MATERIALIZADO_FRESCO = new Date('2026-07-29T06:00:00.000Z'); // 6h de idade
const MATERIALIZADO_VELHO = new Date('2026-07-26T12:00:00.000Z'); // 72h = 3 dias

function linha(over: Partial<GiroMaterializadoRow> = {}): GiroMaterializadoRow {
  return {
    pro_codigo: PRO_COM_GRUPO,
    pro_descricao: 'PASTILHA DE FREIO DIANTEIRA',
    materializado_em: MATERIALIZADO_FRESCO,
    group_id: GROUP_ID,
    grupo_chave: 'PASTILHA FREIO DIANT / GOL G5',
    grupo_qtd_itens: 4,
    demanda_media_dia: 0.85,
    demanda_planejamento_dia: 0.9,
    grp_demanda_media_dia: 3.42,
    grupo_demanda_dia: 3.75,
    tempo_medio_estoque: 47.5,
    fator_tendencia: 1.18,
    tendencia_label: 'ALTA',
    vendas_ult_12m: 310,
    vendas_12m_ant: 262,
    grp_vendas_ult_12m: 1240.5,
    grp_vendas_12m_ant: 1051,
    ...over,
  };
}

function fakeRepo(
  vinculos: Array<[number, string]>,
  materializacao: Array<[number, GiroMaterializadoRow]>,
): GiroRepository {
  return {
    buscarVinculosDeGrupo: jest.fn().mockResolvedValue(new Map(vinculos)),
    buscarMaterializacao: jest.fn().mockResolvedValue(new Map(materializacao)),
  } as unknown as GiroRepository;
}

describe('GiroService (US-039 / T-022)', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(AGORA);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
  });

  // AC 1 -----------------------------------------------------------------------
  describe('Dado um produto que pertence a um grupo de itens iguais', () => {
    const service = () =>
      new GiroService(fakeRepo([[PRO_COM_GRUPO, GROUP_ID]], [[PRO_COM_GRUPO, linha()]]));

    it('Então giro real e esperado vêm do GRUPO, não do produto isolado', async () => {
      const dto = await service().sinalGiro(PRO_COM_GRUPO);

      expect(dto.giro).toEqual([
        expect.objectContaining({
          metrica: 'giro_real',
          valor: 3.42, // grp_demanda_media_dia, e não demanda_media_dia (0.85)
          escopo: 'grupo',
          campo_origem: 'com_fifo_completo.grp_demanda_media_dia',
        }),
        expect.objectContaining({
          metrica: 'giro_esperado',
          valor: 3.75, // grupo_demanda_dia, e não demanda_planejamento_dia (0.9)
          escopo: 'grupo',
          campo_origem: 'com_fifo_completo.grupo_demanda_dia',
        }),
      ]);
    });

    it('E recebo a data/hora da materialização que originou os números', async () => {
      const dto = await service().sinalGiro(PRO_COM_GRUPO);

      expect(dto.materializado_em).toBe(MATERIALIZADO_FRESCO.toISOString());
    });

    it('E o escopo declarado é o do grupo, com a identificação do grupo', async () => {
      const dto = await service().sinalGiro(PRO_COM_GRUPO);

      expect(dto).toMatchObject({
        escopo: 'grupo',
        sem_grupo: false,
        group_id: GROUP_ID,
        grupo_qtd_itens: 4,
        agrupamento_materializado: true,
      });
    });
  });

  // AC 2 -----------------------------------------------------------------------
  describe('Dado um produto que não pertence a nenhum grupo de itens iguais', () => {
    const service = () =>
      new GiroService(
        fakeRepo(
          [],
          [
            [
              PRO_SEM_GRUPO,
              linha({
                pro_codigo: PRO_SEM_GRUPO,
                group_id: null,
                grupo_chave: null,
                grupo_qtd_itens: null,
                grp_demanda_media_dia: null,
                grupo_demanda_dia: null,
              }),
            ],
          ],
        ),
      );

    it('Então o retorno indica explicitamente "sem grupo"', async () => {
      const dto = await service().sinalGiro(PRO_SEM_GRUPO);

      expect(dto).toMatchObject({ sem_grupo: true, group_id: null, escopo: 'produto' });
    });

    it('E recebo o sinal do PRÓPRIO produto', async () => {
      const dto = await service().sinalGiro(PRO_SEM_GRUPO);

      expect(dto.giro).toEqual([
        expect.objectContaining({
          metrica: 'giro_real',
          valor: 0.85,
          escopo: 'produto',
          campo_origem: 'com_fifo_completo.demanda_media_dia',
        }),
        expect.objectContaining({
          metrica: 'giro_esperado',
          valor: 0.9,
          escopo: 'produto',
          campo_origem: 'com_fifo_completo.demanda_planejamento_dia',
        }),
      ]);
    });
  });

  // AC 3 -----------------------------------------------------------------------
  describe('Dado que a materialização do giro tem mais de 1 dia', () => {
    const service = () =>
      new GiroService(
        fakeRepo(
          [[PRO_DESATUALIZADO, GROUP_ID]],
          [
            [
              PRO_DESATUALIZADO,
              linha({ pro_codigo: PRO_DESATUALIZADO, materializado_em: MATERIALIZADO_VELHO }),
            ],
          ],
        ),
      );

    it('Então o retorno traz a idade do dado em horas e em dias', async () => {
      const dto = await service().sinalGiro(PRO_DESATUALIZADO);

      expect(dto).toMatchObject({ idade_horas: 72, idade_dias: 3 });
    });

    it('E o dado é marcado como obsoleto contra a janela de frescura declarada', async () => {
      const dto = await service().sinalGiro(PRO_DESATUALIZADO);

      expect(dto).toMatchObject({
        frescor: 'obsoleto',
        dado_desatualizado: true,
        janela_frescura_horas: JANELA_FRESCURA_HORAS,
      });
    });

    it('E dado dentro da janela permanece fresco', async () => {
      const svc = new GiroService(
        fakeRepo([[PRO_COM_GRUPO, GROUP_ID]], [[PRO_COM_GRUPO, linha()]]),
      );

      const dto = await svc.sinalGiro(PRO_COM_GRUPO);

      expect(dto).toMatchObject({ frescor: 'fresco', dado_desatualizado: false, idade_horas: 6 });
    });
  });

  // Bordas ---------------------------------------------------------------------
  describe('Dado um grupo criado depois do último processamento do ETL', () => {
    it('Então o número cai para o escopo do produto e o retorno declara isso', async () => {
      const svc = new GiroService(
        fakeRepo(
          [[PRO_COM_GRUPO, GROUP_ID]],
          [
            [
              PRO_COM_GRUPO,
              linha({ group_id: null, grp_demanda_media_dia: null, grupo_demanda_dia: null }),
            ],
          ],
        ),
      );

      const dto = await svc.sinalGiro(PRO_COM_GRUPO);

      expect(dto).toMatchObject({
        sem_grupo: false,
        group_id: GROUP_ID,
        escopo: 'produto',
        agrupamento_materializado: false,
      });
    });
  });

  describe('Dado um produto sem materialização e sem vínculo de grupo', () => {
    it('Então a consulta por produto responde 404', async () => {
      const svc = new GiroService(fakeRepo([], []));

      await expect(svc.sinalGiro(99999)).rejects.toBeInstanceOf(NotFoundException);
    });
  });

  describe('Dado um pro_codigo inválido', () => {
    it('Então a consulta por produto responde 400', async () => {
      const svc = new GiroService(fakeRepo([], []));

      await expect(svc.sinalGiro('abc')).rejects.toBeInstanceOf(BadRequestException);
    });
  });

  describe('Dado um lote de códigos', () => {
    it('Então códigos desconhecidos simplesmente não voltam na lista', async () => {
      const svc = new GiroService(
        fakeRepo([[PRO_COM_GRUPO, GROUP_ID]], [[PRO_COM_GRUPO, linha()]]),
      );

      const lista = await svc.sinalGiroLote([PRO_COM_GRUPO, 99999]);

      expect(lista.map((d) => d.pro_codigo)).toEqual([PRO_COM_GRUPO]);
    });

    it('E um lote vazio é rejeitado', async () => {
      const svc = new GiroService(fakeRepo([], []));

      await expect(svc.sinalGiroLote([])).rejects.toBeInstanceOf(BadRequestException);
    });
  });

  // Perna de banco real --------------------------------------------------------
  // eslint-disable-next-line jest/no-disabled-tests
  it.skip('pendente-provisao: agregado grp_* confere com a soma dos membros no Postgres real', () => {
    // Requer Postgres `intranet` provisionado para teste (TESTING.md#gates).
  });
});
