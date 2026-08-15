import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { RabbitMqConsumerService } from '../src/compras/cotacao/job/rabbitmq-consumer.service';
import { PrismaService } from '../src/prisma/prisma.service';
import { OpenQueryService } from '../src/shared/database/openquery/openquery.service';

/**
 * US-047 / T-027 — **primeiro smoke HTTP do container**.
 *
 * ## Por que ele existe
 *
 * Até a sprint-05 nenhum gate compilava o `AppModule`. Os specs de precificação montavam
 * services à mão e o smoke de DI resolvia só o `PrecificacaoModule` isolado — e é o
 * `AppModule` que registra o `RouterModule` com o prefixo `compras`. Ou seja: as 13 rotas do
 * módulo estavam documentadas no Swagger e **nunca tinham sido exercidas por HTTP**. Um erro
 * de prefixo, um controller fora de `controllers[]` ou um `RouterModule.register` esquecido
 * passariam verdes por toda a suíte e só apareceriam em produção.
 *
 * ## O que ele prova (e o que não prova)
 *
 * Prova **roteamento**: que cada fatia responde sob `/compras/precificacao/...` e **não**
 * responde sem o prefixo, e que os códigos declarados nos `@ApiResponse` (400 / 404 / 503)
 * saem como declarados em ao menos um caso cada.
 *
 * **Não** prova I/O: Prisma e o ERP (OPENQUERY) são dublês. É de propósito — os casos abaixo
 * foram escolhidos entre os que decidem **antes** de qualquer leitura (validação de formato)
 * ou **a partir de uma leitura vazia** (a régua sem faixas ⇒ 503). Nenhuma asserção depende
 * de dado real, então o smoke é determinístico em qualquer máquina, sem Postgres e sem ERP.
 *
 * O consumidor de verdade destas rotas é o proxy `/api/proxy/financeiro` do front; o que este
 * spec fixa é o contrato de caminho que aquele proxy monta.
 */

/** Prisma dublê: qualquer model, qualquer leitura, resultado vazio. */
function prismaVazio(): Record<string, unknown> {
  const model = new Proxy(
    {},
    {
      get: (_alvo, metodo: string) => {
        if (metodo === 'findMany') return jest.fn().mockResolvedValue([]);
        if (metodo === 'count') return jest.fn().mockResolvedValue(0);
        if (metodo === 'aggregate' || metodo === 'groupBy') return jest.fn().mockResolvedValue([]);
        return jest.fn().mockResolvedValue(null);
      },
    },
  );

  return new Proxy(
    {},
    {
      get: (_alvo, prop: string) => {
        if (prop === 'then') return undefined; // não finge ser Promise
        if (prop === '$connect' || prop === '$disconnect') return jest.fn().mockResolvedValue(undefined);
        if (prop === '$queryRaw' || prop === '$queryRawUnsafe') return jest.fn().mockResolvedValue([]);
        if (prop === '$executeRaw' || prop === '$executeRawUnsafe') return jest.fn().mockResolvedValue(0);
        if (prop === '$transaction') {
          return jest.fn(async (arg: unknown) =>
            typeof arg === 'function' ? (arg as (t: unknown) => unknown)(prismaVazio()) : [],
          );
        }
        if (prop === 'onModuleInit' || prop === 'onModuleDestroy' || prop === 'enableShutdownHooks') {
          return jest.fn().mockResolvedValue(undefined);
        }
        return model;
      },
    },
  );
}

/** Base de todas as rotas do módulo — o prefixo vem do `RouterModule` do `AppModule`. */
const BASE = '/compras/precificacao';

describe('Rotas HTTP do módulo precificação (smoke do AppModule completo)', () => {
  let app: INestApplication;
  let http: ReturnType<typeof request>;

  beforeAll(async () => {
    // Given o AppModule INTEIRO — é ele que monta o RouterModule com o prefixo `compras`
    const moduleRef: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      // Bordas de I/O trocadas por dublês: o alvo é o roteamento, não o banco.
      .overrideProvider(PrismaService)
      .useValue(prismaVazio())
      .overrideProvider(OpenQueryService)
      .useValue({
        query: jest.fn().mockResolvedValue([]),
        queryOne: jest.fn().mockResolvedValue(null),
        exec: jest.fn().mockResolvedValue(undefined),
        getPool: jest.fn().mockRejectedValue(new Error('ERP indisponível no smoke')),
        onModuleDestroy: jest.fn().mockResolvedValue(undefined),
        dispose: jest.fn().mockResolvedValue(undefined),
      })
      // O consumer do RabbitMQ reconecta a cada 5s sozinho; num teste isso só deixa
      // handle aberto. Sem fila, sem timer.
      .overrideProvider(RabbitMqConsumerService)
      .useValue({ onModuleInit: jest.fn(), onModuleDestroy: jest.fn() })
      .compile();

    app = moduleRef.createNestApplication();
    // When o app HTTP sobe de verdade (init roda os lifecycle hooks e monta as rotas)
    await app.init();
    http = request(app.getHttpServer());
  }, 60_000);

  afterAll(async () => {
    await app?.close();
  });

  // =========================================================================
  // Uma rota por fatia — as SEIS fatias, sob o prefixo `compras`
  // =========================================================================

  /**
   * `Cannot GET /x` é a resposta do Nest para caminho **não roteado**. Se essa mensagem
   * aparece, a rota não existe; se não aparece, o handler foi alcançado — que é exatamente
   * o que este bloco mede. O status em si é medido nos blocos de código declarado abaixo.
   */
  const naoRoteada = (corpo: any) => /^Cannot (GET|POST|PUT|PATCH|DELETE) /.test(corpo?.message ?? '');

  it.each([
    ['custo', `${BASE}/custo/nfe/35260712345678901234550010000123451234567890`],
    ['tabelas-preco', `${BASE}/tabelas-preco/33090`],
    ['giro', `${BASE}/giro/33090`],
    ['regua', `${BASE}/regua`],
    ['proposta', `${BASE}/proposta/nfe/35260712345678901234550010000123451234567890`],
    ['triagem', `${BASE}/triagem/nfe/35260712345678901234550010000123451234567890`],
  ])('a fatia %s responde sob o prefixo compras', async (_fatia, caminho) => {
    const res = await http.get(caminho);

    // Then o handler foi alcançado — não é o 404 de rota inexistente do Nest
    expect(naoRoteada(res.body)).toBe(false);
  });

  it.each([
    ['custo', '/precificacao/custo/nfe/35260712345678901234550010000123451234567890'],
    ['tabelas-preco', '/precificacao/tabelas-preco/33090'],
    ['giro', '/precificacao/giro/33090'],
    ['regua', '/precificacao/regua'],
    ['proposta', '/precificacao/proposta/nfe/35260712345678901234550010000123451234567890'],
    ['triagem', '/precificacao/triagem/nfe/35260712345678901234550010000123451234567890'],
  ])('a fatia %s NÃO responde sem o prefixo compras', async (_fatia, caminho) => {
    const res = await http.get(caminho);

    // Then o prefixo é obrigatório — o proxy do front precisa montá-lo
    expect(res.status).toBe(404);
    expect(naoRoteada(res.body)).toBe(true);
  });

  it('a régua expõe as quatro rotas declaradas no Swagger, todas sob o prefixo', async () => {
    const respostas = await Promise.all([
      http.get(`${BASE}/regua/historico`),
      http.get(`${BASE}/regua/corredor?tabela=varejo&custo=40.48`),
      http.post(`${BASE}/regua/avaliar`).send({ tabela: 'varejo', custo: 40.48, preco: 101.2 }),
      http.put(`${BASE}/regua/faixa`).send([]),
    ]);

    // Then nenhuma delas é caminho inexistente
    for (const res of respostas) expect(naoRoteada(res.body)).toBe(false);
  });

  // =========================================================================
  // Os códigos declarados nos @ApiResponse — um caso de cada
  // =========================================================================

  it('400: pro_codigo não numérico em tabelas-preco sai como Bad Request', async () => {
    // Given um pro_codigo que não é número — validação antes de qualquer I/O
    const res = await http.get(`${BASE}/tabelas-preco/abc`);

    // Then o código é o declarado no @ApiResponse do controller
    expect(res.status).toBe(400);
    expect(String(res.body.message)).toContain('pro_codigo inválido');
  });

  it('400: pro_codigo não numérico em giro sai como Bad Request', async () => {
    const res = await http.get(`${BASE}/giro/abc`);

    expect(res.status).toBe(400);
  });

  it('404: chave de NF-e fora do formato de 44 dígitos sai como Not Found no custo', async () => {
    // Given uma chave curta — o service recusa antes de consultar o espelho
    const res = await http.get(`${BASE}/custo/nfe/123`);

    // Then o código é o declarado no @ApiResponse do controller
    expect(res.status).toBe(404);
  });

  it('404: a proposta herda o Not Found do custo para chave inválida', async () => {
    // Given a mesma chave inválida, agora pela fatia que orquestra as outras cinco
    const res = await http.get(`${BASE}/proposta/nfe/123`);

    // Then a proposta não engole o erro da fatia de custo
    expect(res.status).toBe(404);
  });

  it('404: a triagem herda o Not Found da proposta para chave inválida', async () => {
    // Given a mesma chave inválida, agora pela fatia que classifica a proposta
    const res = await http.get(`${BASE}/triagem/nfe/123`);

    // Then a triagem não engole o erro das fatias que consome
    expect(res.status).toBe(404);
  });

  /**
   * A série da triagem é o **contra-exemplo deliberado** da régua: com a DDL manual ausente
   * (`sql/2026-07-30_precificacao_triagem_execucao.sql`, entregue e não aplicada) a régua
   * responde 503 e derruba a proposta, enquanto a série responde **200 declarando-se
   * indisponível**. A triagem não fica meio-desligada esperando o responsável rodar o `.sql`.
   */
  it('200: a série de execuções responde mesmo sem a DDL aplicada, declarando o estado', async () => {
    const res = await http.get(`${BASE}/triagem/execucoes`);

    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty('disponivel');
    expect(Array.isArray(res.body.execucoes)).toBe(true);
  });

  it('503: régua sem faixas vigentes sai como Service Unavailable', async () => {
    // Given `com_precificacao_faixa` vazia (o Prisma dublê devolve [])
    const res = await http.get(`${BASE}/regua`);

    // Then a régua se declara indisponível em vez de inventar corredor — o código
    // declarado no @ApiResponse
    expect(res.status).toBe(503);
  });
});
