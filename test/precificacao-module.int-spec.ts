import { Test, TestingModule } from '@nestjs/testing';
import { CustoController } from '../src/compras/precificacao/custo.controller';
import { CustoService } from '../src/compras/precificacao/custo.service';
import { GiroController } from '../src/compras/precificacao/giro.controller';
import { GiroService } from '../src/compras/precificacao/giro.service';
import { ElegibilidadeService } from '../src/compras/precificacao/elegibilidade.service';
import { PrecificacaoModule } from '../src/compras/precificacao/precificacao.module';
import { PropostaController } from '../src/compras/precificacao/proposta.controller';
import { PropostaRepository } from '../src/compras/precificacao/proposta.repository';
import { PropostaService } from '../src/compras/precificacao/proposta.service';
import { ReguaController } from '../src/compras/precificacao/regua.controller';
import { ReguaService } from '../src/compras/precificacao/regua.service';
import { TabelasPrecoController } from '../src/compras/precificacao/tabelas-preco.controller';
import { TabelasPrecoService } from '../src/compras/precificacao/tabelas-preco.service';

/**
 * US-040 / T-023 — smoke de bootstrap do `PrecificacaoModule`.
 *
 * Gate exigido por TESTING.md#gates ("smoke de bootstrap de módulo obrigatório por
 * módulo Nest novo ou estendido"): o build compile-only da sprint-04 passava verde e
 * deixava erro de DI escapar para runtime — `notaFiscal.service.spec.ts` compila e
 * falha por injeção, e nenhum dos cinco comandos de gate daquela sprint resolvia o
 * `PrecificacaoModule`. Este spec resolve o grafo de verdade.
 *
 * `.compile()` instancia os providers (roda os construtores e cobra cada dependência do
 * grafo) **sem** disparar `onModuleInit` — portanto não abre conexão com o Postgres nem
 * com o ERP. É um teste de fiação de DI, não de I/O: qualquer provider faltando em
 * `providers`/`imports` derruba a suíte aqui, e não em produção.
 */
describe('PrecificacaoModule (smoke de bootstrap)', () => {
  let moduleRef: TestingModule;

  beforeAll(async () => {
    // Given o módulo de precificação declarado com PrismaModule + OpenQueryModule
    // When o container do Nest resolve o grafo completo de dependências
    moduleRef = await Test.createTestingModule({
      imports: [PrecificacaoModule],
    }).compile();
  });

  afterAll(async () => {
    await moduleRef?.close();
  });

  it('resolve o grafo de DI do módulo sem provider faltando', () => {
    // Then o container existe — o compile() do beforeAll não lançou
    expect(moduleRef).toBeDefined();
  });

  // As SEIS fatias do módulo, nomeadas uma a uma (T-027). Até a sprint-05 só as três
  // primeiras tinham assertion: régua, elegibilidade e proposta eram cobertas por
  // *implicação* do `.compile()` acima — se faltasse provider, o beforeAll derrubava a
  // suíte inteira. A implicação vale, mas cobertura declarada nomeia o culpado.
  it.each([
    ['CustoService', CustoService],
    ['TabelasPrecoService', TabelasPrecoService],
    ['GiroService', GiroService],
    ['ReguaService', ReguaService],
    ['ElegibilidadeService', ElegibilidadeService],
    ['PropostaService', PropostaService],
  ])('injeta o provider exportado %s', (_nome, token) => {
    // Then cada service exportado é instanciável a partir do container
    expect(moduleRef.get(token)).toBeInstanceOf(token);
  });

  it('injeta o PropostaRepository, extraído do service pela T-027', () => {
    // Then a porta de leitura da proposta é um provider de verdade do módulo, e não um
    // objeto construído à mão dentro do service
    expect(moduleRef.get(PropostaRepository)).toBeInstanceOf(PropostaRepository);
  });

  // Cinco controllers para seis fatias: `ElegibilidadeService` é política pura e foi
  // exportada de propósito sem controller próprio (ver o cabeçalho do módulo).
  it.each([
    ['CustoController', CustoController],
    ['TabelasPrecoController', TabelasPrecoController],
    ['GiroController', GiroController],
    ['ReguaController', ReguaController],
    ['PropostaController', PropostaController],
  ])('injeta o controller registrado %s', (_nome, token) => {
    // Then cada controller do módulo recebe suas dependências de construtor
    expect(moduleRef.get(token)).toBeInstanceOf(token);
  });
});
