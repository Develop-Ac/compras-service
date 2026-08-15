import { Test, TestingModule } from '@nestjs/testing';
import { NotaFiscalService } from './notaFiscal.service';
import { NotaFiscalRepository } from './notaFiscal.repository';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';
import { PrismaService } from '../../../prisma/prisma.service';
import { FornecedorGrupoService } from '../../fornecedor-grupo/fornecedor-grupo.service';

/**
 * T-027 — DI reparada. O spec nasceu quando o `NotaFiscalService` falava direto com o
 * `OpenQueryService`; hoje o construtor pede `NotaFiscalRepository`, `PrismaService` e
 * `FornecedorGrupoService`, e a suíte quebrava com "Nest can't resolve dependencies of the
 * NotaFiscalService ... NotaFiscalRepository at index [0]".
 *
 * A correção **provê o repositório real** (ele é fino: monta o SQL e delega ao
 * `OpenQueryService`, que continua sendo o único dublê de I/O). Assim a asserção original —
 * `openQueryService.query` chamado com uma string — continua verdadeira e continua medindo
 * a mesma coisa: que `getNfeDistribuicao` chega ao ERP. Prisma e o serviço de grupo entram
 * como dublês vazios porque `getNfeDistribuicao` não passa por eles.
 */
describe('NotaFiscalService', () => {
  let service: NotaFiscalService;
  let openQueryService: OpenQueryService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        NotaFiscalService,
        NotaFiscalRepository,
        {
          provide: OpenQueryService,
          useValue: {
            query: jest.fn(),
          },
        },
        { provide: PrismaService, useValue: {} },
        { provide: FornecedorGrupoService, useValue: {} },
      ],
    }).compile();

    service = module.get<NotaFiscalService>(NotaFiscalService);
    openQueryService = module.get<OpenQueryService>(OpenQueryService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should fetch NFE_DISTRIBUICAO data', async () => {
    const mockData = [{ id: 1 }];
    jest.spyOn(openQueryService, 'query').mockResolvedValue(mockData);

    const result = await service.getNfeDistribuicao();
    expect(result).toEqual(mockData);
    expect(openQueryService.query).toHaveBeenCalledWith(expect.any(String));
  });
});