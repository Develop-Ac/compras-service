import { Test, TestingModule } from '@nestjs/testing';
import { NotaFiscalService } from './notaFiscal.service';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';

describe('NotaFiscalService', () => {
  let service: NotaFiscalService;
  let openQueryService: OpenQueryService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        NotaFiscalService,
        {
          provide: OpenQueryService,
          useValue: {
            query: jest.fn(),
          },
        },
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