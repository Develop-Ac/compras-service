import { Test, TestingModule } from '@nestjs/testing';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { CotacaoSyncService } from './cotacao-sync.service';
import { CotacaoSyncRepository } from './cotacao-sync.repository';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';

describe('CotacaoSyncService', () => {
  let service: CotacaoSyncService;

  const mockCotacaoSyncRepository = {
    upsertFornecedorComItensTx: jest.fn(),
    listFornecedoresLocal: jest.fn(),
  };

  const mockHttpService = {
    get: jest.fn(),
  };

  const mockConfigService = {
    get: jest.fn(),
  };

  const mockOpenQueryService = {
    getPool: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CotacaoSyncService,
        {
          provide: CotacaoSyncRepository,
          useValue: mockCotacaoSyncRepository,
        },
        {
          provide: HttpService,
          useValue: mockHttpService,
        },
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: OpenQueryService,
          useValue: mockOpenQueryService,
        },
      ],
    }).compile();

    service = module.get<CotacaoSyncService>(CotacaoSyncService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('formatMoneyValue', () => {
    it('should format decimal values correctly', () => {
      // Acessar método privado para teste
      const formatMoneyValue = service['formatMoneyValue'].bind(service);
      
      expect(formatMoneyValue(4343.66)).toBe(4343.66);
      expect(formatMoneyValue(4343.6)).toBe(4343.6);
      expect(formatMoneyValue(4343)).toBe(4343);
      expect(formatMoneyValue(4343.666)).toBe(4343.67); // arredondamento
      expect(formatMoneyValue(null)).toBe(null);
      expect(formatMoneyValue(undefined)).toBe(null);
    });

    it('should handle edge cases', () => {
      const formatMoneyValue = service['formatMoneyValue'].bind(service);
      
      expect(formatMoneyValue(0)).toBe(0);
      expect(formatMoneyValue(0.01)).toBe(0.01);
      expect(formatMoneyValue(999999.99)).toBe(999999.99);
    });
  });

  describe('parseMoney', () => {
    it('should parse money strings correctly', () => {
      const parseMoney = service['parseMoney'].bind(service);
      
      expect(parseMoney('test', '4343.66')).toBe(4343.66);
      expect(parseMoney('test', '4.343,66')).toBe(4343.66);
      expect(parseMoney('test', '4343')).toBe(4343);
      expect(parseMoney('test', null)).toBe(null);
      expect(parseMoney('test', '')).toBe(null);
      expect(parseMoney('test', '   ')).toBe(null);
    });
  });
});