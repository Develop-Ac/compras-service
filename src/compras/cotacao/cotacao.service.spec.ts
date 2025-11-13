import { Test, TestingModule } from '@nestjs/testing';
import { NotFoundException } from '@nestjs/common';
import { CotacaoService } from '../../../src/compras/cotacao/cotacao.service';
import { CotacaoRepository } from '../../../src/compras/cotacao/cotacao.repository';
import { CreateCotacaoDto } from '../../../src/compras/cotacao/cotacao.dto';

describe('CotacaoService', () => {
  let service: CotacaoService;
  let repository: any;

  const mockCotacaoRepository = {
    upsertCotacaoWithItems: jest.fn(),
    countCotacao: jest.fn(),
    listHeaders: jest.fn(),
    groupItemCounts: jest.fn(),
    listItensForPedidos: jest.fn(),
    getCotacaoHeader: jest.fn(),
    listItensByPedido: jest.fn(),
    findAll: jest.fn(),
    findByPedidoCotacao: jest.fn(),
    delete: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CotacaoService,
        {
          provide: CotacaoRepository,
          useValue: mockCotacaoRepository,
        },
      ],
    }).compile();

    service = module.get<CotacaoService>(CotacaoService);
    repository = module.get<CotacaoRepository>(CotacaoRepository);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('upsertCotacao', () => {
    it('deve criar ou atualizar uma cotação com itens', async () => {
      const createCotacaoDto: CreateCotacaoDto = {
        empresa: 3,
        pedido_cotacao: 12345,
        itens: [
          {
            PEDIDO_COTACAO: 12345,
            EMISSAO: '2024-01-15',
            PRO_CODIGO: 23251,
            PRO_DESCRICAO: 'CAPA P/CHOQUE DIANT. S-10 12/16 PRETO LISO - DTS',
            MAR_DESCRICAO: 'DTS',
            REFERENCIA: '056597',
            UNIDADE: 'UN',
            QUANTIDADE: 2,
          },
        ],
      };

      const mockCotacaoResult = {
        id: 'cotacao-123',
        empresa: 3,
        pedido_cotacao: 12345,
        com_cotacao_itens: [
          {
            id: 'item-456',
            pedido_cotacao: 12345,
            emissao: new Date('2024-01-15T00:00:00Z'),
            pro_codigo: 23251,
            pro_descricao: 'CAPA P/CHOQUE DIANT. S-10 12/16 PRETO LISO - DTS',
            mar_descricao: 'DTS',
            referencia: '056597',
            unidade: 'UN',
            quantidade: 2,
          },
        ],
      };

      repository.upsertCotacaoWithItems.mockResolvedValue(mockCotacaoResult);

      const result = await service.upsertCotacao(createCotacaoDto);

      expect(repository.upsertCotacaoWithItems).toHaveBeenCalledWith(
        3,
        12345,
        expect.arrayContaining([
          expect.objectContaining({
            pedido_cotacao: 12345,
            emissao: expect.any(Date),
            pro_codigo: 23251,
            pro_descricao: 'CAPA P/CHOQUE DIANT. S-10 12/16 PRETO LISO - DTS',
            mar_descricao: 'DTS',
            referencia: '056597',
            unidade: 'UN',
            quantidade: 2,
          }),
        ]),
      );
      expect(result).toEqual({
        ok: true,
        empresa: 3,
        pedido_cotacao: 12345,
        total_itens: 1
      });
    });

    it('deve tratar itens sem data de emissão', async () => {
      const createCotacaoDto: CreateCotacaoDto = {
        empresa: 3,
        pedido_cotacao: 12345,
        itens: [
          {
            PEDIDO_COTACAO: 12345,
            PRO_CODIGO: 23251,
            PRO_DESCRICAO: 'PRODUTO TESTE',
            QUANTIDADE: 1,
          },
        ],
      };

      const mockCotacaoResult = {
        id: 'cotacao-123',
        empresa: 3,
        pedido_cotacao: 12345,
        com_cotacao_itens: [],
      };

      repository.upsertCotacaoWithItems.mockResolvedValue(mockCotacaoResult);

      await service.upsertCotacao(createCotacaoDto);

      expect(repository.upsertCotacaoWithItems).toHaveBeenCalledWith(
        3,
        12345,
        expect.arrayContaining([
          expect.objectContaining({
            emissao: null,
            pro_codigo: 23251,
            pro_descricao: 'PRODUTO TESTE',
            quantidade: 1,
          }),
        ]),
      );
    });

    it('deve propagar erro do repository', async () => {
      const createCotacaoDto: CreateCotacaoDto = {
        empresa: 3,
        pedido_cotacao: 12345,
        itens: [],
      };

      repository.upsertCotacaoWithItems.mockRejectedValue(new Error('Erro na base de dados'));

      await expect(service.upsertCotacao(createCotacaoDto)).rejects.toThrow('Erro na base de dados');
    });
  });

  describe('listAll', () => {
    it('deve retornar lista paginada de cotações', async () => {
      const params = {
        empresa: 3,
        page: 1,
        pageSize: 10,
        includeItems: true,
      };

      const mockCotacoes = [
        {
          id: 'cotacao-123',
          empresa: 3,
          pedido_cotacao: 12345,
          com_cotacao_itens: [
            {
              id: 'item-456',
              pro_codigo: 23251,
              pro_descricao: 'PRODUTO TESTE',
              quantidade: 2,
            },
          ],
        },
      ];

      repository.countCotacao.mockResolvedValue(10);
      repository.listHeaders.mockResolvedValue(mockCotacoes);
      repository.groupItemCounts.mockResolvedValue([]);
      repository.listItensForPedidos.mockResolvedValue([]);

      const result = await service.listAll(params);

      expect(repository.countCotacao).toHaveBeenCalled();
      expect(repository.listHeaders).toHaveBeenCalled();
      expect(result).toHaveProperty('total');
      expect(result).toHaveProperty('data');
    });

    it('deve usar parâmetros padrão quando não especificados', async () => {
      const params = {
        page: 1,
        pageSize: 10,
        includeItems: false,
      };

      const mockCotacoes = [];
      repository.countCotacao.mockResolvedValue(0);
      repository.listHeaders.mockResolvedValue(mockCotacoes);
      repository.groupItemCounts.mockResolvedValue([]);
      repository.listItensForPedidos.mockResolvedValue([]);

      const result = await service.listAll(params);

      expect(repository.countCotacao).toHaveBeenCalled();
      expect(repository.listHeaders).toHaveBeenCalled();
      expect(result).toHaveProperty('total');
      expect(result).toHaveProperty('data');
    });
  });

});