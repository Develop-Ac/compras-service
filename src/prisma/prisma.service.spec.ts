import { Test, TestingModule } from '@nestjs/testing';
import { PrismaService } from './prisma.service';

describe('PrismaService', () => {
  let service: PrismaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PrismaService],
    }).compile();

    service = module.get<PrismaService>(PrismaService);
  });

  afterEach(async () => {
    // Cleanup
    if (service) {
      await service.$disconnect();
    }
  });

  describe('onModuleInit', () => {
    it('deve inicializar corretamente', async () => {
      // Mock do método $connect para evitar conexão real
      const connectSpy = jest.spyOn(service, '$connect').mockResolvedValue();

      await service.onModuleInit();

      expect(connectSpy).toHaveBeenCalled();

      connectSpy.mockRestore();
    });

    it('deve propagar erro de conexão', async () => {
      const connectSpy = jest.spyOn(service, '$connect').mockRejectedValue(new Error('Falha na conexão'));

      await expect(service.onModuleInit()).rejects.toThrow('Falha na conexão');

      connectSpy.mockRestore();
    });
  });

  describe('onModuleDestroy', () => {
    it('deve desconectar corretamente', async () => {
      const disconnectSpy = jest.spyOn(service, '$disconnect').mockResolvedValue();

      await service.onModuleDestroy();

      expect(disconnectSpy).toHaveBeenCalled();

      disconnectSpy.mockRestore();
    });

    it('deve propagar erro de desconexão', async () => {
      const disconnectSpy = jest.spyOn(service, '$disconnect').mockRejectedValue(new Error('Falha na desconexão'));

      await expect(service.onModuleDestroy()).rejects.toThrow('Falha na desconexão');

      disconnectSpy.mockRestore();
    });
  });

  describe('integração', () => {
    it('deve ser definido', () => {
      expect(service).toBeDefined();
    });

    it('deve ter cliente Prisma disponível', () => {
      expect(service.sis_usuarios).toBeDefined();
      expect(service.est_contagem).toBeDefined();
      expect(service.est_contagem_itens).toBeDefined();
      expect(service.com_cotacao).toBeDefined();
      expect(service.com_cotacao_itens).toBeDefined();
    });

    it('deve ter métodos de transação disponíveis', () => {
      expect(service.$transaction).toBeDefined();
      expect(typeof service.$transaction).toBe('function');
    });

    it('deve ter métodos de conexão disponíveis', () => {
      expect(service.$connect).toBeDefined();
      expect(service.$disconnect).toBeDefined();
      expect(typeof service.$connect).toBe('function');
      expect(typeof service.$disconnect).toBe('function');
    });
  });
});