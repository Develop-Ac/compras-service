// test/mocks/prisma.mock.ts

const createMockModel = () => ({
  findMany: jest.fn(),
  findFirst: jest.fn(),
  findUnique: jest.fn(),
  create: jest.fn(),
  update: jest.fn(),
  updateMany: jest.fn(),
  delete: jest.fn(),
  deleteMany: jest.fn(),
});

export const mockPrismaService = {
  sis_usuarios: createMockModel(),
  est_contagem: createMockModel(),
  est_contagem_itens: createMockModel(),
  com_cotacao: createMockModel(),
  com_cotacao_itens: createMockModel(),
  com_cotacao_fornecedor: createMockModel(),
  com_cotacao_pedido: createMockModel(),
  $transaction: jest.fn(),
  $connect: jest.fn(),
  $disconnect: jest.fn(),
};