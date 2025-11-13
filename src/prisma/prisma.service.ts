import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PrismaClient, Prisma } from '@prisma/client';

@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, OnModuleDestroy
{
  constructor() {
    super({
      // Se quiser logs, use níveis simples (evita problemas de tipo)
      log: ['warn', 'error'], // adicione 'info' se quiser
      // Se precisar de 'query' para debug, pode adicionar, mas é ruidoso:
      // log: ['query', 'info', 'warn', 'error'],
    });

    // Exemplo: escutar queries (desative em produção)
    // this.$on('query' as any, (e: Prisma.QueryEvent) => {
    //   console.log(`[PRISMA] ${e.query} ${e.params} (${e.duration}ms)`);
    // });
  }

  async onModuleInit() {
    await this.$connect();
  }

  async onModuleDestroy() {
    await this.$disconnect();
  }
}