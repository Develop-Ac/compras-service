import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaClient } from '@prisma/client';

import { FornecedorController } from './fornecedor.controller';
import { FornecedorService } from './fornecedor.service';
import { FornecedorRepository } from './fornecedor.repository';
import { FornecedorGrupoModule } from '../../fornecedor-grupo/fornecedor-grupo.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    HttpModule.register({ timeout: 10000, maxRedirects: 0 }),
    FornecedorGrupoModule,
  ],
  controllers: [FornecedorController],
  providers: [
    FornecedorService,
    FornecedorRepository,
    {
      provide: PrismaClient,
      useFactory: () => new PrismaClient(),
    },
  ],
  exports: [FornecedorService, FornecedorRepository],
})
export class FornecedorModule {}
