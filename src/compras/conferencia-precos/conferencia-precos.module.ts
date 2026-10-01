import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { ConferenciaPrecosController } from './conferencia-precos.controller';
import { ConferenciaPrecosService } from './conferencia-precos.service';
import { ConferenciaPrecosVarreduraService } from './conferencia-precos-varredura.service';

// ErpApiModule é global (app.module); aqui só o Prisma.
@Module({
  imports: [PrismaModule],
  controllers: [ConferenciaPrecosController],
  providers: [ConferenciaPrecosService, ConferenciaPrecosVarreduraService],
})
export class ConferenciaPrecosModule {}
