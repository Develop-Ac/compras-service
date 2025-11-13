import { Module } from '@nestjs/common';
import { NotaFiscalController } from './notaFiscal.controller';
import { NotaFiscalService } from './notaFiscal.service';
import { NotaFiscalRepository } from './notaFiscal.repository';
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';

@Module({
  controllers: [NotaFiscalController],
  providers: [NotaFiscalService, NotaFiscalRepository, OpenQueryService],
})
export class NotaFiscalModule {}