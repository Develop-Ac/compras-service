import { Module } from '@nestjs/common';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';
import { VinculacaoNfeController } from './vinculacao-nfe.controller';
import { VinculacaoNfeService } from './vinculacao-nfe.service';
import { VinculacaoNfeRepository } from './vinculacao-nfe.repository';

@Module({
  imports: [OpenQueryModule],
  controllers: [VinculacaoNfeController],
  providers: [VinculacaoNfeService, VinculacaoNfeRepository],
  exports: [VinculacaoNfeService],
})
export class VinculacaoNfeModule {}
