import { Module } from '@nestjs/common';
import { ItensController } from './itens.controller';
import { ItensService } from './itens.service';
import { ItensRepository } from './itens.repository';
import { OpenQueryModule } from '../../shared/database/openquery/openquery.module';

@Module({
	imports: [OpenQueryModule],
	controllers: [ItensController],
	providers: [ItensService, ItensRepository],
})
export class ItensModule {}
