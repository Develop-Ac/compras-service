import { Module } from '@nestjs/common';
import { OpenQueryController } from './openquery.controller';
import { ConsultaOpenqueryService } from './openquery.service';
import { ConsultaOpenqueryRepository } from './openquery.repository';
import { HttpModule } from '@nestjs/axios';
import { OpenQueryModule } from 'src/shared/database/openquery/openquery.module';

@Module({
  imports: [HttpModule, OpenQueryModule], 
  controllers: [OpenQueryController],
  providers: [ConsultaOpenqueryService, ConsultaOpenqueryRepository],
  exports: [ConsultaOpenqueryService, ConsultaOpenqueryRepository],
})
export class OpenQueryHttpModule {}
