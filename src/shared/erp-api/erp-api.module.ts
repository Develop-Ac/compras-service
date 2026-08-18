import { Global, Module } from '@nestjs/common';
import { ErpApiService } from './erp-api.service';

/** Global como o OpenQueryModule: a leitura do ERP é usada por mais de um módulo. */
@Global()
@Module({
  providers: [ErpApiService],
  exports: [ErpApiService],
})
export class ErpApiModule {}
