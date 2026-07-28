import { Global, Module } from '@nestjs/common';
import { CompraCasadaController } from './compra-casada.controller';
import { CompraCasadaFornecedoresRepository } from './compra-casada.repository';
import { CompraCasadaFornecedoresService } from './compra-casada.service';
import { MssqlService } from '../../common/mssql/mssql.service';

@Global()
@Module({
  controllers: [CompraCasadaController],
  providers: [CompraCasadaFornecedoresRepository, CompraCasadaFornecedoresService, MssqlService],
})
export class CompraCasadaModule {}
