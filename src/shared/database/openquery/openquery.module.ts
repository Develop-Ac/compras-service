import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { OpenQueryService } from './openquery.service';

@Module({
  imports: [ConfigModule],
  providers: [OpenQueryService],
  exports: [OpenQueryService], // ← exporta para outros módulos
})
export class OpenQueryModule {}
