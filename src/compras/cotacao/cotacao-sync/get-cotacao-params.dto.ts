// src/cotacao-sync/dto/get-cotacao-params.dto.ts
import { IsInt, Min } from 'class-validator';
import { Type } from 'class-transformer';

export class GetCotacaoParamsDto {
  @Type(() => Number)
  @IsInt()
  @Min(1)
  pedido_cotacao!: number;
}
