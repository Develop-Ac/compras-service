import { IsInt, IsNotEmpty, IsOptional, IsString, ValidateNested, IsArray } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateFornecedorItemDto {
  PEDIDO_COTACAO!: number;
  EMISSAO?: string | null;
  PRO_CODIGO!: number | string;
  PRO_DESCRICAO!: string;
  MAR_DESCRICAO?: string | null;
  REFERENCIA?: string | null;
  UNIDADE?: string | null;
  QUANTIDADE!: number | string;
}

export class CreateFornecedorDto {
  @IsInt()
  pedido_cotacao!: number;

  @IsInt()
  for_codigo!: number;

  @IsString()
  @IsNotEmpty()
  for_nome!: string;

  @IsString()
  @IsOptional()
  cpf_cnpj?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateFornecedorItemDto)
  @IsOptional()
  itens?: CreateFornecedorItemDto[];
}
