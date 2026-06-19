import { Body, Controller, Delete, Get, Param, Post, Query } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { FornecedorGrupoService } from './fornecedor-grupo.service';
import { SalvarGrupoDto } from './fornecedor-grupo.dto';

@ApiTags('Compras - Fornecedores (grupos)')
@Controller('fornecedor-grupo')
export class FornecedorGrupoController {
  constructor(private readonly service: FornecedorGrupoService) {}

  @Get()
  @ApiOperation({ summary: 'Lista os grupos de fornecedores relacionados (matriz/filiais)' })
  async listar() {
    const grupos = await this.service.listarGrupos();
    return { data: grupos, total: grupos.length };
  }

  @Get('fornecedores')
  @ApiOperation({ summary: 'Busca fornecedores no ERP (Stage_Fornecedores) por nome/CNPJ/código' })
  async buscar(@Query('q') q: string) {
    const data = await this.service.buscarFornecedores(q ?? '');
    return { data, total: data.length };
  }

  @Get('sugestoes/:for_codigo')
  @ApiOperation({ summary: 'Sugere filiais (mesma raiz de CNPJ) para um fornecedor' })
  async sugestoes(@Param('for_codigo') forCodigo: string, @Query('excluir') excluir?: string) {
    const n = Number(forCodigo);
    if (!Number.isFinite(n)) return { data: [], total: 0 };
    const ja = (excluir ?? '')
      .split(',')
      .map((s) => Number(s.trim()))
      .filter((x) => Number.isFinite(x));
    const data = await this.service.sugerirFiliais(n, ja);
    return { data, total: data.length };
  }

  @Post()
  @ApiOperation({ summary: 'Cria/atualiza um grupo de fornecedores' })
  async salvar(@Body() dto: SalvarGrupoDto) {
    const membros = (dto.membros ?? []).map((m) => Number(m)).filter((x) => Number.isFinite(x));
    return this.service.salvarGrupo({
      groupId: dto.group_id ?? null,
      membros,
      principal: dto.principal == null ? null : Number(dto.principal),
    });
  }

  @Delete('fornecedor/:for_codigo')
  @ApiOperation({ summary: 'Remove um fornecedor de qualquer grupo' })
  async removerFornecedor(@Param('for_codigo') forCodigo: string) {
    return this.service.removerFornecedor(Number(forCodigo));
  }

  @Delete('grupo/:group_id')
  @ApiOperation({ summary: 'Dissolve um grupo inteiro' })
  async removerGrupo(@Param('group_id') groupId: string) {
    return this.service.removerGrupo(groupId);
  }
}
