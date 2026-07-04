import { Body, Controller, Delete, Get, Param, Post, Put, Query } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { FornecedorGrupoService } from './fornecedor-grupo.service';
import { SalvarGrupoDto, SalvarParametrosDto } from './fornecedor-grupo.dto';

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

  @Get('fornecedor/:for_codigo')
  @ApiOperation({ summary: 'Cadastro completo de um fornecedor (Stage_Fornecedores) — aba Fornecedor' })
  async fornecedor(@Param('for_codigo') forCodigo: string) {
    const n = Number(forCodigo);
    if (!Number.isFinite(n)) return null;
    return this.service.getFornecedorCompleto(n);
  }

  @Get('cliente/:for_codigo')
  @ApiOperation({ summary: 'Cliente de mesmo CNPJ do fornecedor (chaveia a config de garantia)' })
  async cliente(@Param('for_codigo') forCodigo: string) {
    const n = Number(forCodigo);
    if (!Number.isFinite(n)) return { cliente: null };
    const cliente = await this.service.getClienteDoFornecedor(n);
    return { cliente };
  }

  @Get('clientes')
  @ApiOperation({ summary: 'Busca CLIENTES no ERP (nome/CNPJ/código) para vincular manualmente na garantia' })
  async clientes(@Query('q') q: string) {
    const data = await this.service.buscarClientes(q ?? '');
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

  @Get('parametros/:for_codigo')
  @ApiOperation({ summary: 'Parâmetros de compra do fornecedor (lead time, revisão, pedido mínimo)' })
  async parametros(@Param('for_codigo') forCodigo: string) {
    const n = Number(forCodigo);
    if (!Number.isFinite(n)) return { for_codigo: null, parametros: null };
    return this.service.getParametros(n);
  }

  @Put('parametros')
  @ApiOperation({ summary: 'Salva os parâmetros de compra (replica ao grupo por padrão)' })
  async salvarParametros(@Body() dto: SalvarParametrosDto) {
    const num = (v: any) => {
      const n = Number(v);
      return v == null || v === '' || !Number.isFinite(n) || n <= 0 ? null : n;
    };
    return this.service.salvarParametros({
      forCodigo: Number(dto.for_codigo),
      aplicarGrupo: dto.aplicar_grupo !== false,
      lead_time_dias: num(dto.lead_time_dias),
      tempo_revisao_dias: num(dto.tempo_revisao_dias),
      pedido_minimo_valor: num(dto.pedido_minimo_valor),
      updated_by: dto.updated_by?.trim() || null,
    });
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
