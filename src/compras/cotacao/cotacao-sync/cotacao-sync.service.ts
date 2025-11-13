// src/cotacao-sync/cotacao-sync.service.ts
import { Injectable, HttpException, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as sql from 'mssql';

// ✅ novo: serviço compartilhado que gerencia o pool/conexão MSSQL (OpenQuery)
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';
import { CotacaoSyncRepository } from './cotacao-sync.repository';

type NextFornecedor = {
  pedido_cotacao: number;
  for_codigo: number;
  for_nome: string;
  cpf_cnpj: string | null;
  itens: Array<{
    id: number;
    pro_codigo: string;
    pro_descricao: string;
    mar_descricao: string | null;
    referencia: string | null;
    unidade: string | null;
    quantidade: string;
    emissao: string | null;
    valor_unitario: string | null;
  }>;
};

@Injectable()
export class CotacaoSyncService {
  private readonly logger = new Logger(CotacaoSyncService.name);

  constructor(
    private readonly repo: CotacaoSyncRepository,
    private readonly http: HttpService,
    private readonly config: ConfigService,
    private readonly openQuery: OpenQueryService,
  ) {}

  private parseIntStrict(label: string, v: unknown): number {
    const n = Number(v);
    if (!Number.isFinite(n)) throw new HttpException(`${label} inválido: ${v}`, 400);
    return Math.trunc(n);
  }

  private parseMoney(label: string, v: string | null): number | null {
    if (v == null) return null;
    const trimmed = v.trim();
    if (trimmed === '') return null;
    
    // Se contém vírgula, trata como formato brasileiro (1.234,56)
    if (trimmed.includes(',')) {
      const normalized = trimmed.replace(/\./g, '').replace(',', '.');
      const n = Number(normalized);
      if (!Number.isFinite(n) || n < 0) throw new HttpException(`${label} inválido: ${v}`, 400);
      return n;
    }
    
    // Se não contém vírgula, trata como formato americano (1234.56)
    const n = Number(trimmed);
    if (!Number.isFinite(n) || n < 0) throw new HttpException(`${label} inválido: ${v}`, 400);
    return n;
  }

  /**
   * Formata valores monetários para garantir a precisão decimal correta
   */
  private formatMoneyValue(value: number | null): number | null {
    if (value === null || value === undefined) return null;
    // Converte para número e garante 2 casas decimais de precisão
    return Number(Number(value).toFixed(2));
  }

  /**
   * UM SELECT SÓ: busca CUSTO_FABRICA, CUSTO_MEDIO, ESTOQUE_DISPONIVEL
   * em [dbo].[Stage_Produtos] para TODA a lista de códigos (deduplicada) via IN (...).
   * Retorna Map<pro_codigo, { custo_fabrica, custo_medio, estoque_disponivel }>.
   *
   * ⚠️ Requer que o OpenQueryService entregue um pool MSSQL conectado.
   */
  private async fetchProdutosInfoOneShot(
    codigos: number[],
    empresa = 3,
  ): Promise<
    Map<number, { custo_fabrica: number | null; custo_medio: number | null; estoque_disponivel: number | null }>
  > {
    const map = new Map<number, { custo_fabrica: number | null; custo_medio: number | null; estoque_disponivel: number | null }>();

    // Sanitiza e deduplica
    const unique = Array.from(
      new Set(
        codigos
          .map((c) => (Number.isFinite(c) ? Math.trunc(Number(c)) : NaN))
          .filter((n) => Number.isFinite(n)) as number[],
      ),
    );
    if (unique.length === 0) return map;

    // Monta lista literal segura (apenas números)
    const inList = unique.join(',');

    // ✅ usa o pool fornecido pelo OpenQueryService
    const pool = await this.openQuery.getPool();
    const req = new sql.Request(pool);
    req.timeout = 60_000;

    const query = `
      SELECT
          PRO.PRO_CODIGO,
          PRO.CUSTO_FABRICA,
          PRO.CUSTO_MEDIO,
          PRO.ESTOQUE_DISPONIVEL
      FROM [dbo].[Stage_Produtos] AS PRO
      WHERE PRO.EMPRESA = @empresa
        AND PRO.PRO_CODIGO IN (${inList})
      -- OPTION (RECOMPILE)
    `;

    req.input('empresa', sql.Int, empresa);

    const res = await req.query<{
      PRO_CODIGO: number;
      CUSTO_FABRICA: number | null;
      CUSTO_MEDIO: number | null;
      ESTOQUE_DISPONIVEL: number | null;
    }>(query);

    for (const row of res.recordset) {
      map.set(row.PRO_CODIGO, {
        custo_fabrica: row.CUSTO_FABRICA ?? null,
        custo_medio: row.CUSTO_MEDIO ?? null,
        estoque_disponivel: row.ESTOQUE_DISPONIVEL ?? null,
      });
    }

    return map;
  }

  /** Chama o Next (GET /api/cotacao/detalhe?pedido_cotacao=) com timeout de 60s. */
  private async fetchFromNextDetalhe(pedido_cotacao: number) {
    const base = this.config.get<string>('NEXT_BASE_URL', 'http://127.0.0.1:3002');
    if (!base) throw new HttpException('NEXT_BASE_URL não configurado no Nest', 500);

    let url: string;
    try {
      url = new URL(
        `/api/cotacao/detalhe?pedido_cotacao=${encodeURIComponent(String(pedido_cotacao))}`,
        base,
      ).toString();
    } catch {
      throw new HttpException(`NEXT_BASE_URL inválido: ${base}`, 500);
    }

    try {
      const res = await firstValueFrom(
        this.http.get(url, {
          timeout: 60_000,
          maxRedirects: 0,
          validateStatus: () => true,
        }),
      );

      this.logger.debug(`[CotacaoSync] GET ${url} -> status=${res.status}`);

      if (res.status >= 400) {
        throw new HttpException(res.data?.error || 'Falha ao consultar Next', res.status);
      }
      return res.data;
    } catch (err: any) {
      this.logger.error(
        `[CotacaoSync] Falha de conexão com Next: ${base}\n` +
          `code=${err?.code || ''} message=${err?.message || ''}`,
      );
      throw new HttpException(
        {
          error: 'Falha ao contatar Next',
          hint:
            'Confira se o Next está rodando e acessível a partir do processo do Nest. Use 127.0.0.1/host.docker.internal conforme seu setup.',
          details: { base, code: err?.code, message: err?.message },
        },
        502,
      );
    }
  }

  /** Sincroniza e devolve fornecedores + itens enriquecidos com ERP (MSSQL) — com UM SELECT para produtos. */
  async syncByPedido(pedido_cotacao: number) {
    // 1) Buscar no Next
    const data = await this.fetchFromNextDetalhe(pedido_cotacao);

    const fornecedores: NextFornecedor[] = Array.isArray(data?.fornecedores)
      ? data.fornecedores
      : Array.isArray(data)
      ? data
      : [];

    // 2) Persistir local (espelho)
    if (fornecedores.length > 0) {
      const mapped = fornecedores.map((forn) => ({
        pedido_cotacao: forn.pedido_cotacao,
        for_codigo: forn.for_codigo,
        for_nome: forn.for_nome,
        cpf_cnpj: forn.cpf_cnpj,
        itens: (forn.itens || []).map((i) => ({
          emissao: i.emissao ? new Date(i.emissao) : null,
          pro_codigo: this.parseIntStrict('PRO_CODIGO', i.pro_codigo),
          pro_descricao: i.pro_descricao,
          mar_descricao: i.mar_descricao ?? null,
          referencia: i.referencia ?? null,
          unidade: i.unidade ?? null,
          quantidade: this.parseIntStrict('QUANTIDADE', i.quantidade),
          valor_unitario: this.parseMoney('VALOR_UNITARIO', i.valor_unitario) ?? null,
        })),
      }));
      await this.repo.upsertFornecedorComItensTx(mapped);
    }

    // 3) Ler local e enriquecer via ERP — coleta todos os códigos e consulta em UM SELECT
    const fornecedoresLocal = await this.repo.listFornecedoresLocal(pedido_cotacao);

    const allCodes: number[] = [];
    fornecedoresLocal.forEach((f) =>
      f.itens.forEach((it) => {
        if (Number.isFinite(it.pro_codigo)) allCodes.push(it.pro_codigo);
      }),
    );

    const erpMap = await this.fetchProdutosInfoOneShot(allCodes, 3);

    return fornecedoresLocal.map((f) => ({
      ...f,
      itens: f.itens.map((it) => {
        const extra = erpMap.get(it.pro_codigo) || {
          custo_fabrica: null,
          custo_medio: null,
          estoque_disponivel: null,
        };
        return {
          ...it,
          // Garantir que valor_unitario seja retornado como número decimal formatado
          valor_unitario: this.formatMoneyValue(it.valor_unitario),
          // 🔁 retornando custo_fabrica/custo_medio/estoque_disponivel do ERP formatados
          custo_fabrica: this.formatMoneyValue(extra.custo_fabrica),
          custo_medio: this.formatMoneyValue(extra.custo_medio),
          estoque_disponivel: this.formatMoneyValue(extra.estoque_disponivel),
        };
      }),
    }));
  }
}
