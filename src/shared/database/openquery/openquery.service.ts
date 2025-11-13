// src/shared/database/openquery/openquery.service.ts
import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as sql from 'mssql';

export type MssqlQueryParams = Record<
  string,
  | { type?: sql.ISqlTypeFactory; value: any }
  | number
  | string
  | null
  | Date
  | Buffer
  | boolean
>;

export interface MssqlQueryOptions {
  /** Timeout por request (ms). Default: 60_000 */
  timeout?: number;
  /** Se true, não lança erro quando não houver linhas; retorna [] */
  allowZeroRows?: boolean;
}

@Injectable()
export class OpenQueryService implements OnModuleDestroy {
  private readonly logger = new Logger(OpenQueryService.name);
  private pool?: sql.ConnectionPool;

  constructor(private readonly config: ConfigService) {}

  /**
   * Lê configuração do MSSQL de env ou usa defaults do seu projeto.
   */
  private getConfig(): sql.config {
    const env = (k: string, def?: any) => this.config.get(k) ?? def;

    // Defaults copiados do seu código anterior:
    const server = env('MSSQL_HOST', '192.168.1.146');
    const port = Number(env('MSSQL_PORT', 1433));
    const database = env('MSSQL_DB', 'BI');
    const user = env('MSSQL_USER', 'BI_AC');
    const password = env('MSSQL_PASSWORD', 'Ac@2025acesso');

    const encrypt = String(env('MSSQL_ENCRYPT', 'false')).toLowerCase() === 'true';
    const trust = String(env('MSSQL_TRUST_CERT', 'true')).toLowerCase() === 'true';

    const requestTimeout = Number(env('MSSQL_REQUEST_TIMEOUT_MS', 3_600_000));
    const cancelTimeout = Number(env('MSSQL_CANCEL_TIMEOUT_MS', 3_600_000));
    const connectTimeout = Number(env('MSSQL_CONNECT_TIMEOUT_MS', 60_000));

    const poolMax = Number(env('MSSQL_POOL_MAX', 10));
    const poolMin = Number(env('MSSQL_POOL_MIN', 0));
    const poolIdle = Number(env('MSSQL_POOL_IDLE_MS', 30_000));

    const cfg: sql.config = {
      server,
      port,
      database,
      user,
      password,
      options: {
        encrypt,
        trustServerCertificate: trust,
        enableArithAbort: true,
        requestTimeout,
        cancelTimeout,
        connectTimeout,
      },
      pool: {
        max: poolMax,
        min: poolMin,
        idleTimeoutMillis: poolIdle,
      },
    };

    return cfg;
  }

  /**
   * Retorna (e inicializa se preciso) o pool MSSQL.
   * Inclui um teste "SELECT 1" na primeira conexão.
   */
  async getPool(): Promise<sql.ConnectionPool> {
    if (this.pool) return this.pool;

    const cfg = this.getConfig();
    this.logger.log(
      `[MSSQL] Conectando em ${cfg.server}:${cfg.port} db=${cfg.database} (encrypt=${cfg.options?.encrypt}, trust=${cfg.options?.trustServerCertificate})`,
    );

    const pool = new sql.ConnectionPool(cfg);
    await pool.connect();

    // Smoke test
    try {
      const r = new sql.Request(pool);
      r.timeout = 60_000;
      const test = await r.query<{ ok: number }>('SELECT 1 AS ok');
      this.logger.log(`[MSSQL] conectado. Teste= ${test?.recordset?.[0]?.ok}`);
    } catch (e: any) {
      this.logger.error('[MSSQL] Falha no teste de conexão: ' + (e?.message || e));
      throw e;
    }

    this.pool = pool;
    return pool;
  }

  /**
   * Executa SELECT e retorna recordset tipado.
   * @param text  SQL com parâmetros @nome
   * @param params  Mapa de parâmetros
   * @param opts  Opções (timeout, etc.)
   */
  async query<T = any>(
    text: string,
    params: MssqlQueryParams = {},
    opts: MssqlQueryOptions = {},
  ): Promise<T[]> {
    const pool = await this.getPool();
    const req = new sql.Request(pool);
    req.timeout = opts.timeout ?? 60_000;

    // Bind de parâmetros
    for (const [name, raw] of Object.entries(params)) {
      if (raw && typeof raw === 'object' && 'value' in raw) {
        const r = raw as { type?: sql.ISqlTypeFactory; value: any };
        if (r.type) req.input(name, r.type, r.value);
        else req.input(name, r.value);
      } else {
        req.input(name, raw as any);
      }
    }

    try {
      const { recordset } = await req.query<T>(text);
      if (!recordset?.length && !opts.allowZeroRows) return [];
      return recordset ?? [];
    } catch (err: any) {
      this.logger.error(this.formatSqlError('query', text, params, err));
      throw err;
    }
  }

  /**
   * Executa SELECT e retorna a primeira linha ou undefined.
   */
  async queryOne<T = any>(
    text: string,
    params: MssqlQueryParams = {},
    opts: MssqlQueryOptions = {},
  ): Promise<T | undefined> {
    const rs = await this.query<T>(text, params, { ...opts, allowZeroRows: true });
    return rs[0];
  }

  /**
   * Executa comandos (INSERT/UPDATE/DELETE/DDL).
   * Retorna linha(s) afetadas quando disponível.
   */
  async exec(
    text: string,
    params: MssqlQueryParams = {},
    opts: MssqlQueryOptions = {},
  ): Promise<{ rowsAffected: number[] }> {
    const pool = await this.getPool();
    const req = new sql.Request(pool);
    req.timeout = opts.timeout ?? 60_000;

    for (const [name, raw] of Object.entries(params)) {
      if (raw && typeof raw === 'object' && 'value' in raw) {
        const r = raw as { type?: sql.ISqlTypeFactory; value: any };
        if (r.type) req.input(name, r.type, r.value);
        else req.input(name, r.value);
      } else {
        req.input(name, raw as any);
      }
    }

    try {
      const res = await req.batch(text);
      return { rowsAffected: res.rowsAffected ?? [] };
    } catch (err: any) {
      this.logger.error(this.formatSqlError('exec', text, params, err));
      throw err;
    }
  }

  /**
   * Fecha a conexão em shutdown do Nest.
   */
  async onModuleDestroy() {
    await this.dispose();
  }

  async dispose() {
    if (this.pool) {
      try {
        await this.pool.close();
        this.logger.log('[MSSQL] Pool fechado.');
      } catch (e: any) {
        this.logger.error('[MSSQL] Erro ao fechar pool: ' + (e?.message || e));
      } finally {
        this.pool = undefined;
      }
    }
  }

  /**
   * Helper de log (mostra os primeiros 500 chars do SQL e parâmetros).
   */
  private formatSqlError(kind: 'query' | 'exec', text: string, params: MssqlQueryParams, err: any): string {
    const sqlPreview = text.replace(/\s+/g, ' ').trim().slice(0, 500);
    const p: Record<string, any> = {};
    for (const [k, v] of Object.entries(params || {})) {
      if (v && typeof v === 'object' && 'value' in v) p[k] = (v as any).value;
      else p[k] = v;
    }
    return `[MSSQL ${kind.toUpperCase()}] ${err?.message || err}\nSQL: ${sqlPreview}\nPARAMS: ${JSON.stringify(p)}`;
  }
}
