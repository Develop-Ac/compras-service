import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import * as sql from 'mssql';

/**
 * Conexão de LEITURA ao SQL Server BI (DW). Read-only.
 * Variáveis de ambiente:
 *   BI_SQL_SERVER, BI_SQL_DATABASE, BI_SQL_USER, BI_SQL_PASSWORD, BI_SQL_PORT (opcional, default 1433)
 * Aceita também os nomes antigos MSSQL_HOST/MSSQL_DATABASE/MSSQL_USER/MSSQL_PASSWORD/MSSQL_PORT
 * como fallback, para ambientes que ainda não migraram o .env.
 */
@Injectable()
export class MssqlService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MssqlService.name);
  private pool: sql.ConnectionPool | null = null;
  private connecting: Promise<sql.ConnectionPool> | null = null;

  private buildConfig(): sql.config {
    return {
      server: process.env.BI_SQL_SERVER ?? process.env.MSSQL_HOST ?? '192.168.1.146',
      database: process.env.BI_SQL_DATABASE ?? process.env.MSSQL_DATABASE ?? 'BI',
      user: process.env.BI_SQL_USER ?? process.env.MSSQL_USER ?? '',
      password: process.env.BI_SQL_PASSWORD ?? process.env.MSSQL_PASSWORD ?? '',
      port: parseInt(process.env.BI_SQL_PORT ?? process.env.MSSQL_PORT ?? '1433', 10),
      options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true,
      },
      pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
      requestTimeout: 120000,
      connectionTimeout: 15000,
    };
  }

  async onModuleInit() {
    // Conexão preguiçosa — não derruba o boot se o BI estiver indisponível.
    try {
      await this.getPool();
    } catch (err) {
      this.logger.warn(
        `BI (SQL Server) indisponível no boot: ${(err as Error).message}. Tentará reconectar sob demanda.`,
      );
    }
  }

  async onModuleDestroy() {
    if (this.pool) {
      await this.pool.close().catch(() => undefined);
      this.pool = null;
    }
  }

  private async getPool(): Promise<sql.ConnectionPool> {
    if (this.pool?.connected) return this.pool;
    if (this.connecting) return this.connecting;

    this.connecting = (async () => {
      const pool = new sql.ConnectionPool(this.buildConfig());
      pool.on('error', (e) => this.logger.error(`Pool BI erro: ${e.message}`));
      await pool.connect();
      this.pool = pool;
      this.logger.log('Conectado ao SQL Server BI (read-only).');
      return pool;
    })();

    try {
      return await this.connecting;
    } finally {
      this.connecting = null;
    }
  }

  /**
   * Executa uma query parametrizada. params = { nome: valor }.
   * Tipos inferidos pelo driver mssql.
   */
  async query<T = any>(
    text: string,
    params: Record<string, unknown> = {},
  ): Promise<T[]> {
    const pool = await this.getPool();
    const request = pool.request();
    for (const [key, value] of Object.entries(params)) {
      request.input(key, value as never);
    }
    const result = await request.query<T>(text);
    return result.recordset ?? [];
  }
}
