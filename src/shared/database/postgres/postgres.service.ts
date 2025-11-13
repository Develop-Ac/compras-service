import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';

@Injectable()
export class PostgresService implements OnModuleDestroy {
  private pool: Pool;

  constructor() {
    const sslMode = process.env.DB_SSLMODE || 'disable';
    
    this.pool = new Pool({
      user: process.env.DB_USER || 'logistica',
      password: process.env.DB_PASSWORD || 'Ac@2025acesso',
      host: process.env.DB_HOST || 'atendimento.acacessorios.com.br',
      port: parseInt(process.env.DB_PORT || '5523'),
      database: process.env.DB_NAME || 'logistica',
      ssl: sslMode === 'disable' ? false : undefined,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  async query(text: string, params?: any[]): Promise<any> {
    const client: PoolClient = await this.pool.connect();
    try {
      const result = await client.query(text, params);
      return result.rows;
    } finally {
      client.release();
    }
  }

  async onModuleDestroy() {
    await this.pool.end();
  }
}