// src/common/middlewares/app-token.middleware.ts
import { Injectable, NestMiddleware } from '@nestjs/common';
import type { FastifyReply, FastifyRequest } from 'fastify';

const APP_TOKEN = process.env.APP_TOKEN || '';

/**
 * ATENÇÃO (migração Express -> Fastify): esta classe hoje NÃO está registrada
 * em nenhum módulo. Se for ativada, registre-a como hook `preHandler` do
 * Fastify (`app.getHttpAdapter().getInstance().addHook('preHandler', ...)`) ou
 * como Guard — o `MiddlewareConsumer` do Nest, sob o Fastify, roda via middie e
 * entrega os objetos crus do Node (sem `query`/`body` já parseados).
 */
@Injectable()
export class AppTokenMiddleware implements NestMiddleware {
  use(req: FastifyRequest, res: FastifyReply, next: (err?: Error) => void) {
    if (req.method === 'OPTIONS') return res.status(204).send();

    // whitelist do Swagger
    const path = req.url.split('?')[0] || '';
    if (
      path.startsWith('/docs') ||     // UI e assets
      path.startsWith('/docs-json') ||// JSON
      path.startsWith('/health')      // healthcheck, se tiver
    ) {
      return next();
    }

    const body = req.body as Record<string, unknown> | undefined;
    const query = req.query as Record<string, unknown> | undefined;

    const tokenFromBody = (body && (body.token as string)) || '';
    const tokenFromQuery = (query?.token as string) || '';
    const token = tokenFromBody || tokenFromQuery;

    if (!token) {
      return res.status(401).send({ error: 'TOKEN_MISSING', message: 'Token é obrigatório.' });
    }
    if (token !== APP_TOKEN) {
      return res.status(403).send({ error: 'TOKEN_INVALID', message: 'Token inválido.' });
    }

    if (body && 'token' in body) delete (body as any).token;
    next();
  }
}
