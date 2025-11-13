// src/common/middlewares/app-token.middleware.ts
import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';

const APP_TOKEN = process.env.APP_TOKEN || '';

@Injectable()
export class AppTokenMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    if (req.method === 'OPTIONS') return res.sendStatus(204);

    // whitelist do Swagger
    const path = req.path || '';
    if (
      path.startsWith('/docs') ||     // UI e assets
      path.startsWith('/docs-json') ||// JSON
      path.startsWith('/health')      // healthcheck, se tiver
    ) {
      return next();
    }

    const tokenFromBody = (req.body && (req.body.token as string)) || '';
    const tokenFromQuery = (req.query?.token as string) || '';
    const token = tokenFromBody || tokenFromQuery;

    if (!token) {
      return res.status(401).json({ error: 'TOKEN_MISSING', message: 'Token é obrigatório.' });
    }
    if (token !== APP_TOKEN) {
      return res.status(403).json({ error: 'TOKEN_INVALID', message: 'Token inválido.' });
    }

    if (req.body && 'token' in req.body) delete (req.body as any).token;
    next();
  }
}
