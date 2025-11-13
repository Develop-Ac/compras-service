// src/main.ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import helmet from 'helmet';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import * as bodyParser from 'body-parser';
// (opcional, mas recomendado quando usa cookies/autenticação)
import cookieParser from 'cookie-parser';

function parseOrigins(env?: string): (string | RegExp)[] {
  if (!env) return [];
  return env
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(s => {
      // Permite regex usando prefixo "regex:"
      if (s.startsWith('regex:')) {
        const pattern = s.slice(6);
        return new RegExp(pattern);
      }
      return s;
    });
}

function isAllowedOrigin(origin: string | undefined, allowed: (string | RegExp)[]) {
  if (!origin) return true; // requests server-to-server, curl, etc.
  if (allowed.length === 0) return true; // se não configurou nada, libera
  for (const rule of allowed) {
    if (rule instanceof RegExp && rule.test(origin)) return true;
    if (typeof rule === 'string' && rule === origin) return true;
  }
  return false;
}

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });

  // Se estiver atrás de proxy reverso (Nginx/Traefik) e usar cookies Secure, habilite:
  // app.set('trust proxy', 1);

  app.use(cookieParser());

  app.use(
    helmet({
      contentSecurityPolicy: false,     // necessário para swagger-ui
      crossOriginEmbedderPolicy: false, // evita bloqueio de assets
    }),
  );

  const allowedOrigins = parseOrigins(process.env.CORS_ORIGIN);
  // Ex.: CORS_ORIGIN="http://intranet.acacessorios.local,http://localhost:3000"
  // ou   CORS_ORIGIN="regex:^https?://(localhost:\d+|.*\.acacessorios\.local)$"

  app.enableCors({
    origin: (origin, callback) => {
      const ok = isAllowedOrigin(origin, allowedOrigins);
      callback(null, ok);
    },
    credentials: true, // necessário se usar cookies/autenticação
    methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS'],
    allowedHeaders: [
      'Content-Type',
      'Authorization',
      'X-Requested-With',
      'Accept',
      'Origin',
      'Cache-Control',
      'Pragma',
    ],
    exposedHeaders: ['Content-Disposition'],
    maxAge: 86400, // cache do preflight por 1 dia
  });

  // Garante Vary: Origin (útil se usar origin dinâmico/função)
  app.use((req, res, next) => {
    res.setHeader('Vary', 'Origin');
    next();
  });

  app.use(bodyParser.json({ limit: '25mb' }));
  app.use(bodyParser.urlencoded({ limit: '25mb', extended: true }));

  // (opcional) prefixo global
  // app.setGlobalPrefix('api');

  // === Swagger only if enabled ===
  if (process.env.SWAGGER_ENABLED === 'true') {
    const config = new DocumentBuilder()
      .setTitle('Intranet AC Acessórios - Compras API')
          .setDescription(`
          API do microserviço de Compras da intranet AC Acessórios.

          ## Funcionalidades:
          - **Cotações**: Gerenciamento de solicitações e respostas de cotação
          - **Pedidos**: Criação e acompanhamento de pedidos de compra
          - **Fornecedores**: Cadastro e consulta de fornecedores
          - **Kanban**: Visualização e gerenciamento do fluxo de compras em formato Kanban

          ## Autenticação:
          A API utiliza tokens de acesso que podem ser enviados via query parameter \`token\` ou header \`Authorization: Bearer <token>\`.
      `)
      .setVersion('2.0.0')
      .setContact('AC Acessórios - TI', 'https://acacessorios.com.br', 'ti@acacessorios.com.br')
      .setLicense('Proprietário', '')
      .addBearerAuth(
      {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
        description: 'Token JWT para autenticação',
      },
      'jwt',
      )
      .addApiKey(
      {
        type: 'apiKey',
        name: 'token',
        in: 'query',
        description: 'TOKEN de acesso da aplicação enviado via query parameter',
      },
      'appToken',
      )
      .addServer(process.env.PUBLIC_URL ?? 'http://localhost:8000', 'Servidor de Desenvolvimento')
      .addServer('http://compras-service.acacessorios.local', 'Servidor de Produção')
      .build();

    const document = SwaggerModule.createDocument(app, config, {
      operationIdFactory: (controllerKey: string, methodKey: string) => methodKey,
      deepScanRoutes: true,
    });

    SwaggerModule.setup('docs', app, document, {
      swaggerOptions: {
        persistAuthorization: true,
        displayRequestDuration: true,
        docExpansion: 'none',
        filter: true,
        showRequestHeaders: true,
        tryItOutEnabled: true,
      },
      customSiteTitle: 'Intranet AC Acessórios — API Documentation',
      customfavIcon: '/favicon.ico',
      customJs: [
        'https://unpkg.com/swagger-ui-themes@3.0.1/themes/3.x/theme-material.css',
      ],
      customCssUrl: [
        'https://unpkg.com/swagger-ui-themes@3.0.1/themes/3.x/theme-material.css',
      ],
    });
    // UI: /docs • JSON: /docs-json
  }

  const port = parseInt(process.env.PORT || '8000', 10);
  await app.listen(port, '0.0.0.0');
  console.log(`API listening on http://localhost:${port}`);
  if (process.env.SWAGGER_ENABLED === 'true') {
    console.log(`Swagger em http://localhost:${port}/docs`);
  }
}
bootstrap();
