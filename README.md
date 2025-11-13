# Intranet AC Acessórios - Backend API

> **API NestJS para sistema de intranet da AC Acessórios com gestão de cotações, estoque, oficina e usuários**

[![Node.js](https://img.shields.io/badge/Node.js-20+-green.svg)](https://nodejs.org/)
[![NestJS](https://img.shields.io/badge/NestJS-10+-red.svg)](https://nestjs.com/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5+-blue.svg)](https://www.typescriptlang.org/)
[![Prisma](https://img.shields.io/badge/Prisma-5+-purple.svg)](https://www.prisma.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)

## 📋 Índice

- [Sobre o Projeto](#sobre-o-projeto)
- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Executando o Projeto](#executando-o-projeto)
- [Documentação da API](#documentação-da-api)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Módulos Principais](#módulos-principais)
- [Database](#database)
- [Deploy](#deploy)
- [Contribuição](#contribuição)
- [Licença](#licença)

## 🎯 Sobre o Projeto

Este é o backend da aplicação de intranet da **AC Acessórios**, uma API REST desenvolvida em **NestJS** que fornece funcionalidades para:

- **Gestão de Cotações**: Sistema completo de cotações com fornecedores
- **Controle de Estoque**: Contagem e monitoramento de estoque
- **Gestão de Oficina**: Checklists e ordens de serviço
- **Autenticação**: Sistema de login e controle de usuários
- **Integração ERP**: Conexão com sistema legado via OpenQuery

## ✨ Funcionalidades

### 🛒 Módulo de Compras
- **Cotações**
  - Criação e gestão de cotações
  - Sincronização com fornecedores
  - Histórico de cotações
  - Comparativo de preços

- **Fornecedores**
  - Cadastro e gestão de fornecedores
  - Histórico de transações
  - Avaliação de fornecedores

- **Pedidos**
  - Gestão de pedidos de compra
  - Acompanhamento de status
  - Integração com estoque

### 📦 Módulo de Estoque
- **Contagem de Estoque**
  - Sistema de contagem sequencial (1ª, 2ª, 3ª contagem)
  - Liberação progressiva de contagens
  - Controle de conferência por item
  - Agrupamento de contagens por CUID
  - Consulta de estoque via OpenQuery

### 🔧 Módulo de Oficina
- **Checklists**
  - Criação de checklists personalizados
  - Gestão de itens e avarias
  - Upload de imagens
  - Geração de PDFs

- **Ordem de Serviço**
  - Controle de ordens de serviço
  - Acompanhamento de status
  - Histórico de serviços

### 👥 Módulo de Usuários
- Sistema de autenticação
- Controle de permissões
- Gestão de perfis de usuário
- Integração com setores

### 🛠️ Utilitários
- Upload de arquivos (AWS S3)
- Geração de relatórios
- Consultas customizadas
- Integração com ERP via OpenQuery

## 🏗️ Arquitetura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Backend API   │    │   Database      │
│   (React/Vue)   │◄──►│   (NestJS)      │◄──►│   (PostgreSQL)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   ERP Legacy    │
                       │   (Firebird)    │
                       └─────────────────┘
```

### Padrões Arquiteturais
- **Clean Architecture**: Separação clara entre camadas
- **Domain-Driven Design**: Organização por domínios de negócio
- **Repository Pattern**: Abstração de acesso a dados
- **Dependency Injection**: Inversão de controle e testabilidade
- **OpenQuery Integration**: Ponte com sistema legado

## 🚀 Tecnologias Utilizadas

### Core
- **[NestJS](https://nestjs.com/)** - Framework Node.js progressivo
- **[TypeScript](https://www.typescriptlang.org/)** - JavaScript tipado
- **[Prisma](https://www.prisma.io/)** - ORM moderno para TypeScript
- **[PostgreSQL](https://www.postgresql.org/)** - Banco de dados principal

### Integração e APIs
- **[Swagger/OpenAPI](https://swagger.io/)** - Documentação automática da API
- **[Axios](https://axios-http.com/)** - Cliente HTTP
- **[OpenQuery](https://docs.microsoft.com/en-us/sql/t-sql/functions/openquery-transact-sql)** - Integração com ERP legado

### Utilitários
- **[AWS SDK](https://aws.amazon.com/sdk-for-javascript/)** - Integração com AWS S3
- **[PDFKit](https://pdfkit.org/)** - Geração de PDFs
- **[Sharp](https://sharp.pixelplumbing.com/)** - Processamento de imagens
- **[Helmet.js](https://helmetjs.github.io/)** - Segurança HTTP
- **[bcryptjs](https://github.com/dcodeIO/bcrypt.js)** - Hash de senhas

### Desenvolvimento
- **[Jest](https://jestjs.io/)** - Testes unitários e E2E
- **[ESLint](https://eslint.org/)** - Linting
- **[Prettier](https://prettier.io/)** - Formatação de código

## 📋 Pré-requisitos

- **Node.js** 20+ 
- **npm** ou **yarn**
- **PostgreSQL** 15+
- **Git**
- **AWS Account** (para S3, opcional)
- **Acesso ao ERP legado** (para OpenQuery)

## 🔧 Instalação

### 1. Clone o repositório
```bash
git clone https://github.com/Develop-Ac/cotacao-backend.git
cd cotacao-backend
```

### 2. Instale as dependências
```bash
npm install
```

### 3. Configure o banco de dados
```bash
# Execute as migrações
npx prisma migrate deploy

# Gere o cliente Prisma
npx prisma generate
```

## ⚙️ Configuração

### Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
# Database
DATABASE_URL="postgresql://usuario:senha@localhost:5432/intranet_ac"

# Server
PORT=3000
NODE_ENV=development

# CORS
CORS_ORIGIN="http://localhost:3000,http://localhost:5173"

# Swagger
SWAGGER_ENABLED=true

# AWS S3 (opcional)
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
AWS_REGION=us-east-1
AWS_BUCKET_NAME=seu_bucket

# OpenQuery / ERP Integration
ERP_CONNECTION_STRING="Server=servidor;Database=database;User=user;Password=pass;"

# Security
JWT_SECRET=seu_jwt_secret_muito_seguro
BCRYPT_ROUNDS=12

# Application Token Middleware
APP_TOKEN=seu_token_de_aplicacao
```

### Configuração do PostgreSQL

```sql
-- Criar banco de dados
CREATE DATABASE intranet_ac;

-- Criar usuário (opcional)
CREATE USER api_user WITH PASSWORD 'senha_segura';
GRANT ALL PRIVILEGES ON DATABASE intranet_ac TO api_user;
```

## 🚀 Executando o Projeto

### Desenvolvimento
```bash
# Modo watch (recarrega automaticamente)
npm run dev

# Ou usando o comando do NestJS
npx nest start --watch
```

### Produção
```bash
# Build do projeto
npm run build

# Executar em produção
npm run start:prod

# Com migração automática
npm run start:migrate
```

### Testes
```bash
# Testes unitários
npm run test

# Testes E2E
npm run test:e2e

# Coverage
npm run test:cov
```

## 📚 Documentação da API

Após executar o projeto, acesse:

- **Swagger UI**: `http://localhost:3000/api-docs`
- **OpenAPI JSON**: `http://localhost:3000/api-docs-json`

### Principais Endpoints

#### Autenticação
```http
POST /login - Realizar login
```

#### Estoque
```http
GET    /contagem/:id_usuario - Listar contagens por usuário
GET    /contagem/grupo/:contagem_cuid - Listar contagens por grupo
POST   /contagem - Criar nova contagem
PUT    /contagem/liberar - Liberar próxima contagem
PUT    /contagem/item/:id - Atualizar conferência de item
GET    /contagem/conferir/:cod_produto - Consultar estoque via ERP
```

#### Cotações
```http
GET    /cotacao - Listar cotações
POST   /cotacao - Criar cotação
PUT    /cotacao/:id - Atualizar cotação
DELETE /cotacao/:id - Remover cotação
```

#### Fornecedores
```http
GET    /fornecedor - Listar fornecedores
POST   /fornecedor - Criar fornecedor
PUT    /fornecedor/:id - Atualizar fornecedor
```

## 📁 Estrutura do Projeto

```
src/
├── app.module.ts              # Módulo raiz da aplicação
├── main.ts                    # Ponto de entrada da aplicação
│
├── common/                    # Componentes compartilhados
│   └── middlewares/
│       └── app-token.middleware.ts
│
├── compras/                   # Módulo de compras
│   ├── cotacao/              # Gestão de cotações
│   │   ├── cotacao-sync/     # Sincronização
│   │   ├── fornecedor/       # Fornecedores
│   │   ├── openquery/        # Integração ERP
│   │   └── pedido/           # Pedidos
│
├── estoque/                   # Módulo de estoque
│   └── contagem/             # Contagem de estoque
│       ├── dto/              # Data Transfer Objects
│       ├── contagem.controller.ts
│       ├── contagem.service.ts
│       ├── contagem.repository.ts
│       └── contagem.types.ts
│
├── login/                     # Autenticação
├── oficina/                   # Módulo de oficina
│   ├── checkList/            # Checklists
│   └── s3/                   # Upload de arquivos
│
├── prisma/                    # Configuração Prisma
├── shared/                    # Recursos compartilhados
│   └── database/
│       └── openquery/        # Serviço OpenQuery
│
├── storage/                   # Gestão de arquivos
├── usuario/                   # Gestão de usuários
└── utils/                     # Utilitários gerais
```

## 📊 Módulos Principais

### Estoque - Contagem
Sistema de contagem de estoque com controle sequencial:

- **1ª Contagem**: Liberada automaticamente na criação
- **2ª Contagem**: Liberada após conclusão da 1ª
- **3ª Contagem**: Liberada após conclusão da 2ª
- **Itens Compartilhados**: Mesmos itens para todas as contagens do grupo
- **Conferência Individual**: Controle de conferência por item

### Compras - Cotações
Gestão completa de cotações:

- **Criação de Cotações**: Com múltiplos itens e fornecedores
- **Sincronização**: Atualização automática de dados
- **Comparativo**: Análise de preços entre fornecedores
- **Histórico**: Rastreabilidade completa

### Oficina - Checklists
Sistema de checklists para oficina:

- **Modelos Flexíveis**: Checklists customizáveis
- **Captura de Imagens**: Upload e gestão de fotos
- **Geração de PDFs**: Relatórios automáticos
- **Controle de Qualidade**: Verificação de itens

## 🗄️ Database

### Principais Tabelas

```sql
-- Usuários do sistema
sis_usuarios (
  id: String (CUID)
  nome: String
  codigo: String (unique)
  setor: String
  senha: String (hash)
  trash: Int (soft delete)
)

-- Contagens de estoque
est_contagem (
  id: String (CUID)
  colaborador: String (FK)
  contagem: Int (1, 2, 3)
  contagem_cuid: String (agrupamento)
  liberado_contagem: Boolean
  created_at: DateTime
)

-- Itens das contagens
est_contagem_itens (
  id: String (CUID)
  contagem_cuid: String (agrupamento)
  cod_produto: Int
  desc_produto: String
  estoque: Int
  conferir: Boolean
  -- outros campos...
)

-- Cotações
com_cotacao (
  id: String (CUID)
  empresa: Int
  pedido_cotacao: Int (unique)
)
```

### Migrações

```bash
# Criar nova migração
npx prisma migrate dev --name nome_da_migracao

# Aplicar migrações pendentes
npx prisma migrate deploy

# Reset do banco (desenvolvimento)
npx prisma migrate reset

# Status das migrações
npx prisma migrate status
```

## 🌐 Deploy

### Docker

```dockerfile
# Dockerfile
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

EXPOSE 3000

CMD ["npm", "run", "start:migrate"]
```

### Docker Compose

```yaml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "3000:3000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/intranet_ac
      - NODE_ENV=production
    depends_on:
      - db

  db:
    image: postgres:15
    environment:
      - POSTGRES_DB=intranet_ac
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

### Deploy na Nuvem

#### Railway
1. Conecte seu repositório GitHub
2. Configure as variáveis de ambiente
3. Railway detectará automaticamente o `nixpacks.toml`

#### Vercel
```json
{
  "version": 2,
  "builds": [
    {
      "src": "dist/main.js",
      "use": "@vercel/node"
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "dist/main.js"
    }
  ]
}
```

## 🔒 Segurança

### Medidas Implementadas

- **Helmet.js**: Headers de segurança HTTP
- **CORS**: Controle de origem de requisições
- **Rate Limiting**: Proteção contra spam
- **Input Validation**: Validação com class-validator
- **SQL Injection Protection**: Prisma ORM
- **Password Hashing**: bcryptjs
- **Environment Variables**: Configurações sensíveis

### Middleware de Token

```typescript
// Middleware para autenticação por token de aplicação
@Injectable()
export class AppTokenMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const token = req.headers['app-token'];
    if (token !== process.env.APP_TOKEN) {
      throw new UnauthorizedException('Token inválido');
    }
    next();
  }
}
```

## 🧪 Testes

### Estrutura de Testes

```
test/
├── app.e2e-spec.ts           # Testes E2E principais
├── jest-e2e.json             # Configuração Jest E2E
└── modules/
    ├── contagem.e2e-spec.ts  # Testes E2E contagem
    ├── cotacao.e2e-spec.ts   # Testes E2E cotação
    └── auth.e2e-spec.ts      # Testes E2E autenticação
```

### Executando Testes

```bash
# Todos os testes
npm test

# Testes específicos
npm test -- contagem

# Modo watch
npm test -- --watch

# Coverage
npm run test:cov
```

## 📈 Monitoramento e Logs

### Logs Estruturados

```typescript
import { Logger } from '@nestjs/common';

@Injectable()
export class ContagemService {
  private readonly logger = new Logger(ContagemService.name);

  async createContagem(dto: CreateContagemDto) {
    this.logger.log(`Criando contagem para usuário: ${dto.colaborador}`);
    // ... lógica
  }
}
```

### Health Check

```typescript
@Get('health')
@ApiOperation({ summary: 'Health check' })
async healthCheck() {
  return {
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
  };
}
```

## 🤝 Contribuição

### Workflow de Contribuição

1. **Fork** o projeto
2. **Clone** sua fork
3. **Crie** uma branch para sua feature (`git checkout -b feature/nova-funcionalidade`)
4. **Commit** suas mudanças (`git commit -m 'feat: adiciona nova funcionalidade'`)
5. **Push** para a branch (`git push origin feature/nova-funcionalidade`)
6. **Abra** um Pull Request

### Padrões de Commit

```
feat: nova funcionalidade
fix: correção de bug
docs: documentação
style: formatação
refactor: refatoração
test: testes
chore: manutenção
```

### Code Review

- Código deve seguir os padrões ESLint
- Testes devem cobrir no mínimo 80%
- Documentação deve ser atualizada
- Endpoints devem ter documentação Swagger

## 📄 Licença

Este projeto está sob a licença **MIT**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

## 🚀 Quick Start

```bash
# Clone e configure
git clone https://github.com/Develop-Ac/cotacao-backend.git
cd cotacao-backend
npm install

# Configure o .env
cp .env.example .env
# Edite as variáveis necessárias

# Execute as migrações
npx prisma migrate dev

# Inicie o servidor
npm run dev
```

**API rodando em**: `http://localhost:3000`  
**Documentação**: `http://localhost:3000/api-docs`

---

<div align="center">

**[⬆ Voltar ao topo](#intranet-ac-acessórios---backend-api)**

Desenvolvido com ❤️ pela equipe **AC Acessórios**

</div>