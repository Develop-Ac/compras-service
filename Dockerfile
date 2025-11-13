# ============
# Build stage
# ============
FROM node:20-alpine AS build
WORKDIR /app

# Toolchain + headers para node-canvas
RUN apk add --no-cache \
  openssl \
  python3 make g++ pkgconf \
  cairo-dev pango-dev jpeg-dev giflib-dev librsvg-dev pixman-dev

# >>> Defina o Python para o node-gyp via env (npm 10 não aceita 'npm config set python')
ENV PYTHON=/usr/bin/python3
ENV npm_config_python=/usr/bin/python3
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Instala deps antes do código (cache melhor)
COPY package*.json ./
RUN npm ci

# Código e configs
COPY prisma ./prisma
COPY tsconfig*.json nest-cli.json* ./
COPY src ./src

# Prisma + build
RUN npx prisma generate
RUN npm run build

# ==============
# Runtime stage
# ==============
FROM node:20-alpine AS runtime
WORKDIR /app

# Somente libs de execução (sem -dev)
RUN apk add --no-cache \
  openssl \
  cairo pango jpeg giflib librsvg pixman

ENV NODE_ENV=production
ENV PORT=8000

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/prisma ./prisma
COPY --from=build /app/dist ./dist

EXPOSE 8000
CMD ["sh","-c","npx prisma migrate deploy || true; node dist/main.js"]
