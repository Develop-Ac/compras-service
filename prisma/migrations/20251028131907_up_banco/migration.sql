-- CreateTable
CREATE TABLE "com_usuario" (
    "id" TEXT NOT NULL,
    "nome" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "senha" TEXT NOT NULL,
    "trash" INTEGER NOT NULL,

    CONSTRAINT "com_usuario_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_cotacao" (
    "id" TEXT NOT NULL,
    "empresa" INTEGER NOT NULL,
    "pedido_cotacao" INTEGER NOT NULL,

    CONSTRAINT "com_cotacao_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_cotacao_itens" (
    "pedido_cotacao" INTEGER NOT NULL,
    "emissao" TIMESTAMP(3),
    "pro_codigo" INTEGER NOT NULL,
    "pro_descricao" TEXT NOT NULL,
    "mar_descricao" TEXT,
    "referencia" TEXT,
    "unidade" TEXT,
    "quantidade" INTEGER NOT NULL,

    CONSTRAINT "com_cotacao_itens_pkey" PRIMARY KEY ("pedido_cotacao","pro_codigo")
);

-- CreateTable
CREATE TABLE "com_usuarios" (
    "usuario_id" SERIAL NOT NULL,
    "nome" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "senha" TEXT NOT NULL,
    "trash" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "com_usuarios_pkey" PRIMARY KEY ("usuario_id")
);

-- CreateTable
CREATE TABLE "ofi_checklists" (
    "id" TEXT NOT NULL,
    "osInterna" TEXT,
    "dataHoraEntrada" TIMESTAMP(3),
    "observacoes" TEXT,
    "combustivelPercentual" INTEGER,
    "clienteNome" TEXT,
    "clienteDoc" TEXT,
    "clienteTel" TEXT,
    "clienteEnd" TEXT,
    "veiculoNome" TEXT,
    "veiculoPlaca" TEXT,
    "veiculoCor" TEXT,
    "veiculoKm" BIGINT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "assinaturasclienteBase64" TEXT,
    "assinaturasresponsavelBase64" TEXT,

    CONSTRAINT "ofi_checklists_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ofi_checklists_avarias" (
    "id" TEXT NOT NULL,
    "checklistId" TEXT NOT NULL,
    "tipo" TEXT,
    "peca" TEXT,
    "observacoes" TEXT,
    "posX" DOUBLE PRECISION,
    "posY" DOUBLE PRECISION,
    "posZ" DOUBLE PRECISION,
    "normX" DOUBLE PRECISION,
    "normY" DOUBLE PRECISION,
    "normZ" DOUBLE PRECISION,
    "fotoBase64" TEXT,
    "timestamp" TIMESTAMP(3),

    CONSTRAINT "ofi_checklists_avarias_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ofi_checklists_items" (
    "id" TEXT NOT NULL,
    "checklistId" TEXT NOT NULL,
    "item" TEXT NOT NULL,
    "status" TEXT NOT NULL,

    CONSTRAINT "ofi_checklists_items_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_cotacao_for" (
    "id" TEXT NOT NULL,
    "empresa" INTEGER NOT NULL DEFAULT 3,
    "pedido_cotacao" INTEGER NOT NULL,
    "for_codigo" INTEGER NOT NULL,
    "for_nome" TEXT,
    "cpf_cnpj" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "com_cotacao_for_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_cotacao_itens_for" (
    "id" TEXT NOT NULL,
    "pedido_cotacao" INTEGER NOT NULL,
    "for_codigo" INTEGER NOT NULL,
    "emissao" TIMESTAMP(3),
    "pro_codigo" INTEGER NOT NULL,
    "pro_descricao" TEXT NOT NULL,
    "mar_descricao" TEXT,
    "referencia" TEXT,
    "unidade" TEXT,
    "quantidade" INTEGER NOT NULL,
    "valor_unitario" DOUBLE PRECISION,

    CONSTRAINT "com_cotacao_itens_for_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_pedido" (
    "id" TEXT NOT NULL,
    "pedido_cotacao" INTEGER NOT NULL,
    "for_codigo" INTEGER NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "com_pedido_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "com_pedido_itens" (
    "id" TEXT NOT NULL,
    "pedido_id" TEXT NOT NULL,
    "item_id_origem" TEXT,
    "pro_codigo" INTEGER NOT NULL,
    "pro_descricao" TEXT NOT NULL,
    "mar_descricao" TEXT,
    "referencia" TEXT,
    "unidade" TEXT,
    "emissao" TIMESTAMP(3),
    "valor_unitario" DECIMAL(65,30),
    "custo_fabrica" DECIMAL(65,30),
    "preco_custo" DECIMAL(65,30),
    "for_codigo" INTEGER NOT NULL,
    "quantidade" DECIMAL(65,30) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "com_pedido_itens_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "com_usuario_email_key" ON "com_usuario"("email");

-- CreateIndex
CREATE UNIQUE INDEX "com_cotacao_pedido_cotacao_key" ON "com_cotacao"("pedido_cotacao");

-- CreateIndex
CREATE UNIQUE INDEX "com_usuarios_email_key" ON "com_usuarios"("email");

-- CreateIndex
CREATE INDEX "ofi_checklists_avarias_checklistId_idx" ON "ofi_checklists_avarias"("checklistId");

-- CreateIndex
CREATE INDEX "ofi_checklists_items_checklistId_idx" ON "ofi_checklists_items"("checklistId");

-- CreateIndex
CREATE INDEX "com_cotacao_for_pedido_cotacao_idx" ON "com_cotacao_for"("pedido_cotacao");

-- CreateIndex
CREATE UNIQUE INDEX "com_cotacao_for_pedido_cotacao_for_codigo_key" ON "com_cotacao_for"("pedido_cotacao", "for_codigo");

-- CreateIndex
CREATE INDEX "com_cotacao_itens_for_pedido_cotacao_for_codigo_idx" ON "com_cotacao_itens_for"("pedido_cotacao", "for_codigo");

-- CreateIndex
CREATE INDEX "com_pedido_pedido_cotacao_idx" ON "com_pedido"("pedido_cotacao");

-- CreateIndex
CREATE INDEX "com_pedido_for_codigo_idx" ON "com_pedido"("for_codigo");

-- CreateIndex
CREATE UNIQUE INDEX "com_pedido_pedido_cotacao_for_codigo_key" ON "com_pedido"("pedido_cotacao", "for_codigo");

-- CreateIndex
CREATE INDEX "com_pedido_itens_pedido_id_idx" ON "com_pedido_itens"("pedido_id");

-- CreateIndex
CREATE UNIQUE INDEX "com_pedido_itens_pedido_id_pro_codigo_for_codigo_key" ON "com_pedido_itens"("pedido_id", "pro_codigo", "for_codigo");

-- AddForeignKey
ALTER TABLE "com_cotacao_itens" ADD CONSTRAINT "com_cotacao_itens_pedido_cotacao_fkey" FOREIGN KEY ("pedido_cotacao") REFERENCES "com_cotacao"("pedido_cotacao") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ofi_checklists_avarias" ADD CONSTRAINT "ofi_checklists_avarias_checklistId_fkey" FOREIGN KEY ("checklistId") REFERENCES "ofi_checklists"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ofi_checklists_items" ADD CONSTRAINT "ofi_checklists_items_checklistId_fkey" FOREIGN KEY ("checklistId") REFERENCES "ofi_checklists"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "com_cotacao_itens_for" ADD CONSTRAINT "com_cotacao_itens_for_pedido_cotacao_for_codigo_fkey" FOREIGN KEY ("pedido_cotacao", "for_codigo") REFERENCES "com_cotacao_for"("pedido_cotacao", "for_codigo") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "com_pedido_itens" ADD CONSTRAINT "com_pedido_itens_pedido_id_fkey" FOREIGN KEY ("pedido_id") REFERENCES "com_pedido"("id") ON DELETE CASCADE ON UPDATE CASCADE;
