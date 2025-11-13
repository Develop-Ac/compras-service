/*
  Warnings:

  - You are about to drop the `com_usuario` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `com_usuarios` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "com_usuario";

-- DropTable
DROP TABLE "com_usuarios";

-- CreateTable
CREATE TABLE "sis_usuarios" (
    "usuario_id" SERIAL NOT NULL,
    "nome" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "senha" TEXT NOT NULL,
    "trash" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "sis_usuarios_pkey" PRIMARY KEY ("usuario_id")
);

-- CreateIndex
CREATE UNIQUE INDEX "sis_usuarios_email_key" ON "sis_usuarios"("email");
