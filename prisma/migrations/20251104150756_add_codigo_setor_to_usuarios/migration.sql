/*
  Warnings:

  - You are about to drop the column `email` on the `sis_usuarios` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[codigo]` on the table `sis_usuarios` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `codigo` to the `sis_usuarios` table without a default value. This is not possible if the table is not empty.
  - Added the required column `setor` to the `sis_usuarios` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX "sis_usuarios_email_key";

-- AlterTable: Add columns with default values first
ALTER TABLE "sis_usuarios" 
DROP COLUMN "email",
ADD COLUMN "codigo" TEXT NOT NULL DEFAULT 'TEMP001',
ADD COLUMN "setor" TEXT NOT NULL DEFAULT 'GERAL';

-- Update existing records with unique codes based on their id
UPDATE "sis_usuarios" SET 
  "codigo" = 'USR' || substr(id, 1, 8),
  "setor" = 'TI'
WHERE "codigo" = 'TEMP001';

-- Remove default values
ALTER TABLE "sis_usuarios" ALTER COLUMN "codigo" DROP DEFAULT;
ALTER TABLE "sis_usuarios" ALTER COLUMN "setor" DROP DEFAULT;

-- CreateIndex
CREATE UNIQUE INDEX "sis_usuarios_codigo_key" ON "sis_usuarios"("codigo");
