/*
  Warnings:

  - The primary key for the `sis_usuarios` table will be changed. If it partially fails, the table could be left without primary key constraint.
  - You are about to drop the column `usuario_id` on the `sis_usuarios` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "sis_usuarios" DROP CONSTRAINT "sis_usuarios_pkey",
DROP COLUMN "usuario_id",
ADD COLUMN     "id" SERIAL NOT NULL,
ADD CONSTRAINT "sis_usuarios_pkey" PRIMARY KEY ("id");
