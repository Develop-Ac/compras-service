/*
  Warnings:

  - The primary key for the `sis_usuarios` table will be changed. If it partially fails, the table could be left without primary key constraint.

*/
-- AlterTable
ALTER TABLE "sis_usuarios" DROP CONSTRAINT "sis_usuarios_pkey",
ALTER COLUMN "id" DROP DEFAULT,
ALTER COLUMN "id" SET DATA TYPE TEXT,
ADD CONSTRAINT "sis_usuarios_pkey" PRIMARY KEY ("id");
DROP SEQUENCE "sis_usuarios_id_seq";
