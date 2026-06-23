/**
 * Dispara, a partir desta máquina, a varredura de AUTO-VÍNCULO em todos os
 * pedidos elegíveis (status 'Aguardando analise' | 'Liberado' |
 * 'Faturado parcialmente' e SEM vínculo confirmado), criando SUGESTÕES
 * (confirmado=false) para validação manual. Não confirma nada automaticamente.
 *
 * Usa o código ATUAL (dist recém-buildado): geração escopada por fornecedor e
 * status 'Liberado'. Conecta no Postgres (DATABASE_URL) e no MSSQL/BI (defaults
 * 192.168.1.146) de produção.
 *
 * Uso:  node scripts/rodar-autovinculo-tudo.js
 *       AUTOVINCULO_LIMITE=500 node scripts/rodar-autovinculo-tudo.js   # lote maior
 */

// Desliga o cron interno (não queremos que dispare em paralelo durante o script)
// e amplia o limite por rodada. Precisa ser ANTES de carregar o AppModule.
process.env.AUTOVINCULO_CRON = '0 0 1 1 *'; // 1x/ano (1º jan) — não dispara durante o script
process.env.AUTOVINCULO_LIMITE = process.env.AUTOVINCULO_LIMITE || '1000';

const { NestFactory } = require('@nestjs/core');
const { AppModule } = require('../dist/app.module');
const { AutoVinculoService } = require('../dist/compras/vicunlacao-nfe/auto-vinculo.service');

(async () => {
  const inicio = Date.now();
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: ['error', 'warn', 'log'],
  });
  try {
    const svc = app.get(AutoVinculoService);
    console.log('Iniciando varredura de auto-vínculo...');
    const res = await svc.executarVarredura();
    const seg = ((Date.now() - inicio) / 1000).toFixed(1);
    console.log('\n==== RESULTADO ====');
    console.log(JSON.stringify(res, null, 2));
    console.log(`Tempo: ${seg}s`);
    if (res.truncado) {
      console.log(
        `\nATENÇÃO: havia mais pedidos que o limite (${res.limite}). Rode de novo ` +
          `(os que viraram "Vínculo sugerido" saem do scan) ou use AUTOVINCULO_LIMITE maior.`,
      );
    }
  } finally {
    await app.close();
  }
})().catch((e) => {
  console.error('FALHA:', e?.message || e);
  process.exit(1);
});
