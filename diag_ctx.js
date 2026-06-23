process.env.AUTOVINCULO_CRON = '0 0 1 1 *';
const { NestFactory } = require('@nestjs/core');
const { AppModule } = require('./dist/app.module');
const { OpenQueryService } = require('./dist/shared/database/openquery/openquery.service');
(async () => {
  const app = await NestFactory.createApplicationContext(AppModule, { logger:['error'] });
  const oq = app.get(OpenQueryService);
  const run = async (label, fb) => {
    const tsql = `SELECT * FROM OPENQUERY([CONSULTA], '${fb.replace(/'/g,"''")}')`;
    try { const r = await oq.query(tsql, {}, { timeout:60000, allowZeroRows:true }); console.log(label, '->', JSON.stringify(r).slice(0,300)); }
    catch(e){ console.log(label, 'ERRO:', e.message); }
  };
  try {
    await run('FORNECEDORES 936 por empresa', `select fo.empresa, fo.for_nome, fo.cpf_cnpj from FORNECEDORES fo where fo.for_codigo = 936`);
    await run('FORNECEDORES count empresas', `select fo.empresa, count(*) qt from FORNECEDORES fo group by fo.empresa`);
    await run('colunas FORNECEDORES (1 linha)', `select first 1 * from FORNECEDORES`);
  } finally { await app.close(); }
})().catch(e=>{console.error('FALHA:', e?.message||e); process.exit(1);});
