import { Injectable } from '@nestjs/common';
import { NotaFiscalRepository } from './notaFiscal.repository';
import { PrismaService } from '../../../prisma/prisma.service';
import { FornecedorGrupoService } from '../../fornecedor-grupo/fornecedor-grupo.service';
import axios from 'axios';
import { writeFile, unlink } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
const FormData = require('form-data');
import { createReadStream } from 'fs';
import fs from 'node:fs';
import zlib from 'zlib';
import { promises as fsp } from 'node:fs';

@Injectable()
export class NotaFiscalService {
  constructor(
    private readonly notaFiscalRepository: NotaFiscalRepository,
    private readonly prisma: PrismaService,
    private readonly grupo: FornecedorGrupoService,
  ) {}

  async getNfeDistribuicao() {
    const data = await this.notaFiscalRepository.fetchNfeDistribuicao();
    return data;
  }

  async getNfeDisponiveis(pedidoId?: string, mostrarTodas = false) {
    const data = (await this.notaFiscalRepository.fetchNfeDisponiveis()) as Array<
      Record<string, any>
    >;

    // Enriquece com o valor total da NF a partir de com_nfe_conciliacao (Postgres),
    // cruzando pela chave de acesso. Notas sem registro de conciliação ficam sem valor.
    const chaves = data
      .map((r) => (r?.CHAVE_NFE == null ? null : String(r.CHAVE_NFE)))
      .filter((c): c is string => !!c);

    let valorPorChave = new Map<string, number>();
    if (chaves.length) {
      const conc = await this.prisma.com_nfe_conciliacao.findMany({
        where: { chave_nfe: { in: chaves } },
        select: { chave_nfe: true, valor_total: true },
      });
      valorPorChave = new Map(conc.map((c) => [c.chave_nfe, c.valor_total]));
    }

    for (const row of data) {
      const chave = row?.CHAVE_NFE == null ? null : String(row.CHAVE_NFE);
      row.VALOR_TOTAL_NF =
        chave != null && valorPorChave.has(chave) ? valorPorChave.get(chave) : null;
      row.STATUS_ERP = 'PENDENTE';
    }

    // Sem pedido de referência: mantém o comportamento antigo (apenas pendentes),
    // mas ainda escondendo as NF-e sem saldo.
    if (!pedidoId) {
      return this.removerChavesSemSaldo(data);
    }

    // Com pedido: inclui também as NF-e já LANCADA no ERP cuja emissão é
    // POSTERIOR à data do pedido (uma NF do pedido é emitida depois dele).
    const pedido = await this.prisma.com_pedido.findUnique({
      where: { id: pedidoId },
      select: { created_at: true, for_codigo: true },
    });
    const dataPedido = pedido?.created_at ?? null;

    const chavesPendentes = new Set(chaves);

    const lancadas = await this.prisma.com_nfe_conciliacao.findMany({
      where: {
        status_erp: 'LANCADA',
        ...(dataPedido ? { data_emissao: { gt: dataPedido } } : {}),
      },
      select: {
        chave_nfe: true,
        emitente: true,
        cnpj_emitente: true,
        data_emissao: true,
        valor_total: true,
        tipo_operacao_desc: true,
      },
      orderBy: { data_emissao: 'desc' },
      take: 2000,
    });

    const lancadasMapeadas = lancadas
      .filter((l) => !chavesPendentes.has(l.chave_nfe))
      .map((l) => ({
        EMPRESA: 1,
        CHAVE_NFE: l.chave_nfe,
        CPF_CNPJ_EMITENTE: l.cnpj_emitente,
        NOME_EMITENTE: l.emitente,
        DATA_EMISSAO: l.data_emissao,
        TIPO_OPERACAO_DESC: l.tipo_operacao_desc,
        VALOR_TOTAL_NF: l.valor_total,
        STATUS_ERP: 'LANCADA',
      }));

    let resultado = [...data, ...lancadasMapeadas];

    // Esconde as NF-e JÁ vinculadas a este pedido (aparecem na seção "Notas já
    // vinculadas", não devem reaparecer na lista de disponíveis).
    const jaVinculadas = await this.prisma.com_pedido_nfe_vinculo.findMany({
      where: { pedido_id: pedidoId },
      select: { chave_nfe: true },
      distinct: ['chave_nfe'],
    });
    if (jaVinculadas.length) {
      const chavesVinculadas = new Set(jaVinculadas.map((v) => v.chave_nfe));
      resultado = resultado.filter(
        (r) => !chavesVinculadas.has(String(r?.CHAVE_NFE ?? '')),
      );
    }

    // Filtro por GRUPO de fornecedores (matriz/filiais): por padrão mostra só as
    // NF-e emitidas por algum CNPJ do grupo do fornecedor do pedido. 'mostrarTodas'
    // libera o restante. Mesmo critério usado no auto-vínculo (cnpjsDoGrupo).
    if (!mostrarTodas && pedido?.for_codigo != null) {
      const cnpjsGrupo = new Set(await this.grupo.cnpjsDoGrupo(pedido.for_codigo));
      if (cnpjsGrupo.size) {
        resultado = resultado.filter((r) => {
          const cnpj = String(r?.CPF_CNPJ_EMITENTE ?? '').replace(/\D/g, '');
          return cnpj && cnpjsGrupo.has(cnpj);
        });
      }
    }

    // Esconde as NF-e sem saldo (totalmente consumidas por vínculos confirmados).
    return this.removerChavesSemSaldo(resultado);
  }

  /**
   * Remove da lista as NF-e que TÊM snapshot de saldo (com_nfe_saldo_item) e estão
   * TOTALMENTE consumidas por vínculos confirmados. NF sem snapshot (nunca vinculada)
   * é mantida — assume saldo cheio. Espelha VinculacaoNfeRepository.chavesSemSaldo.
   */
  private async removerChavesSemSaldo(
    rows: Array<Record<string, any>>,
  ): Promise<Array<Record<string, any>>> {
    const chaves = rows
      .map((r) => (r?.CHAVE_NFE == null ? null : String(r.CHAVE_NFE)))
      .filter((c): c is string => !!c);
    if (!chaves.length) return rows;

    const totais = await this.prisma.com_nfe_saldo_item.groupBy({
      by: ['chave_nfe'],
      where: { chave_nfe: { in: chaves } },
      _sum: { qtd_total: true },
    });
    if (!totais.length) return rows;

    const consumidoRows = await this.prisma.com_pedido_nfe_vinculo_item.findMany({
      where: {
        tipo: 'vinculado',
        vinculo: { chave_nfe: { in: chaves }, confirmado: true },
      },
      select: {
        quantidade_alocada: true,
        quantidade_xml: true,
        vinculo: { select: { chave_nfe: true } },
      },
    });
    const consumidoPorChave = new Map<string, number>();
    for (const r of consumidoRows) {
      const chave = r.vinculo?.chave_nfe;
      if (!chave) continue;
      const q = Number(r.quantidade_alocada ?? r.quantidade_xml ?? 0);
      consumidoPorChave.set(chave, (consumidoPorChave.get(chave) ?? 0) + (Number.isFinite(q) ? q : 0));
    }

    const TOL = 0.001;
    const semSaldo = new Set<string>();
    for (const t of totais) {
      const total = Number(t._sum.qtd_total ?? 0);
      if (total <= 0) continue;
      const consumido = consumidoPorChave.get(t.chave_nfe) ?? 0;
      if (total - consumido <= TOL) semSaldo.add(t.chave_nfe);
    }

    if (!semSaldo.size) return rows;
    return rows.filter((r) => !semSaldo.has(String(r?.CHAVE_NFE ?? '')));
  }

  private decodeXmlFromField(xmlCompleto: string): string {
    // Se vier base64+gzip, decodifica. Se já for XML, retorna como está.
    const trimmed = (xmlCompleto ?? '').trim();

    // Heurística simples: se começa com '<', já é XML
    if (trimmed.startsWith('<')) return trimmed;

    // Caso comum: base64 (possivelmente gzip)
    const b64 = trimmed.replace(/\s+/g, '');
    const buf = Buffer.from(b64, 'base64');

    try { return zlib.gunzipSync(buf).toString('utf8'); } catch (_) {}
    try { return zlib.unzipSync(buf).toString('utf8'); } catch (_) {}

    // Se não era gzip, pode ser XML puro em UTF-8 codificado em base64
    const asText = buf.toString('utf8');
    if (asText.trim().startsWith('<')) return asText;

    // Último recurso: devolve mesmo assim (pode dar erro no serviço)
    return asText;
  }

  async generateDanfe(chaveNfe: string): Promise<Buffer> {
    const rawData = await this.notaFiscalRepository.fetchNfeDistribuicao();
    const rows = chaveNfe ? rawData.filter((r) => r.CHAVE_NFE === chaveNfe) : rawData;

    if (!rows?.[0]?.XML_COMPLETO) {
      throw new Error(`Nenhum XML encontrado para a chave: ${chaveNfe}`);
    }

    const xmlText = this.decodeXmlFromField(rows[0].XML_COMPLETO);

    // cria .xml temporário
    const fileName = `${chaveNfe || 'nfe'}.xml`;
    const tempPath = join(tmpdir(), fileName);
    await fsp.writeFile(tempPath, xmlText, 'utf8');

    try {
      const form = new FormData();
      form.append('file', fs.createReadStream(tempPath), {
        filename: fileName,
        contentType: 'application/xml',
      });

      const resp = await axios.post('http://xml-to-pdf-service.acacessorios.local/danfe/file', form, {
        headers: form.getHeaders(),
        responseType: 'arraybuffer',
        validateStatus: () => true, // vamos inspecionar manualmente
        maxContentLength: Infinity,
        maxBodyLength:    Infinity,
      });

      if (resp.status !== 200) {
        // mostra msg de erro textual que o FastAPI devolve
        const msg = Buffer.from(resp.data).toString('utf8');
        throw new Error(`Falha ao gerar DANFE (HTTP ${resp.status}): ${msg}`);
      }

      // sucesso: retorna o PDF
      return Buffer.from(resp.data);
    } finally {
      // limpa o arquivo temporário
      await fsp.unlink(tempPath).catch(() => {});
    }
  }
}