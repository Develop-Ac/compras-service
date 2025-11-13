import { Injectable } from '@nestjs/common';
import { NotaFiscalRepository } from './notaFiscal.repository';
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
  constructor(private readonly notaFiscalRepository: NotaFiscalRepository) {}

  async getNfeDistribuicao() {
    const data = await this.notaFiscalRepository.fetchNfeDistribuicao();
    return data;
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