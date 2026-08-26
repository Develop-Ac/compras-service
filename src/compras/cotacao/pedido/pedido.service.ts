// src/pedido/pedido.service.ts
import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import { PassThrough } from 'stream';
import { Prisma, sis_feriados } from '@prisma/client';
import { PedidoRepository } from './pedido.repository';
import { CreatePedidoDto } from './dto/create-pedido.dto';
import * as fs from 'fs';
import * as path from 'path';
import PDFDocument = require('pdfkit');
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';
import { ErpApiService } from '../../../shared/erp-api/erp-api.service';
import { CotacaoSyncService } from '../../cotacao/cotacao-sync/cotacao-sync.service';
import { GarantiaService } from '../../garantia/garantia.service';

import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import * as sql from 'mssql';

type FornecedorRow = {
  FOR_NOME: string | null;
  CELULAR: string | null;
  FONE: string | null;
  CONTATO: string | null;
  /** Se false, remove a coluna "Marca" e transfere a largura para "Descrição" */
  marca?: boolean;
};

type PdfOpts = {
  /** Se false, remove a coluna "Marca" e transfere a largura para "Descrição" */
  marca?: boolean;
};

@Injectable()
export class PedidoService {
  constructor(
    private readonly repo: PedidoRepository,
    private readonly oq: OpenQueryService,
    private readonly erpApi: ErpApiService,
    private readonly cotacaoSyncService: CotacaoSyncService, // Adicione a injeção aqui
    private readonly garantia: GarantiaService,
  ) {}

  /* ----------------------- Utils ----------------------- */
  private clampText(s: string | null | undefined, max: number) {
    const v = (s ?? '').trim();
    return v.length > max ? v.slice(0, max - 1) + '…' : v;
  }

  /**
   * Rótulo do número da cotação/pedido. Os gerados na intranet ficam numa faixa
   * reservada (>= INTRANET_COTACAO_BASE, default 100.000) para não colidir com o
   * ERP mãe e são exibidos como "I-100000"; os vindos do ERP mantêm o número puro.
   */
  private fmtPedidoCotacao(n: number | null | undefined): string {
    // Robusto ao formato do env ("100.000" -> 100000; ponto é decimal em JS)
    const base = Number(String(process.env.INTRANET_COTACAO_BASE ?? '').replace(/[^\d]/g, '')) || 100_000;
    const num = Number(n);
    if (!Number.isFinite(num)) return String(n ?? '');
    return num >= base ? `I-${num}` : String(num);
  }

  private resolveLogoPath(): string | null {
    // 1) Permitir override por ENV (robusto em Docker/prod)
    const envPath = process.env.PUBLIC_LOGO_PATH;
    if (envPath) {
      try {
        const abs = path.resolve(envPath);
        if (fs.existsSync(abs)) return abs;
      } catch {}
    }

    // 2) Candidatos comuns em dev e produção (dist/)
    const candidates = [
      // dist
      path.resolve(process.cwd(), 'dist', 'assets', 'assets', 'icon-192.png'),
      path.resolve(__dirname, '..', '..', '..', 'assets', 'icon-192.png'), // quando __dirname está em dist/src/pedido
      path.resolve(__dirname, '..', '..', 'assets', 'icon-192.png'),

      // src / root
      path.resolve(process.cwd(), 'assets', 'icon-192.png'),
      path.resolve(process.cwd(), 'src', 'assets', 'icon-192.png'),
    ];

    for (const p of candidates) {
      try {
        if (fs.existsSync(p)) return p;
      } catch {}
    }

    // Log útil para diagnosticar (não interrompe o PDF)
    // eslint-disable-next-line no-console
    console.warn('[PDF] Logo não encontrada. Candidatos testados:', candidates);
    return null;
  }

    /**
   * Busca pedido e itens por id (para sincronização)
   */
  async buscarPedidoSincronizacao(id: string) {
    const pedido = await this.repo.findByIdWithAllForSincronizacao(id);

    if (!pedido || pedido.length === 0) throw new NotFoundException('Pedido não encontrado');

    const syncData = await firstValueFrom(
      new (await import('@nestjs/axios')).HttpService().get(`http://localhost:8000/compras/cotacao-sync/${id}`)
    );
    const syncJson = syncData.data;

    // Monta um map de fornecedores da syncJson por for_codigo
    const syncForMap: Record<number, any> = {};
    for (const forn of (syncJson?.fornecedores ?? [])) {
      syncForMap[Number(forn.for_codigo)] = forn;
    }

    // Para cada pedido (agrupado por fornecedor), mescla os itens
    const result = pedido.map((p: any) => {
      const syncForn = syncForMap[Number(p.for_codigo)];

      if (!syncForn) return p;

      // Monta map de itens do syncJson por pro_codigo
      const syncItensMap: Record<number, any> = {};
      for (const syncItem of (syncForn.itens ?? [])) {
        syncItensMap[Number(syncItem.pro_codigo)] = syncItem;
      }

      // Para cada item do pedido, sobrescreve os campos divergentes com os do pedido (pedido tem prioridade)
      const itens = (p.itens ?? []).map((item: any) => {
        const syncItem = syncItensMap[Number(item.pro_codigo)];
        if (!syncItem) return item;
        // Pedido sobrescreve syncJson onde os valores forem diferentes
        return {
          ...syncItem,
          ...item,
        };
      });

      // Adiciona itens que estão no sync mas não estão no pedido
      for (const syncItem of (syncForn.itens ?? [])) {
        const exists = itens.some((i: any) => Number(i.pro_codigo) === Number(syncItem.pro_codigo));
        if (!exists) {
          itens.push({
            ...syncItem,
            ipi: syncItem.ipi != null ? Number(syncItem.ipi) : null,
            icms: syncItem.icms ?? null,
          });
        }
      }

      // Marca este fornecedor do syncJson como já processado
      syncForn.__processado = true;

      return {
        ...p,
        itens,
      };
    });

    // Adiciona fornecedores que estão no syncJson mas não estão no pedido
    for (const forn of (syncJson?.fornecedores ?? [])) {
      if (!forn.__processado) {
        result.push(forn);
      }
    }

    // Adiciona o nome do fornecedor (for_nome) em cada registro
    const nomesCache = new Map<number, string | null>();
    for (const item of result) {
      const forCodigo = Number(item?.for_codigo);
      if (!Number.isFinite(forCodigo)) {
        item.for_nome = item?.for_nome ?? null;
        continue;
      }
      if (!nomesCache.has(forCodigo)) {
        nomesCache.set(forCodigo, await this.repo.findNameFonecedorByForCodigo(forCodigo));
      }
      item.for_nome = nomesCache.get(forCodigo) ?? null;
    }

    return result;
  }

  /**
   * Busca pedido e itens completos por id (para gerencial)
   */
  async buscarPedidoGerencial(id: string) {
    const pedido = await this.repo.findByIdGerencial(id);

    if (pedido  !== null && pedido.itens  !== null) {
      // Uma consulta só para todos os itens — a busca já é em lote por natureza.
      const infoProdutos = await this.cotacaoSyncService.fetchProdutosInfoOneShot(
        pedido.itens.map((item) => item.pro_codigo),
        3,
      );

      pedido.itens = pedido.itens.map((item): any => ({
        ...item,
        min: (item as any).qtd_sugerida_min ?? null,
        max: (item as any).qtd_sugerida_max ?? null,
        ipi: item.ipi === null ? null : Number(item.ipi),
        icms: item.icms ?? null,
        pro_descricao: (item.pro_descricao ?? ''),
        custo_fabrica: infoProdutos.get(item.pro_codigo)?.custo_fabrica ?? null,
      }));
    }


    if (!pedido) throw new NotFoundException('Pedido não encontrado');
    return pedido;
  }


  /** Monta a OPENQUERY corretamente (sem alias no SELECT externo) */
  private buildFornecedorOpenQuery(forCodigo: number): string {
    const cod = Number.isFinite(forCodigo) ? Math.trunc(forCodigo) : 0;

    // SELECT interno (Firebird) — define os nomes de colunas expostos ao SELECT externo
    const inner = `
      SELECT
        FO.FOR_NOME   AS FOR_NOME,
        FO.CELULAR    AS CELULAR,
        FO.FONE       AS FONE,
        FO.CONTATO    AS CONTATO,
        FO.EMAIL      AS EMAIL
      FROM FORNECEDORES FO
      WHERE FO.EMPRESA = 3
        AND FO.FOR_CODIGO = ${cod}
    `.replace(/\s+/g, ' ').trim();

    // SELECT externo NÃO usa "FO." — usa os nomes expostos acima
    return `
      SELECT FOR_NOME, CELULAR, FONE, CONTATO, EMAIL
      FROM OPENQUERY(CONSULTA, '${inner}')
    `;
  }

  private async getFornecedor(forCodigo: number): Promise<FornecedorRow | undefined> {
    return this.erpApi.comFallback<FornecedorRow | undefined>(
      async () => {
        const r = await this.erpApi.fornecedorCompleto(forCodigo, 3);
        if (!r) return undefined;
        return {
          FOR_NOME: r.FOR_NOME ?? null,
          CELULAR: r.CELULAR ?? null,
          FONE: r.FONE ?? null,
          CONTATO: r.CONTATO ?? null,
        };
      },
      async () => {
        const sql = this.buildFornecedorOpenQuery(forCodigo);
        return this.oq.queryOne<FornecedorRow>(sql, {}, { timeout: 60_000 });
      },
    );
  }

  /* ----------------------- Casos de uso ----------------------- */

  // === Listagem leve (formata totais e valores) ===
  async listagem() {
    const pedidos = await this.repo.findAllWithLightItens();

    const fmtBR = new Intl.NumberFormat('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

    // Busca for_nome para cada pedido
    const results: Array<{
      id: string;
      pedido_cotacao: number;
      for_codigo: number;
      for_nome: string | null;
      created_at: Date;
      itens_count: number;
      total_qtd: number;
      total_valor: number;
      total_valor_fmt: string;
      status: string;
      produtos_cod: string;
      produtos_desc: string;
      tem_garantia: boolean;
      garantia_qtd_titulos: number | null;
      garantia_qtd_produtos: number | null;
    }> = [];
    // Nome do fornecedor resolvido UMA vez por código distinto: lote no cache
    // local + uma única leitura do ERP para os ausentes. Resolver dentro do
    // loop serializa uma consulta por pedido — e cada ida ao CONSULTA pode
    // custar segundos, travando a abertura da tela.
    const codigosForn = [
      ...new Set(pedidos.map((p) => Number(p.for_codigo)).filter(Number.isFinite)),
    ];
    const nomePorCodigo = new Map<number, string | null>();
    for (const f of await this.repo.findFornecedoresByIds(codigosForn)) {
      nomePorCodigo.set(f.for_codigo, f.for_nome ?? null);
    }

    const faltantes = codigosForn.filter((c) => !nomePorCodigo.has(c));
    if (faltantes.length) {
      try {
        // Leitura do cadastro: erp-firebird-api primeiro; OPENQUERY é o plano B.
        const rows = await this.erpApi.comFallback<
          Array<{ FOR_CODIGO: number; FOR_NOME: string | null }>
        >(
          () => this.erpApi.fornecedoresPorCodigo(faltantes, 3),
          () =>
            this.oq.query(
              `SELECT FOR_CODIGO, FOR_NOME FROM OPENQUERY(CONSULTA, 'select FO.FOR_CODIGO, FO.FOR_NOME from FORNECEDORES FO where FO.EMPRESA = 3 and FO.FOR_CODIGO in (${faltantes.join(',')})')`,
              {},
              { timeout: 15000, allowZeroRows: true },
            ),
        );
        const novos: { for_codigo: number; for_nome: string }[] = [];
        for (const r of rows) {
          const cod = Number(r.FOR_CODIGO);
          // CHAR do Firebird volta preenchido com espaços até o tamanho declarado
          const nome = String(r.FOR_NOME ?? '').trim() || null;
          nomePorCodigo.set(cod, nome);
          novos.push({ for_codigo: cod, for_nome: nome ?? '' });
        }
        await this.repo.insertFornecedores(novos);
      } catch {
        // Sem o ERP a listagem sai sem esses nomes; a próxima abertura tenta de novo.
      }
    }

    for (const p of pedidos) {
      let totalQtd = 0;
      let totalValor = 0;
      const codigos: string[] = [];
      const descricoes: string[] = [];
      for (const it of p.itens) {
        const q = Number(it.quantidade ?? 0);
        const vu = it.valor_unitario != null ? Number(it.valor_unitario) : 0;
        totalQtd += q;
        totalValor += q * vu;
        if (it.pro_codigo != null) codigos.push(String(it.pro_codigo));
        if (it.pro_descricao) descricoes.push(String(it.pro_descricao));
      }
      // Strings compactas (minúsculas) p/ o filtro avançado por item no cliente.
      const produtos_cod = codigos.join(' ').toLowerCase();
      const produtos_desc = descricoes.join(' | ').toLowerCase();

      const for_nome = nomePorCodigo.get(Number(p.for_codigo)) ?? null;

      results.push({
        id: p.id,
        pedido_cotacao: p.pedido_cotacao,
        for_codigo: p.for_codigo,
        status: p.status ?? '',
        for_nome,
        created_at: p.created_at,
        itens_count: p._count.itens,
        total_qtd: totalQtd,
        total_valor: totalValor,
        total_valor_fmt: `\u00A0${fmtBR.format(totalValor)}`,
        produtos_cod,
        produtos_desc,
        tem_garantia: !!p.tem_garantia,
        garantia_qtd_titulos: p.garantia_qtd_titulos ?? null,
        garantia_qtd_produtos: p.garantia_qtd_produtos ?? null,
      });
    }
    return results;
  }

  async gerarExcel(res: FastifyReply, id: string) {
    const pedido = await this.repo.findByIdWithItens(id);

    if (!pedido) throw new NotFoundException('Pedido não encontrado');

    const ExcelJSModule = await import('exceljs');
    const ExcelJS = ExcelJSModule.default ?? ExcelJSModule;
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Pedido');

    sheet.columns = [
      { header: 'Codigo', key: 'pro_codigo', width: 12 },
      { header: 'Referencia', key: 'referencia', width: 20 },
      { header: 'Descrição', key: 'pro_descricao', width: 45 },
      { header: 'Uni', key: 'unidade', width: 8 },
      { header: 'Quantidade', key: 'quantidade', width: 14 },
      { header: 'Valor Unitario', key: 'valor_unitario', width: 16 },
    ];

    // Item com quantidade 0 não será comprado — fica fora do relatório
    const itensExcel = pedido.itens.filter(
      (it) => Number(it.quantidade ?? 0) > 0,
    );

    for (const it of itensExcel) {
      sheet.addRow({
      pro_codigo: it.pro_codigo,
      referencia: it.referencia ?? '',
      pro_descricao: it.pro_descricao ?? '',
      unidade: it.unidade ?? '',
      quantidade: Number(it.quantidade ?? 0),
      valor_unitario: it.valor_unitario != null ? Number(it.valor_unitario) : '',
      });
    }

    res.header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.header('Content-Disposition', `attachment; filename="pedido_${this.fmtPedidoCotacao(pedido.pedido_cotacao)}.xlsx"`);

    // No Fastify o streaming é feito enviando um Readable para o reply,
    // preservando os headers já definidos (inclusive os do CORS).
    const stream = new PassThrough();
    res.send(stream);

    await workbook.xlsx.write(stream);
    stream.end();
  }

  /**
   * Gera PDF do pedido por ID (streaming direto no reply do Fastify).
   * - Título central alinhado verticalmente ao centro da logo
   * - Bloco COMPRADOR à esquerda
   * - Sem metadados à direita (removidos)
   * - **Tabela**: 'Ref' 1ª coluna; 'Código' removido; **'Marca' opcional via opts.marca**
   * - Se 'Marca' for removida, 'Descrição' aumenta a largura (somando a largura de 'Marca')
   * - Descrição com clamp dinâmico por largura; valores sem "R$"; totais empilhados
   * - **Novo**: Bloco FORNECEDOR (via OPENQUERY) logo abaixo do endereço do comprador
   */
  async gerarPdfPedidoExpress(
    res: FastifyReply,
    id: string,
    opts?: PdfOpts, // <<<<<<<<<<<<< adicionamos opts.marca
  ) {
    const pedido = await this.repo.findByIdWithItens(id);
    console.log(pedido);
    if (!pedido) throw new NotFoundException('Pedido não encontrado');

    // Busca fornecedor via OPENQUERY antes de montar o PDF
    const fornecedor = await this.getFornecedor(Number(pedido.for_codigo));

    const doc = new PDFDocument({ size: 'A4', margin: 40, bufferPages: true });

    res.header('Content-Type', 'application/pdf');
    res.header(
      'Content-Disposition',
      `inline; filename="pedido_${this.fmtPedidoCotacao(pedido.pedido_cotacao)}_id_${pedido.id}.pdf"`,
    );
    // O Fastify faz o pipe do Readable (PDFDocument) para o socket.
    res.send(doc);

    // Geometria
    const startX = doc.page.margins.left;
    const startY = doc.page.margins.top;
    const usableWidth =
      doc.page.width - doc.page.margins.left - doc.page.margins.right;

    // Logo (usa Buffer + fit para evitar problemas de path e distorção)
    const logoPath = this.resolveLogoPath();
    const logoX = startX;
    const logoY = startY;
    const logoW = 70;
    const logoH = 70;
    if (logoPath) {
      try {
        const imgBuf = fs.readFileSync(logoPath);
        doc.image(imgBuf, logoX, logoY, { fit: [logoW, logoH] });
      } catch (e: any) {
        // eslint-disable-next-line no-console
        console.error('[PDF] Falha ao carregar a logo:', e?.message || e);
      }
    } else {
      // eslint-disable-next-line no-console
      console.warn('[PDF] Prosseguindo sem logo (logoPath null).');
    }

    // Título alinhado ao centro da logo
    const title = `AC Acessórios - Pedido de Compra - ${this.fmtPedidoCotacao(pedido.pedido_cotacao)}`;
    doc.font('Helvetica-Bold').fontSize(14).fillColor('#000');
    const titleLineH = doc.currentLineHeight();
    const logoCenterY = logoY + logoH / 2;
    const titleY = logoCenterY - titleLineH / 2;
    doc.text(title, startX, titleY, { width: usableWidth, align: 'center' });

    // Abaixo do título (sem sobrepor a logo)
    let y = Math.max(startY + logoH, titleY + titleLineH) + 8;

    // Bloco COMPRADOR à esquerda
    const gutter = 16;
    const rightColWidth = 230;
    const leftColWidth = usableWidth - rightColWidth - gutter;
    const leftX = startX;

    let yLeft = y;
    doc.font('Helvetica-Bold').fontSize(10).fillColor('#000');
    doc.text('C.M. SIQUEIRA & CIA LTDA', leftX, yLeft, {
      width: leftColWidth,
      align: 'left',
    });
    yLeft += 12;

    doc.font('Helvetica').fontSize(9).fillColor('#000');
    doc.text(
      'AVENIDA PERIMETRAL SUDESTE, 10187 - CEP: 78.896-052',
      leftX,
      yLeft,
      { width: leftColWidth, align: 'left' },
    );
    yLeft += 12;

    doc.text('CENTRO - SORRISO - MT', leftX, yLeft, {
      width: leftColWidth,
      align: 'left',
    });
    yLeft += 12;

    // === Bloco FORNECEDOR (abaixo do trecho solicitado)
    if (fornecedor) {
      const linha = (valor?: string | null) => (valor ?? '').toString().trim();

      yLeft += 4;
      doc.font('Helvetica-Bold').fontSize(10).fillColor('#000');

      // Limite de 28 caracteres no NOME do fornecedor
      const nomeFornecedor = this.clampText(fornecedor.FOR_NOME, 28);

      doc.text(`FORNECEDOR: ${nomeFornecedor}`, leftX, yLeft, {
        width: leftColWidth,
        align: 'left',
      });
      yLeft += 12;

      doc.font('Helvetica').fontSize(9).fillColor('#000');
      const contato = linha(fornecedor.CONTATO);
      if (contato) {
        doc.text(contato, leftX, yLeft, { width: leftColWidth, align: 'left' });
        yLeft += 12;
      }

      const telefones = [fornecedor.FONE, fornecedor.CELULAR]
        .map(linha)
        .filter((v) => v.length > 0)
        .join(' / ');
      if (telefones) {
        doc.text(telefones, leftX, yLeft, { width: leftColWidth, align: 'left' });
        yLeft += 12;
      }

      const email = fornecedor && 'EMAIL' in fornecedor ? linha((fornecedor as any).EMAIL) : '';
      if (email) {
        doc.text(email, leftX, yLeft, { width: leftColWidth, align: 'left' });
        yLeft += 12;
      }
    }

    // Avança Y só pelo bloco esquerdo (metadados à direita continuam removidos)
    y = yLeft + 12;

    // ====== Tabela ======
    const showMarca = opts?.marca !== false; // padrão: true

    // 1) Larguras base tipadas como literais
    const W = {
      ref: 140, // <<<<<<<< Aumentado para caber "5U0805584C1NN"
      pro_codigo: 50,
      descricao: showMarca ? 230 : 290, // <<<<<<<< Diminuído
      marca: 60,
      un: 20,
      qtd: 25,
      unit: 65,
      total: 65,
    } as const;

    // 🔧 Tipos para colunas (no mesmo escopo)
    type ColumnKey = keyof typeof W;
    type ColumnSpec = { key: ColumnKey; width: number; align?: 'left' | 'right' };

    // 2) Se "Marca" não for exibida, transfere a largura para "Descrição"
    const descricaoWidth = showMarca ? W.descricao : W.descricao + W.marca;

    // 3) Monte as colunas sem spread condicional (evita widening para string)
    const cols: ColumnSpec[] = [
      { key: 'ref', width: W.ref, align: 'left' },
      { key: 'pro_codigo', width: W.pro_codigo, align: 'left' },
      { key: 'descricao', width: descricaoWidth, align: 'left' },
    ];

    if (showMarca) {
      cols.push({ key: 'marca', width: W.marca, align: 'left' });
    }

    cols.push(
      { key: 'un', width: W.un, align: 'left' },
      { key: 'qtd', width: W.qtd, align: 'right' },
      { key: 'unit', width: W.unit, align: 'right' },
      { key: 'total', width: W.total, align: 'right' },
    );

    // 4) Map de headers tipado
    const headerMap: Record<ColumnKey, string> = {
      ref: 'Ref',
      pro_codigo: 'Código',
      descricao: 'Descrição',
      marca: 'Marca',
      un: 'Un',
      qtd: 'Qtd',
      unit: 'Unit',
      total: 'Total',
    };

    // 5) Ajuste proporcional para caber em usableWidth
    const baseWidth = cols.reduce((acc, c) => acc + c.width, 0);
    if (baseWidth > usableWidth) {
      const scale = usableWidth / baseWidth;
      for (const c of cols) c.width = Math.floor(c.width * scale);
    }

    // Helpers de formatação
    const approxClampByWidth = (txt: string, widthPx: number) => {
      // Aproxima caracteres por ~6px cada (fonte 8)
      const maxChars = Math.max(3, Math.floor(widthPx / 6));
      return this.clampText(txt, maxChars);
    };
    const fmtNum2 = (n: number) =>
      new Intl.NumberFormat('pt-BR', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(n);
    const fmtQtd = (n: number) =>
      new Intl.NumberFormat('pt-BR', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 3,
      }).format(n);

    // Header
    doc.font('Helvetica-Bold').fontSize(8).fillColor('#333');
    let x = startX;
    for (const c of cols) {
      doc.text(headerMap[c.key], x, y, { width: c.width, align: c.align ?? 'left' });
      x += c.width;
    }
    y += 12;
    doc
      .moveTo(startX, y - 3)
      .lineTo(startX + usableWidth, y - 3)
      .strokeColor('#CCCCCC')
      .lineWidth(1)
      .stroke();

    // Função para redesenhar o header após quebra
    const drawHeader = () => {
      doc.font('Helvetica-Bold').fontSize(8).fillColor('#333');
      let xx = startX;
      for (const c of cols) {
        doc.text(headerMap[c.key], xx, y, { width: c.width, align: c.align ?? 'left' });
        xx += c.width;
      }
      y += 12;
      doc
        .moveTo(startX, y - 3)
        .lineTo(startX + usableWidth, y - 3)
        .strokeColor('#CCCCCC')
        .lineWidth(1)
        .stroke();
      doc.font('Helvetica').fontSize(8).fillColor('#000');
    };

    // Linhas
    doc.font('Helvetica').fontSize(8).fillColor('#000');
    let totalQtd = 0;
    let totalGeral = 0;

    // Item com quantidade 0 não será comprado — fica fora do relatório
    const itensPdf = pedido.itens.filter(
      (it) => Number(it.quantidade ?? 0) > 0,
    );

    for (const it of itensPdf) {
      // quebra de página
      if (y > doc.page.height - doc.page.margins.bottom - 40) {
        doc.addPage();
        y = doc.page.margins.top;
        drawHeader();
      }

      const qtd = Number(it.quantidade ?? 0);
      const unit = it.valor_unitario != null ? Number(it.valor_unitario) : 0;
      const linha = qtd * unit;

      totalQtd += qtd;
      totalGeral += linha;

      // Valores crus
      const values: Record<ColumnKey, string> = {
        ref: (it.referencia ?? '').toString(),
        pro_codigo: (it.pro_codigo ?? '').toString(),
        descricao: (it.pro_descricao ?? '').toString(),
        marca: (it.mar_descricao ?? '').toString(),
        un: (it.unidade ?? '').toString(),
        qtd: fmtQtd(qtd),
        unit: unit ? fmtNum2(unit) : '',
        total: fmtNum2(linha),
      };

      // Render conforme colunas ativas
      x = startX;
      for (const c of cols) {
        const raw = (values[c.key] ?? '').trim();
        const txt =
          c.key === 'qtd' || c.key === 'unit' || c.key === 'total'
            ? raw
            : approxClampByWidth(raw, c.width);
        doc.text(txt, x, y, { width: c.width, align: c.align ?? 'left' });
        x += c.width;
      }

      y += 11;
    }

    // Totais empilhados
    y += 6;
    doc
      .moveTo(startX, y - 3)
      .lineTo(startX + usableWidth, y - 3)
      .strokeColor('#333333')
      .lineWidth(1.2)
      .stroke();

    doc.font('Helvetica-Bold').fontSize(9).fillColor('#000');
    y += 10;
    doc.text(`Total de Itens: ${fmtQtd(totalQtd)}`, startX, y, { align: 'left' });
    y += 12;
    doc.text(`Preço total: ${fmtNum2(totalGeral)}`, startX, y, { align: 'left' });

    doc.end();
  }

  /**
   * Idempotente por pedido_cotacao:
   * - se já existir com_pedido, limpa os itens e recria.
   * - se não, cria o cabeçalho e os itens.
   */
  async createOrReplace(dto: CreatePedidoDto) {
    const { pedido_cotacao, itens } = dto;

    const BASE =
      process.env.PUBLIC_BASE_URL?.replace(/\/+$/, '') ||
      'https://intranetbackend.acacessorios.local/compras';

    // Agrupa por fornecedor
    // Agrupa itens por fornecedor e armazena o frete de cada grupo (primeiro frete encontrado para o fornecedor)
    const byFor: Record<number, typeof itens> = {};
    const freteByFor: Record<number, number> = {};
    const prazoByFor: Record<string, string> = {};
    const nomeFreteByFor: Record<string, string> = {};
    for (const it of itens) {
      const f = Number(it.for_codigo);
      if (!Number.isFinite(f)) continue;
      (byFor[f] ??= []).push(it);
      // Salva o frete do primeiro item do fornecedor (ou sobrescreva se quiser o último)
      if (freteByFor[f] === undefined && 'frete' in it) {
      freteByFor[f] = typeof it.frete === 'number' ? it.frete : Number(it.frete);
      }
      if (prazoByFor[f] === undefined && 'prazo' in it) {
      prazoByFor[f] = typeof it.prazo === 'string' ? it.prazo : String(it.prazo);
      }
      if (nomeFreteByFor[f] === undefined && 'nomeFrete' in it) {
      nomeFreteByFor[f] = typeof it.nomeFrete === 'string' ? it.nomeFrete : String(it.nomeFrete);
      }
    }

    const result = await this.repo.transaction(async (tx) => {
      const created: { id: string; pedido_cotacao: number; for_codigo: number }[] =
        [];

      for (const [forStr, grupo] of Object.entries(byFor)) {
        const for_codigo = Number(forStr);

        // Upsert do cabeçalho (dentro da TX)
        const pedido = await this.repo.upsertPedidoByCotacaoFornecedor(
          tx,
          pedido_cotacao,
          for_codigo,
          prazoByFor[for_codigo],
        );

        // Limpa itens e recria
        await this.repo.deleteItensByPedidoId(tx, pedido.id);

        const data: Prisma.com_pedido_itensCreateManyInput[] = grupo.map((i) => ({
          pedido_id: pedido.id,
          item_id_origem: i.id ?? null,
          pro_codigo: i.pro_codigo,
          pro_descricao: i.pro_descricao,
          mar_descricao: i.mar_descricao ?? null,
          referencia: i.referencia ?? null,
          unidade: i.unidade ?? null,
          emissao: i.emissao ? new Date(i.emissao) : null,
          valor_unitario:
            i.valor_unitario != null ? new Prisma.Decimal(i.valor_unitario) : null,
          custo_fabrica:
            i.custo_fabrica != null ? new Prisma.Decimal(i.custo_fabrica) : null,
          preco_custo:
            i.preco_custo != null ? new Prisma.Decimal(i.preco_custo) : null,
          for_codigo,
          quantidade: new Prisma.Decimal(i.quantidade as any),
          ipi: i.ipi != null ? new Prisma.Decimal(i.ipi) : null,
          icms: i.icms ?? null,
          justificativa: i.justificativa ?? null,
          quantidade_real: true
        }));

        await this.repo.createManyItens(tx, data);

        for (const i of grupo) {
          await tx.$executeRaw`
            UPDATE com_pedido_itens
            SET
              qtd_sugerida_min = ${i.qtd_sugerida_min ?? null},
              qtd_sugerida_max = ${i.qtd_sugerida_max ?? null}
            WHERE pedido_id = ${pedido.id}
              AND pro_codigo = ${Number(i.pro_codigo)}
              AND for_codigo = ${for_codigo}
          `;
        }

        created.push({
          id: pedido.id,
          pedido_cotacao: pedido.pedido_cotacao,
          for_codigo,
        });
      }

      return created;
    });

    await fetch('http://log-service.acacessorios.local/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          usuario: dto.usuario,
          setor: 'Compras',
          tela: 'Comparativo',
          acao: 'Create',
          descricao: `Pedido criado/atualizado para cotação ${dto.pedido_cotacao} com ${itens.length} itens e ${Object.keys(byFor).length} fornecedores`,
        }),
      });

    // Recalcula o resumo de garantia de cada pedido gerado (fire-and-forget: não
    // bloqueia nem falha a geração se o ERP/Stage estiver indisponível).
    for (const p of result) {
      this.garantia
        .recalcularGarantiaPedido(p.id)
        .catch(() => undefined);
    }

    return {
      ok: true,
      pedidos_criados: result.length,
      pedidos: result.map((p) => ({
        ...p,
        pdf_url: `${BASE}/pedido/${p.id}`,
      })),
    };
  }

  /** Atualiza transportadora (nomeFrete e frete) de um pedido */
  async atualizarTransportadora(id: string, nomeFrete: string, frete: number, categoriaFrete: string) {
    const pedido = await this.repo.updateTransportadora(id, nomeFrete, frete, categoriaFrete);
    return { ok: true, pedido };
  }

  /** Atualiza autorização de um item do pedido */
  async atualizarAutorizacaoItem(
    pedidoId: string, 
    itemId: string, 
    coluna: 'carlos' | 'renato', 
    check: boolean
  ) {
    // Primeiro verifica se o item pertence ao pedido
    const item = await this.repo.findByIdWithItensToAutorizar(pedidoId);
    if (!item) {
      throw new NotFoundException('Pedido não encontrado');
    }

    const itemExiste = item.itens.some(i => i.id === itemId);
    if (!itemExiste) {
      throw new NotFoundException('Item não encontrado no pedido');
    }

    // Atualiza a autorização
    const itemAtualizado = await this.repo.updateItemAutorizacao(itemId, coluna, check);
    
    return {
      ok: true,
      message: `Autorização ${coluna} ${check ? 'concedida' : 'removida'} para o item`,
      item: itemAtualizado
    };
  }

  async atualizarJustificativaItem(pedidoId: string, itemId: string, justificativa: string) {
    const item = await this.repo.updateItemJustificativa(pedidoId, itemId, justificativa);

    if (!item) {
      throw new NotFoundException(
        `Item ${itemId} não encontrado no pedido ${pedidoId}`,
      );
    }

    return {
      ok: true,
      message: 'Justificativa atualizada com sucesso',
      item,
    };
  }

  async atualizarQuantidadeItem(pedidoId: string, itemId: string, quantidade: number, quantidade_antiga: number, usuario: string) {
    const item = await this.repo.updateItemQuantidade(pedidoId, itemId, quantidade);

    const itemInfo = (await this.repo.findItemInfoById(itemId))?.pro_codigo;

    await fetch('http://log-service.acacessorios.local/log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        usuario: usuario,
        setor: 'Compras',
        tela: 'Detalhes do Pedido',
        acao: 'Update',
        descricao: `Atualizada quantidade do item ${itemInfo} no pedido ${pedidoId}, de ${quantidade_antiga} para ${quantidade}.`,
      }),
    });

    if (!item) {
      throw new NotFoundException(
        `Item ${itemId} não encontrado no pedido ${pedidoId}`,
      );
    }

    return {
      ok: true,
      message: 'Quantidade atualizada com sucesso',
      item,
    };
  }

  async atualizarStatus(id: string, status: string) {
    const pedido = await this.repo.updateStatus(id, status);
    if (!pedido) throw new NotFoundException(`Pedido ${id} não encontrado`);
    return pedido;
  }

  async atualizarPrevisaoChegada(id: string, previsao_chegada: string | Date | null) {
    let data: Date | null = null;

    if (previsao_chegada !== null && previsao_chegada !== undefined && previsao_chegada !== '') {
      const parsed = new Date(previsao_chegada);
      if (isNaN(parsed.getTime())) {
        throw new BadRequestException(
          `previsao_chegada inválida: ${String(previsao_chegada)}`,
        );
      }
      data = parsed;
    }

    try {
      const pedido = await this.repo.updatePrevisaoChegada(id, data);
      return {
        ok: true,
        message: 'Previsão de chegada atualizada com sucesso',
        pedido,
      };
    } catch (e) {
      if (
        e instanceof Prisma.PrismaClientKnownRequestError &&
        e.code === 'P2025'
      ) {
        throw new NotFoundException(`Pedido ${id} não encontrado`);
      }
      throw e;
    }
  }

  /* ----------------------- Inserção no Celta ----------------------- */
  /**
   * Monta o payload do pedido a partir do próprio banco (findByIdGerencial) e
   * encaminha para o serviço externo de compras (rota /cotacoes), cujo endereço
   * base vem do env API_COMPRAS_SERVICE e cuja API Key vem do env
   * API_COMPRAS_SERVICE_TOKEN (enviada no header x-api-key). A rota não recebe body.
   */
  async inserirCelta(pedidoId: string) {
    const id = String(pedidoId ?? '').trim();
    if (!id) throw new BadRequestException('pedido_id é obrigatório');

    const pedido = await this.repo.findByIdGerencial(id);
    if (!pedido) throw new NotFoundException(`Pedido ${id} não encontrado`);

    const for_codigo = Number(pedido.for_codigo);
    if (!Number.isFinite(for_codigo)) {
      throw new BadRequestException(`Pedido ${id} está sem for_codigo`);
    }

    const previsao_chegada = pedido.previsao_chegada
      ? new Date(pedido.previsao_chegada).toISOString()
      : null;

    // A intranet trabalha sempre na empresa 3 (mesmo default usado no restante do módulo).
    const empresa = Number(process.env.EMPRESA_CELTA ?? 3) || 3;

    const itens = (pedido.itens ?? []).map((item) => {
      const quantidade = Number(item.quantidade ?? 0);
      const unitario = Number(item.valor_unitario ?? 0);
      return {
        empresa,
        pro_codigo: Number(item.pro_codigo),
        quantidade,
        unitario,
        unidade: item.unidade ?? 'UN',
        total: Number((quantidade * unitario).toFixed(2)),
      };
    });

    if (itens.length === 0) {
      throw new BadRequestException(`Pedido ${id} não possui itens para enviar`);
    }

    const base = String(process.env.API_COMPRAS_SERVICE ?? '')
      .trim()
      .replace(/\/+$/, '');
    if (!base) {
      throw new BadRequestException('API_COMPRAS_SERVICE não configurado no ambiente');
    }

    const apiKey = String(process.env.API_COMPRAS_SERVICE_TOKEN ?? '').trim();
    if (!apiKey) {
      throw new BadRequestException(
        'API_COMPRAS_SERVICE_TOKEN não configurado no ambiente',
      );
    }

    const url = `${base}/cotacoes`;
    const payload = { for_codigo, previsao_chegada, itens };

    let resp: Response;
    try {
      resp = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
        },
        body: JSON.stringify(payload),
      });
    } catch (e: any) {
      throw new BadRequestException(
        `Falha ao chamar ${url}: ${e?.message ?? String(e)}`,
      );
    }

    const texto = await resp.text();
    let data: any = texto;
    try {
      data = texto ? JSON.parse(texto) : null;
    } catch {
      /* resposta não-JSON: mantém o texto cru */
    }

    if (!resp.ok) {
      throw new BadRequestException({
        message: `Erro ao inserir pedido no Celta (${resp.status})`,
        pedido_id: id,
        status: resp.status,
        data,
      });
    }

    // O retorno da API traz o número do pedido no Celta em "pedido_cotacao".
    const pedido_celta = Number(data?.pedido_cotacao);
    if (!Number.isFinite(pedido_celta)) {
      throw new BadRequestException({
        message:
          'A API de compras não devolveu "pedido_cotacao"; a amarração intranet x Celta não foi gravada.',
        pedido_id: id,
        status: resp.status,
        data,
      });
    }

    // O pedido_intranet é o próprio id do pedido (com_pedido.id).
    const pedido_intranet = id;
    const vinculo = await this.repo.upsertPedidoIntranetCelta(
      pedido_intranet,
      pedido_celta,
    );

    return {
      ok: true,
      message: 'Pedido enviado para o Celta com sucesso',
      pedido_id: id,
      pedido_intranet,
      pedido_celta,
      status: resp.status,
      vinculo,
      data,
    };
  }
}
