// src/pedido/pedido.service.ts
import { Injectable, NotFoundException } from '@nestjs/common';
import { Response as ExpressResponse } from 'express';
import { Prisma } from '@prisma/client';
import { PedidoRepository } from './pedido.repository';
import { CreatePedidoDto } from './dto/create-pedido.dto';
import * as fs from 'fs';
import * as path from 'path';
import PDFDocument = require('pdfkit');
import { OpenQueryService } from '../../../shared/database/openquery/openquery.service';

type FornecedorRow = {
  FOR_NOME: string | null;
  CELULAR: string | null;
  FONE: string | null;
  CONTATO: string | null;
  EMAIL: string | null;
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
  ) {}

  /* ----------------------- Utils ----------------------- */
  private clampText(s: string | null | undefined, max: number) {
    const v = (s ?? '').trim();
    return v.length > max ? v.slice(0, max - 1) + '…' : v;
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
    const sql = this.buildFornecedorOpenQuery(forCodigo);
    return await this.oq.queryOne<FornecedorRow>(sql, {}, { timeout: 60_000 });
  }

  /* ----------------------- Casos de uso ----------------------- */

  // === Listagem leve (formata totais e valores) ===
  async listagem() {
    const pedidos = await this.repo.findAllWithLightItens();

    const fmtBR = new Intl.NumberFormat('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

    return pedidos.map((p) => {
      let totalQtd = 0;
      let totalValor = 0;
      for (const it of p.itens) {
        const q = Number(it.quantidade ?? 0);
        const vu = it.valor_unitario != null ? Number(it.valor_unitario) : 0;
        totalQtd += q;
        totalValor += q * vu;
      }
      return {
        id: p.id,
        pedido_cotacao: p.pedido_cotacao,
        for_codigo: p.for_codigo,
        created_at: p.created_at,
        itens_count: p._count.itens,
        total_qtd: totalQtd,
        total_valor: totalValor,
        total_valor_fmt: `\u00A0${fmtBR.format(totalValor)}`, // sem "R$"
      };
    });
  }

  /**
   * Gera PDF do pedido por ID (Express).
   * - Título central alinhado verticalmente ao centro da logo
   * - Bloco COMPRADOR à esquerda
   * - Sem metadados à direita (removidos)
   * - **Tabela**: 'Ref' 1ª coluna; 'Código' removido; **'Marca' opcional via opts.marca**
   * - Se 'Marca' for removida, 'Descrição' aumenta a largura (somando a largura de 'Marca')
   * - Descrição com clamp dinâmico por largura; valores sem "R$"; totais empilhados
   * - **Novo**: Bloco FORNECEDOR (via OPENQUERY) logo abaixo do endereço do comprador
   */
  async gerarPdfPedidoExpress(
    res: ExpressResponse,
    id: string,
    opts?: PdfOpts, // <<<<<<<<<<<<< adicionamos opts.marca
  ) {
    const pedido = await this.repo.findByIdWithItens(id);
    if (!pedido) throw new NotFoundException('Pedido não encontrado');

    // Busca fornecedor via OPENQUERY antes de montar o PDF
    const fornecedor = await this.getFornecedor(Number(pedido.for_codigo));

    const doc = new PDFDocument({ size: 'A4', margin: 40, bufferPages: true });

    res.set('Content-Type', 'application/pdf');
    res.set(
      'Content-Disposition',
      `inline; filename="pedido_${pedido.pedido_cotacao}_id_${pedido.id}.pdf"`,
    );
    doc.pipe(res as unknown as NodeJS.WritableStream);

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
    const title = `AC Acessórios - Pedido de Compra - ${pedido.pedido_cotacao}`;
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

      const email = linha(fornecedor.EMAIL);
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
      ref: 70,
      descricao: 300,
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

    for (const it of pedido.itens) {
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
    const byFor: Record<number, typeof itens> = {};
    for (const it of itens) {
      const f = Number(it.for_codigo);
      if (!Number.isFinite(f)) continue;
      (byFor[f] ??= []).push(it);
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
        }));

        await this.repo.createManyItens(tx, data);

        created.push({
          id: pedido.id,
          pedido_cotacao: pedido.pedido_cotacao,
          for_codigo,
        });
      }

      return created;
    });

    return {
      ok: true,
      pedidos_criados: result.length,
      pedidos: result.map((p) => ({
        ...p,
        pdf_url: `${BASE}/pedido/${p.id}`,
      })),
    };
  }
}
