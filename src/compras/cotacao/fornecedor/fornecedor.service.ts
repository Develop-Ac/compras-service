import { Injectable, HttpException, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { AxiosError } from 'axios';
import { CreateFornecedorDto } from './fornecedor.dto';
import { FornecedorRepository } from './fornecedor.repository';

@Injectable()
export class FornecedorService {
  private readonly logger = new Logger(FornecedorService.name);

  constructor(
    private readonly repository: FornecedorRepository,
    private readonly http: HttpService,
    private readonly config: ConfigService,
  ) {}

  private normalizeDto(dto: CreateFornecedorDto): CreateFornecedorDto {
    // Normaliza itens (quantidade e pro_codigo numéricos; ajuste se pro_codigo no banco for TEXT)
    const itens = (dto.itens ?? []).map((i) => {
      const qtd =
        typeof i.QUANTIDADE === 'string'
          ? Number(String(i.QUANTIDADE).replace(',', '.'))
          : Number(i.QUANTIDADE);
      if (Number.isNaN(qtd)) {
        throw new HttpException(`QUANTIDADE inválida no item ${i.PRO_CODIGO}`, 400);
      }

      const proCodigo = Number(i.PRO_CODIGO);
      if (Number.isNaN(proCodigo)) {
        throw new HttpException(`PRO_CODIGO inválido (esperado número): ${i.PRO_CODIGO}`, 400);
      }

      return {
        ...i,
        QUANTIDADE: qtd,
        PRO_CODIGO: proCodigo,
        EMISSAO: i.EMISSAO ?? null,
      };
    });

    // 🔧 Importante: não enviar null para o Next
    const raw = dto.cpf_cnpj ?? undefined;
    const cpf_cnpj =
      typeof raw === 'string'
        ? (raw.trim() === '' ? undefined : raw.trim())
        : undefined;

    return { ...dto, cpf_cnpj, itens };
  }

  /**
   * Roda o upsert local (Prisma) **e** envia o payload para o Next.
   * Se o POST ao Next falhar, a transação local é revertida.
   */
  async upsertLocalEEnviarParaNext(dtoIn: CreateFornecedorDto) {
    const dto = this.normalizeDto(dtoIn);

    const base = this.config.get<string>('NEXT_BASE_URL', 'http://127.0.0.1:3002');
    // const base = 'http://localhost:3001'
    const apiKey = this.config.get<string>('NEXT_API_KEY', '');
    if (!base) throw new Error('NEXT_BASE_URL não configurado');
    const url = new URL('/api/cotacao', base).toString();

    const idemKey = `cotacao:${dto.pedido_cotacao}:${dto.for_codigo}`;

    // 1) UPSERT cabeçalho local (com_cotacao_for)
    await this.repository.upsertFornecedor({
      where: {
        pedido_for: {
          pedido_cotacao: dto.pedido_cotacao,
          for_codigo: dto.for_codigo,
        },
      },
      update: {
        for_nome: dto.for_nome,
        cpf_cnpj: dto.cpf_cnpj ?? null,
      },
      create: {
        pedido_cotacao: dto.pedido_cotacao,
        for_codigo: dto.for_codigo,
        for_nome: dto.for_nome,
        cpf_cnpj: dto.cpf_cnpj ?? null,
      },
    });

    // 2) Busca ITENS-BASE da cotação apenas pelo pedido_cotacao
    const itensBase = await this.repository.findCotacaoItens(dto.pedido_cotacao);

    // 3) Monta payload no formato esperado pelo Next
    // Tipagem explícita para row, incluindo dt_ultima_compra
    type CotacaoItemRow = {
      emissao?: Date | string | null;
      pro_codigo: number | string;
      pro_descricao: string;
      mar_descricao?: string | null;
      referencia?: string | null;
      unidade?: string | null;
      quantidade: number | string;
      qtd_sugerida: number | string;
      dt_ultima_compra?: Date | string | null;
    };

    const itensParaNext = (itensBase as CotacaoItemRow[]).map((row) => ({
      PEDIDO_COTACAO: dto.pedido_cotacao,
      EMISSAO: row.emissao ? new Date(row.emissao as any).toISOString() : null,
      PRO_CODIGO: row.pro_codigo as number | string, // se no banco for TEXT, string é segura
      PRO_DESCRICAO: row.pro_descricao as string,
      MAR_DESCRICAO: (row.mar_descricao as string | null) ?? null,
      REFERENCIA: (row.referencia as string | null) ?? null,
      UNIDADE: (row.unidade as string | null) ?? null,
      QUANTIDADE: Number(row.quantidade),
      QTD_SUGERIDA: Number(row.qtd_sugerida),
      DT_ULTIMA_COMPRA: row.dt_ultima_compra ?? null,
    }));

    // ⚠️ NÃO envie cpf_cnpj: null — use undefined para omitir no JSON
    const payloadParaNext: any = {
      pedido_cotacao: dto.pedido_cotacao,
      for_codigo: dto.for_codigo,
      for_nome: dto.for_nome,
      cpf_cnpj: dto.cpf_cnpj ?? undefined, // <— omitido se undefined
      itens: itensParaNext,
    };

    // Logs úteis para depuração (sem vazar cpf_cnpj se ausente)
    this.logger.debug(
      `[FornecedorService] Enviando para Next ${url} — itens: ${itensParaNext.length}`,
    );

    try {
      const { data, status } = await firstValueFrom(
        this.http.post(url, payloadParaNext, {
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': apiKey,
            'Idempotency-Key': idemKey,
          },
          validateStatus: (s) => s < 500, // tratamos 4xx aqui
        }),
      );

      this.logger.debug(
        `[FornecedorService] Next respondeu status=${status} body=${JSON.stringify(data)}`,
      );

      if (status >= 400) {
        throw new HttpException(data?.error || 'Falha ao processar no Next', status);
      }

      await fetch('http://log-service.acacessorios.local/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          usuario: dto.usuario,
          setor: 'Compras',
          tela: 'Cotação de Compra',
          acao: 'Create',
          descricao: `Enviada cotação ${dto.pedido_cotacao} para o fornecedor ${dto.for_codigo} no Portal-Fornecedor com ${itensParaNext.length} itens.`,
        }),
      });

      return { ok: true, next: data, itens_enviados: itensParaNext.length };
    } catch (err) {
      const e = err as AxiosError<any>;
      const status = e.response?.status ?? 500;
      const details = e.response?.data ?? e.message ?? 'Erro ao chamar Next';
      this.logger.error(
        `POST ${url} falhou: ${typeof details === 'string' ? details : JSON.stringify(details)}`,
      );
      throw new HttpException(
        { error: 'Next falhou; transação local revertida', details },
        status,
      );
    }
  }

  async listarFornecedoresPorPedido(pedido_cotacao: number) {
    const rows = await this.repository.listarFornecedoresPorPedido(pedido_cotacao);
    return rows.map((r) => ({
      for_codigo: r.for_codigo ?? null,
      for_nome: r.for_nome ?? null,
      cpf_cnpj: r.cpf_cnpj ?? null,
    }));
  }
}
