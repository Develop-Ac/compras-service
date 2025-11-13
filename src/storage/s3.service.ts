// src/storage/s3.service.ts
import { Injectable } from '@nestjs/common';
import {
  S3Client,
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand, // apenas para consistência, não é obrigatório pro presign
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import * as https from 'https';

type S3Opts = {
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucketDefault: string;
  forcePathStyle?: boolean;
  tlsInsecure?: boolean;
};

@Injectable()
export class S3Service {
  private client: S3Client;
  private bucketDefault: string;

  constructor() {
    const opts: S3Opts = {
      endpoint: process.env.S3_ENDPOINT || process.env.MINIO_ENDPOINT || 'http://localhost:9000',
      region: process.env.S3_REGION || 'us-east-1',
      accessKeyId: process.env.S3_ACCESS_KEY || process.env.MINIO_ROOT_USER || 'admin',
      secretAccessKey: process.env.S3_SECRET_KEY || process.env.MINIO_ROOT_PASSWORD || 'password',
      bucketDefault: process.env.S3_BUCKET_DEFAULT || process.env.S3_BUCKET_AVARIAS || 'avarias',
      forcePathStyle: true,
      tlsInsecure: ['1', 'true', 'yes'].includes(String(process.env.S3_TLS_INSECURE || '').toLowerCase()),
    };

    const isHttps = opts.endpoint.startsWith('https://');
    const handler = isHttps && opts.tlsInsecure
      ? new NodeHttpHandler({
          httpsAgent: new https.Agent({ rejectUnauthorized: false }),
        })
      : undefined;

    this.client = new S3Client({
      region: opts.region,
      endpoint: opts.endpoint,
      forcePathStyle: opts.forcePathStyle,
      credentials: {
        accessKeyId: opts.accessKeyId,
        secretAccessKey: opts.secretAccessKey,
      },
      ...(handler ? { requestHandler: handler } : {}),
    });

    this.bucketDefault = opts.bucketDefault;
  }

  getDefaultBucket() {
    return this.bucketDefault;
  }

  async putObject(
    key: string,
    body: Buffer | Uint8Array | Blob | string,
    contentType = 'application/octet-stream',
    bucket = this.bucketDefault,
  ): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body as any,
        ContentType: contentType,
      }),
    );
  }

  /**
   * Verifica existência (lança erro se não existir).
   */
  async headObject(key: string, bucket = this.bucketDefault): Promise<void> {
    await this.client.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
  }

  /**
   * URL pré-assinada para GET
   */
  async getPresignedGetUrl(
    key: string,
    expiresSeconds = 3600,
    bucket = this.bucketDefault,
  ): Promise<string> {
    // valida se o objeto existe — se não existir, HeadObject lança
    await this.headObject(key, bucket);

    const cmd = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });

    const url = await getSignedUrl(this.client, cmd, { expiresIn: expiresSeconds });
    return url;
  }

  // Métodos adicionais para compatibilidade com os testes
  async uploadFile(file: Express.Multer.File, prefix: string = ''): Promise<any> {
    const key = `${prefix}${file.originalname}`;
    await this.putObject(key, file.buffer, file.mimetype);
    return {
      Key: key,
      Location: `${this.bucketDefault}/${key}`,
      Bucket: this.bucketDefault,
    };
  }

  async deleteFile(key: string, bucket = this.bucketDefault): Promise<any> {
    const { DeleteObjectCommand } = await import('@aws-sdk/client-s3');
    const result = await this.client.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
    return result;
  }

  async getSignedUrl(key: string, expiresIn?: number): Promise<string> {
    return this.getPresignedGetUrl(key, expiresIn);
  }

  async listFiles(prefix: string = '', bucket = this.bucketDefault): Promise<any[]> {
    const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    const result = await this.client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
      }),
    );
    return result.Contents || [];
  }
}
