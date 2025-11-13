import { Controller, Get } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiOkResponse } from '@nestjs/swagger';
import { AppService } from './app.service';

@ApiTags('Sistema')
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  @ApiOperation({ 
    summary: 'Health check da API',
    description: 'Endpoint para verificar se a API está funcionando corretamente'
  })
  @ApiOkResponse({
    description: 'API está funcionando',
    example: 'Hello World!'
  })
  getHello(): string {
    return this.appService.getHello();
  }
}
