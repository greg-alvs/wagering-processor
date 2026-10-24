import { Controller, Get, Header } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('health/live')
  getLive() {
    return this.appService.getLive();
  }

  @Get('health/ready')
  getReady() {
    return this.appService.getReady();
  }

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  getMetrics(): Promise<string> {
    return this.appService.getMetrics();
  }
}
