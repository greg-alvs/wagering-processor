export class AppService {
  getLive() {
    return { status: 'ok', timestamp: new Date().toISOString() };
  }

  getReady() {
    return {
      status: 'ok',
      checks: {
        postgres: 'not-configured-yet',
        sqs: 'not-configured-yet',
      },
      timestamp: new Date().toISOString(),
    };
  }
}
