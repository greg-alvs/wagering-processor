import { describe, expect, test } from 'bun:test';
import { Pool } from 'pg';
import { WageringProcessorService } from './wagering-processor.service';

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: 'postgres',
  database: 'wagering',
});

describe('WageringProcessorService', () => {
  test('service initializes with postgres pool', async () => {
    const service = new WageringProcessorService(pool);
    expect(service).toBeInstanceOf(WageringProcessorService);
  });
});
