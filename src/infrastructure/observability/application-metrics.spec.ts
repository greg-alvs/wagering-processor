import { describe, expect, test } from 'bun:test';
import { ApplicationMetrics } from './application-metrics';

describe('ApplicationMetrics', () => {
  test('renders bounded counters, escaped labels, and cumulative histogram buckets', () => {
    ApplicationMetrics.increment('wager_transactions_total', { status: 'PROCESSED' });
    ApplicationMetrics.increment('wager_duplicates_total', { source: 'inbox' });
    ApplicationMetrics.increment('wager_retries_total', { component: 'outbox_publisher' });
    ApplicationMetrics.observe('wager_processing_duration_seconds', 0.02, { source: 'http' });

    const metrics = ApplicationMetrics.render();
    expect(metrics).toContain('wager_transactions_total{status="PROCESSED"}');
    expect(metrics).toContain('wager_duplicates_total{source="inbox"}');
    expect(metrics).toContain('wager_retries_total{component="outbox_publisher"}');
    expect(metrics).toMatch(/wager_processing_duration_seconds_bucket\{le="0\.025",source="http"\} [1-9]\d*/);
    expect(metrics).toMatch(/wager_processing_duration_seconds_bucket\{le="\+Inf",source="http"\} [1-9]\d*/);
    expect(metrics).toMatch(/wager_processing_duration_seconds_count\{source="http"\} [1-9]\d*/);
  });
});
