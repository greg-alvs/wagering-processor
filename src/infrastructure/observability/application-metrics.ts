type MetricLabels = Record<string, string>;

interface MetricDefinition {
  name: string;
  help: string;
  type: 'counter' | 'gauge' | 'histogram';
  labelNames: string[];
  buckets?: number[];
}

interface HistogramSample {
  count: number;
  sum: number;
  buckets: Map<number, number>;
}

const DEFINITIONS: MetricDefinition[] = [
  {
    name: 'wager_transactions_total',
    help: 'Committed wagering transactions by final status.',
    type: 'counter',
    labelNames: ['status'],
  },
  {
    name: 'wager_duplicates_total',
    help: 'Duplicate messages and idempotent transaction replays.',
    type: 'counter',
    labelNames: ['source'],
  },
  {
    name: 'wager_retries_total',
    help: 'Scheduled retries by processing component.',
    type: 'counter',
    labelNames: ['component'],
  },
  {
    name: 'sqs_dlq_messages',
    help: 'Approximate number of messages visible in the wagering dead-letter queue.',
    type: 'gauge',
    labelNames: [],
  },
  {
    name: 'wallet_lock_conflicts_total',
    help: 'Wallet row-lock acquisitions taking at least 25 milliseconds.',
    type: 'counter',
    labelNames: [],
  },
  {
    name: 'wager_processing_duration_seconds',
    help: 'End-to-end transaction processing duration in seconds.',
    type: 'histogram',
    labelNames: ['source'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  },
  {
    name: 'wager_wallet_lock_wait_seconds',
    help: 'Time spent acquiring the wallet row lock.',
    type: 'histogram',
    labelNames: [],
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  },
  {
    name: 'outbox_lag_seconds',
    help: 'Age of the oldest unpublished outbox event.',
    type: 'gauge',
    labelNames: [],
  },
  {
    name: 'wallet_reconciliation_divergences_total',
    help: 'Total wallet reconciliation checks that found a balance mismatch.',
    type: 'counter',
    labelNames: [],
  },
];

export class ApplicationMetrics {
  private static readonly counters = new Map<string, number>();
  private static readonly gauges = new Map<string, number>();
  private static readonly histograms = new Map<string, HistogramSample>();

  static increment(name: string, labels: MetricLabels = {}, value = 1): void {
    const definition = this.definition(name, 'counter');
    this.validateLabels(definition, labels);
    const key = this.key(name, labels);
    this.counters.set(key, (this.counters.get(key) ?? 0) + value);
  }

  static setGauge(name: string, value: number, labels: MetricLabels = {}): void {
    const definition = this.definition(name, 'gauge');
    this.validateLabels(definition, labels);
    this.gauges.set(this.key(name, labels), value);
  }

  static observe(name: string, value: number, labels: MetricLabels = {}): void {
    const definition = this.definition(name, 'histogram');
    this.validateLabels(definition, labels);
    if (!Number.isFinite(value) || value < 0) {
      throw new Error(`Histogram ${name} requires a non-negative finite sample.`);
    }
    const key = this.key(name, labels);
    const sample = this.histograms.get(key) ?? {
      count: 0,
      sum: 0,
      buckets: new Map<number, number>(),
    };
    sample.count += 1;
    sample.sum += value;
    for (const bucket of definition.buckets ?? []) {
      if (value <= bucket) {
        sample.buckets.set(bucket, (sample.buckets.get(bucket) ?? 0) + 1);
      }
    }
    this.histograms.set(key, sample);
  }

  static render(additionalGauges: Record<string, number> = {}): string {
    const output: string[] = [];
    for (const definition of DEFINITIONS) {
      output.push(`# HELP ${definition.name} ${definition.help}`);
      output.push(`# TYPE ${definition.name} ${definition.type}`);
      for (const [key, value] of this.samplesFor(definition)) {
        output.push(`${key} ${value}`);
      }
      if (definition.name in additionalGauges) {
        output.push(`${definition.name} ${additionalGauges[definition.name]}`);
      }
    }
    return `${output.join('\n')}\n`;
  }

  private static samplesFor(definition: MetricDefinition): Array<[string, number]> {
    const samples: Array<[string, number]> = [];
    const source =
      definition.type === 'counter'
        ? this.counters
        : definition.type === 'gauge'
          ? this.gauges
          : this.histograms;

    for (const [key, value] of source) {
      if (!key.startsWith(`${definition.name}|`)) {
        continue;
      }
      const [, encodedLabels] = key.split('|', 2);
      const labels = this.decodeLabels(encodedLabels);
      const labelText = this.formatLabels(labels);
      if (definition.type !== 'histogram') {
        samples.push([`${definition.name}${labelText}`, value as number]);
        continue;
      }

      const histogram = value as HistogramSample;
      for (const bucket of definition.buckets ?? []) {
        samples.push([
          `${definition.name}_bucket${this.formatLabels({ ...labels, le: String(bucket) })}`,
          histogram.buckets.get(bucket) ?? 0,
        ]);
      }
      samples.push([
        `${definition.name}_bucket${this.formatLabels({ ...labels, le: '+Inf' })}`,
        histogram.count,
      ]);
      samples.push([`${definition.name}_sum${labelText}`, histogram.sum]);
      samples.push([`${definition.name}_count${labelText}`, histogram.count]);
    }

    return samples;
  }

  private static definition(name: string, type: MetricDefinition['type']): MetricDefinition {
    const definition = DEFINITIONS.find((item) => item.name === name && item.type === type);
    if (!definition) {
      throw new Error(`Metric ${name} is not registered as a ${type}.`);
    }
    return definition;
  }

  private static validateLabels(definition: MetricDefinition, labels: MetricLabels): void {
    const provided = Object.keys(labels).sort();
    const expected = [...definition.labelNames].sort();
    if (provided.length !== expected.length || provided.some((label, index) => label !== expected[index])) {
      throw new Error(`Metric ${definition.name} requires labels: ${expected.join(', ') || '(none)'}.`);
    }
  }

  private static key(name: string, labels: MetricLabels): string {
    return `${name}|${JSON.stringify(Object.entries(labels).sort(([left], [right]) => left.localeCompare(right)))}`;
  }

  private static decodeLabels(encodedLabels: string): MetricLabels {
    return Object.fromEntries(JSON.parse(encodedLabels) as Array<[string, string]>);
  }

  private static formatLabels(labels: MetricLabels): string {
    const entries = Object.entries(labels);
    if (!entries.length) {
      return '';
    }
    return `{${entries.sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}="${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"').replaceAll('\n', '\\n')}"`)
      .join(',')}}`;
  }
}
