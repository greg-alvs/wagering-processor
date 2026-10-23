export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private readonly minorUnits: bigint;

  private constructor(minorUnits: bigint, public readonly currency: string) {
    this.minorUnits = minorUnits;
  }

  static from(props: MoneyProps): Money {
    const currency = Money.normalizeCurrency(props.currency);
    const amount = Money.parseInputAmount(props.amount, currency);
    return Money.fromMinorUnits(amount, currency);
  }

  static zero(currency: string): Money {
    return new Money(0n, Money.normalizeCurrency(currency));
  }

  static fromMinorUnits(minorUnits: bigint, currency: string): Money {
    return new Money(minorUnits, Money.normalizeCurrency(currency));
  }

  get value(): bigint {
    return this.minorUnits;
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits === other.minorUnits;
  }

  toJSON(): MoneyProps {
    return {
      amount: this.toString(),
      currency: this.currency,
    };
  }

  toString(): string {
    const negative = this.minorUnits < 0n;
    const absolute = negative ? -this.minorUnits : this.minorUnits;
    const whole = absolute / 100n;
    const cents = absolute % 100n;

    return `${negative ? '-' : ''}${whole.toString()}.${String(cents).padStart(2, '0')}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new Error(`Currency mismatch: ${this.currency} != ${other.currency}`);
    }
  }

  private static normalizeCurrency(currency: string): string {
    if (typeof currency !== 'string' || currency.trim() === '') {
      throw new Error('Currency is required.');
    }

    const normalized = currency.trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(normalized)) {
      throw new Error(`Invalid currency code: ${currency}`);
    }

    return normalized;
  }

  private static parseInputAmount(amount: string, currency: string): bigint {
    if (typeof amount !== 'string' || amount.trim() === '') {
      throw new Error(`Invalid amount for ${currency}: value is required.`);
    }

    const trimmed = amount.trim();
    if (!Number.isFinite(Number(trimmed)) || /[eE]/.test(trimmed)) {
      throw new Error(`Invalid amount for ${currency}: not a valid decimal string.`);
    }

    if (trimmed.startsWith('+') || trimmed.startsWith('-')) {
      throw new Error(`Invalid amount for ${currency}: negative values are not allowed for input.`);
    }

    const [wholePart, decimalPart = ''] = trimmed.split('.');
    if (!/^\d+$/.test(wholePart)) {
      throw new Error(`Invalid amount for ${currency}: whole part must be numeric.`);
    }

    if (decimalPart.length > 2) {
      throw new Error(`Invalid amount for ${currency}: more than 2 decimal places are not allowed.`);
    }

    if (decimalPart.length > 0 && !/^\d+$/.test(decimalPart)) {
      throw new Error(`Invalid amount for ${currency}: decimal part must be numeric.`);
    }

    const normalizedWhole = BigInt(wholePart);
    const normalizedFraction = BigInt(decimalPart.padEnd(2, '0'));
    return normalizedWhole * 100n + normalizedFraction;
  }
}
