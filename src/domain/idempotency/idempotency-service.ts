import { canonicalizeJson } from './canonical-payload';
import { FailureCode } from '../wagering/wager-transaction';

export type IdempotencyDecision =
  | { type: 'OK'; payloadHash: string }
  | { type: 'CONFLICT'; reason: string }
  | { type: 'REPLAY'; payloadHash: string }
  | { type: 'UNKNOWN'; payloadHash: string };

export class IdempotencyService {
  static computePayloadHash(payload: unknown): string {
    return canonicalizeJson(payload);
  }

  static evaluate(payload: unknown, existingPayloadHash?: string): IdempotencyDecision {
    const payloadHash = this.computePayloadHash(payload);

    if (!existingPayloadHash) {
      return { type: 'UNKNOWN', payloadHash };
    }

    if (existingPayloadHash === payloadHash) {
      return { type: 'REPLAY', payloadHash };
    }

    return {
      type: 'CONFLICT',
      reason: `Payload hash mismatch for same idempotency key. Expected ${existingPayloadHash}, received ${payloadHash}.`,
    };
  }

  static toFailureCode(result: IdempotencyDecision): FailureCode | undefined {
    if (result.type === 'CONFLICT') return FailureCode.DuplicatePayload;
    return undefined;
  }
}
