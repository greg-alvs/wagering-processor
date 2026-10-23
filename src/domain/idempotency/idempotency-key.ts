export type IdempotencyRecordState = {
  id: string;
  key: string;
  payloadHash: string;
  responseHash: string;
  createdAt: Date | string;
  completedAt?: Date | string;
};

export class IdempotencyKeyRecord {
  private constructor(
    public readonly id: string,
    public readonly key: string,
    public readonly payloadHash: string,
    public readonly responseHash: string,
    public readonly createdAt: Date,
    public readonly completedAt?: Date,
  ) {}

  static create(props: {
    id: string;
    key: string;
    payloadHash: string;
    responseHash: string;
    createdAt?: Date;
    completedAt?: Date;
  }): IdempotencyKeyRecord {
    if (!props.id || !props.key || !props.payloadHash || !props.responseHash) {
      throw new Error('Idempotency record requires id, key, payloadHash and responseHash.');
    }

    return new IdempotencyKeyRecord(
      props.id,
      props.key,
      props.payloadHash,
      props.responseHash,
      props.createdAt ?? new Date(),
      props.completedAt,
    );
  }

  static rehydrate(state: IdempotencyRecordState): IdempotencyKeyRecord {
    return new IdempotencyKeyRecord(
      state.id,
      state.key,
      state.payloadHash,
      state.responseHash,
      new Date(state.createdAt),
      state.completedAt ? new Date(state.completedAt) : undefined,
    );
  }
}
