type BoundedSerialQueueAdmission<T> =
  | { accepted: true; completion: Promise<T> }
  | { accepted: false; reason: "overflow" | "sealed" };

type BoundedSerialQueueTask = {
  sequence: number;
  weight: number;
  run: () => unknown;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
};

type BoundedSerialQueueFlushWaiter = {
  sequence: number;
  resolve: () => void;
};

/**
 * Single-worker FIFO with bounded waiting work.
 *
 * The active task is owned separately from the waiting budget. Overflow seals
 * admission but preserves the already accepted prefix for flush and close.
 */
export class BoundedSerialQueue {
  private readonly pending: BoundedSerialQueueTask[] = [];
  private pendingWeight = 0;
  private active = false;
  private sealed = false;
  private overflowed = false;
  private acceptedSequence = 0;
  private settledSequence = 0;
  private readonly flushWaiters: BoundedSerialQueueFlushWaiter[] = [];

  constructor(
    private readonly options: {
      maxPendingCount: number;
      maxPendingWeight: number;
    },
  ) {
    if (!Number.isSafeInteger(options.maxPendingCount) || options.maxPendingCount < 0) {
      throw new Error("maxPendingCount must be a non-negative safe integer");
    }
    if (!Number.isFinite(options.maxPendingWeight) || options.maxPendingWeight < 0) {
      throw new Error("maxPendingWeight must be a non-negative finite number");
    }
  }

  get isIdle(): boolean {
    return !this.active && this.pending.length === 0;
  }

  get didOverflow(): boolean {
    return this.overflowed;
  }

  enqueue<T>(
    run: () => T | Promise<T>,
    options: { weight?: number } = {},
  ): BoundedSerialQueueAdmission<T> {
    if (this.sealed) {
      return { accepted: false, reason: "sealed" };
    }
    const weight = options.weight ?? 1;
    if (!Number.isFinite(weight) || weight < 0) {
      throw new Error("queue task weight must be a non-negative finite number");
    }
    if (
      this.active &&
      (this.pending.length >= this.options.maxPendingCount ||
        this.pendingWeight + weight > this.options.maxPendingWeight)
    ) {
      this.sealed = true;
      this.overflowed = true;
      return { accepted: false, reason: "overflow" };
    }

    let resolve!: (value: T | PromiseLike<T>) => void;
    let reject!: (reason: unknown) => void;
    const completion = new Promise<T>((accept, fail) => {
      resolve = accept;
      reject = fail;
    });
    const task: BoundedSerialQueueTask = {
      sequence: ++this.acceptedSequence,
      weight,
      run,
      resolve: (value) => resolve(value as T),
      reject,
    };
    if (this.active) {
      this.pending.push(task);
      this.pendingWeight += weight;
    } else {
      this.active = true;
      this.startTask(task);
    }
    return { accepted: true, completion };
  }

  seal(): void {
    this.sealed = true;
  }

  /**
   * Waits for the accepted prefix visible at call time.
   *
   * Later admissions do not extend this barrier, which keeps consult flushes
   * finite while close can seal first to drain the entire accepted prefix.
   */
  flush(): Promise<void> {
    const sequence = this.acceptedSequence;
    if (sequence <= this.settledSequence) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.flushWaiters.push({ sequence, resolve });
    });
  }

  private startTask(task: BoundedSerialQueueTask): void {
    void this.runTask(task);
  }

  private async runTask(task: BoundedSerialQueueTask): Promise<void> {
    try {
      task.resolve(await task.run());
    } catch (error) {
      task.reject(error);
    } finally {
      this.settledSequence = task.sequence;
      this.resolveFlushWaiters();
      const next = this.pending.shift();
      if (next) {
        this.pendingWeight -= next.weight;
        queueMicrotask(() => this.startTask(next));
      } else {
        this.active = false;
      }
    }
  }

  private resolveFlushWaiters(): void {
    for (let index = this.flushWaiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.flushWaiters[index];
      if (waiter && waiter.sequence <= this.settledSequence) {
        this.flushWaiters.splice(index, 1);
        waiter.resolve();
      }
    }
  }
}
