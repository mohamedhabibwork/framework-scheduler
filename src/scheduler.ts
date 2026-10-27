import { FrameworkError, type JsonObject } from '@framework/core';
import type { JobContext, JobDefinition, QueueManager } from '@framework/queue';
import { type ScheduleDefinition, type SchedulerDeps, scheduleFirePayloadSchema } from './types';

export const SCHEDULE_FIRE_JOB = '__schedule.fire';
export const SCHEDULE_LOCK_PREFIX = 'schedule:lock';

const MAX_LOCK_TTL_SECONDS = 3_600;

/** Shape of BullMQ's `getJobSchedulers()` rows we rely on. */
interface RawSchedulerDescriptor {
  readonly key: string;
  readonly next?: number | null;
}

export interface SchedulerDescriptor {
  readonly id: string;
  readonly nextRun: number | null;
}

/**
 * Registers cron timetables on BullMQ and turns due fires into dispatched
 * jobs or workflows. Runs inside the dedicated scheduler process
 * (`bin/scheduler.ts`); the actual work executes on the regular workers.
 */
export class Scheduler {
  constructor(
    private readonly queue: QueueManager,
    private readonly deps: SchedulerDeps = {},
  ) {}

  /** Creates or updates the BullMQ job scheduler for each definition. */
  async sync(
    // oxlint-disable-next-line typescript/no-explicit-any -- heterogeneous schedules, erased like JobList
    schedules: readonly ScheduleDefinition<any>[],
  ): Promise<void> {
    for (const schedule of schedules) {
      await this.queue.queue.upsertJobScheduler(
        `schedule:${schedule.name}`,
        {
          pattern: schedule.cron,
          ...(schedule.timezone ? { tz: schedule.timezone } : {}),
        },
        {
          // The fire template is a proper JobEnvelope so the regular JobWorker consumes it.
          name: SCHEDULE_FIRE_JOB,
          data: {
            v: 1,
            payload: { schedule: schedule.name },
            actor: { id: null, type: 'system' },
            correlationId: `schedule:${schedule.name}`,
          },
        },
      );
    }
  }

  /** Removes a schedule's timetable and lock. */
  async desync(name: string): Promise<void> {
    await this.queue.queue.removeJobScheduler(`schedule:${name}`);
    await this.queue.redis.del(`${SCHEDULE_LOCK_PREFIX}:${name}`);
  }

  /** Enqueues one fire job for a schedule (used by `schedule:run` / `schedule:test`). */
  async fireNow(name: string, correlationId: string): Promise<void> {
    await this.queue.queue.add(
      SCHEDULE_FIRE_JOB,
      {
        v: 1,
        payload: { schedule: name },
        actor: { id: null, type: 'system' },
        correlationId,
      },
      { removeOnComplete: { count: 1_000 }, removeOnFail: { count: 5_000 } },
    );
  }

  /** BullMQ's registered timetables (CLI listing). */
  async descriptors(): Promise<SchedulerDescriptor[]> {
    const raw = (await this.queue.queue.getJobSchedulers()) as unknown as readonly RawSchedulerDescriptor[];
    return raw.map((descriptor) => ({ id: descriptor.key, nextRun: descriptor.next ?? null }));
  }

  /** The distributed overlap lock: true when this fire may proceed. */
  async acquire(name: string, overlap: 'skip' | 'replace', ttlSeconds: number): Promise<boolean> {
    if (overlap === 'replace') return true;
    const ttl = Math.max(1, Math.min(Math.round(ttlSeconds), MAX_LOCK_TTL_SECONDS));
    const key = `${SCHEDULE_LOCK_PREFIX}:${name}`;
    const result = await this.queue.redis.set(key, '1', 'EX', ttl, 'NX');
    return result === 'OK';
  }

  async release(name: string): Promise<void> {
    await this.queue.redis.del(`${SCHEDULE_LOCK_PREFIX}:${name}`);
  }

  /** Time until BullMQ runs this schedule next (bounds the overlap lock). */
  async secondsUntilNextRun(name: string): Promise<number> {
    const descriptors = await this.descriptors();
    const descriptor = descriptors.find((candidate) => candidate.id === `schedule:${name}`);
    if (!descriptor?.nextRun) return 60;
    return Math.max(1, (descriptor.nextRun - Date.now()) / 1_000);
  }

  /**
   * The internal job BullMQ fires on the timetable. The handler dispatches the
   * mapped job or workflow with idempotency `schedule:{name}:{fireJobId}`, so
   * duplicate deliveries collapse, and holds the overlap lock until the next
   * due run when `overlap: 'skip'`.
   */
  fireJob(
    // oxlint-disable-next-line typescript/no-explicit-any -- heterogeneous schedules, erased like JobList
    schedules: readonly ScheduleDefinition<any>[],
  ): JobDefinition<{ schedule: string }, null> {
    return {
      name: SCHEDULE_FIRE_JOB,
      version: 1,
      payload: scheduleFirePayloadSchema,
      attempts: 1,
      timeoutMs: 60_000,
      handle: async (payload: { schedule: string }, context: JobContext) => {
        const schedule = schedules.find((candidate) => candidate.name === payload.schedule);
        if (!schedule) {
          throw new FrameworkError('SCHEDULE_UNKNOWN', `Schedule "${payload.schedule}" is not registered`);
        }
        const allowed = await this.acquire(
          schedule.name,
          schedule.overlap ?? 'skip',
          await this.secondsUntilNextRun(schedule.name),
        );
        if (!allowed) return null;

        const now = new Date();
        const target = schedule.dispatch;
        if (target.kind === 'job') {
          const jobPayload = (target.payload?.(now) ?? {}) as JsonObject;
          await this.queue.dispatch(target.job, jobPayload, context.execution, {
            jobId: `schedule-${schedule.name}-${context.jobId}`,
          });
        } else {
          const input = typeof target.input === 'function' ? target.input(now) : (target.input ?? {});
          await this.deps.dispatchWorkflow?.(target.workflow, input, context.execution);
        }
        return null;
      },
    };
  }
}
