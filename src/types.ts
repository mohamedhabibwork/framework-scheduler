import type { ExecutionContext, JsonObject, JsonValue } from '@framework/core';
import type { JobDefinition } from '@framework/queue';
import { z } from 'zod/v4';

/**
 * What a due schedule dispatches: a registered job (with a payload builder),
 * or a registered workflow (with its input).
 */
export type ScheduleTarget<TPayload extends JsonObject = JsonObject> =
  | {
      readonly kind: 'job';
      readonly job: JobDefinition<TPayload, JsonValue>;
      readonly payload?: (now: Date) => TPayload;
    }
  | {
      readonly kind: 'workflow';
      readonly workflow: string;
      readonly input?: JsonObject | ((now: Date) => JsonObject);
    };

/**
 * A cron schedule registered on the queue. BullMQ owns the timetable
 * (`upsertJobScheduler`), the framework owns dispatch and the distributed
 * overlap lock, so several scheduler processes can run safely side by side.
 */
export interface ScheduleDefinition<TPayload extends JsonObject = JsonObject> {
  readonly name: string;
  /** 5-field cron expression, evaluated by BullMQ. */
  readonly cron: string;
  readonly timezone?: string;
  /** `skip` (default) holds a Redis lock until the next due run; `replace` allows overlaps. */
  readonly overlap?: 'skip' | 'replace';
  readonly dispatch: ScheduleTarget<TPayload>;
}

export const scheduleFirePayloadSchema = z.object({ schedule: z.string().min(1).max(120) });

export type ScheduleFirePayload = z.output<typeof scheduleFirePayloadSchema>;

/** Resolves dispatch targets for fire jobs (wired by the app bootstrap). */
export interface SchedulerDeps {
  /** Dispatches a registered workflow by name. */
  readonly dispatchWorkflow?: (name: string, input: JsonObject, context: ExecutionContext) => Promise<unknown>;
}

/** Minimal view of BullMQ's scheduler descriptors used for overlap TTLs. */
export interface SchedulerDescriptor {
  readonly id: string;
  readonly nextRun?: number | null;
}
