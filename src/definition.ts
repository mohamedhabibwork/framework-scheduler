import type { JsonObject } from '@framework/core';
import type { ScheduleDefinition } from './types';

/** Declares a cron schedule; identity helper preserving type inference. */
export const defineSchedule = <TPayload extends JsonObject>(
  definition: ScheduleDefinition<TPayload>,
): ScheduleDefinition<TPayload> => definition;
