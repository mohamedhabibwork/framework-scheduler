import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Application, ExecutionContext } from '@framework/core';
import { defineJob, JobWorker, QueueManager } from '@framework/queue';
import { z } from 'zod/v4';
import { defineSchedule } from '../src/definition';
import { Scheduler } from '../src/scheduler';
import { scheduleFirePayloadSchema } from '../src/types';

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) throw new Error('REDIS_URL is not set; run `bun run db:up`');

const tick = defineJob({
  name: 'sched.tick',
  version: 1,
  payload: z.object({ at: z.string() }),
  async handle(payload) {
    fired.push(payload.at);
    return null;
  },
});
const fired: string[] = [];

const everyMinute = defineSchedule({
  name: 'every-minute',
  cron: '* * * * *',
  dispatch: { kind: 'job', job: tick, payload: (now) => ({ at: now.toISOString() }) },
});

const workflowSchedule = defineSchedule({
  name: 'nightly-workflow',
  cron: '0 3 * * *',
  overlap: 'replace',
  dispatch: { kind: 'workflow', workflow: 'demo', input: { orderId: 'scheduled' } },
});

const workflowDispatches: { name: string; input: Record<string, unknown> }[] = [];

const prefix = `sched-test-${crypto.randomUUID()}`;
const queue = new QueueManager({ url: redisUrl, prefix, queue: 'sched-tests' });
const application = new Application({ config: {}, providers: [] });
const scheduler = new Scheduler(queue, {
  dispatchWorkflow: async (name, input) => {
    workflowDispatches.push({ name, input });
  },
});
let worker: JobWorker;

async function settleFire(count: number, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fired.length + workflowDispatches.length >= count) return;
    await Bun.sleep(25);
  }
  throw new Error('scheduled fire did not settle');
}

beforeAll(async () => {
  await application.boot();
  await scheduler.sync([everyMinute, workflowSchedule]);
  worker = new JobWorker(queue, application, [scheduler.fireJob([everyMinute, workflowSchedule]), tick], {
    concurrency: 2,
  });
  worker.start();
});

afterAll(async () => {
  await worker?.close();
  await scheduler.desync('every-minute');
  await scheduler.desync('nightly-workflow');
  await queue.queue.obliterate({ force: true });
  await queue.close();
  await application.shutdown();
});

describe('Scheduler (BullMQ job schedulers on real Redis)', () => {
  test('syncs timetables that BullMQ reports back', async () => {
    const descriptors = await scheduler.descriptors();
    const ids = descriptors.map((d) => d.id);
    expect(ids).toContain('schedule:every-minute');
    expect(ids).toContain('schedule:nightly-workflow');
  });

  test('a due fire dispatches the mapped job', async () => {
    await queue.queue.add('__schedule.fire', {
      v: 1,
      payload: { schedule: 'every-minute' },
      actor: { id: null, type: 'system' },
      correlationId: 'corr-sched',
    });
    await settleFire(1);
    expect(fired.length).toBe(1);
  });

  test('overlap skip holds the lock against a concurrent fire', async () => {
    // A fresh name so leftover locks from earlier fires cannot interfere.
    const name = `lock-${crypto.randomUUID().slice(0, 8)}`;
    const allowed = await scheduler.acquire(name, 'skip', 60);
    expect(allowed).toBe(true);
    const blocked = await scheduler.acquire(name, 'skip', 60);
    expect(blocked).toBe(false);
    await scheduler.release(name);
    expect(await scheduler.acquire(name, 'skip', 60)).toBe(true);
    await scheduler.release(name);
  });

  test('a due fire can dispatch a workflow through the deps', async () => {
    await queue.queue.add('__schedule.fire', {
      v: 1,
      payload: { schedule: 'nightly-workflow' },
      actor: { id: null, type: 'system' },
      correlationId: 'corr-sched',
    });
    await settleFire(2);
    expect(workflowDispatches).toMatchObject([{ name: 'demo', input: { orderId: 'scheduled' } }]);
  });

  test('desync removes the timetable', async () => {
    await scheduler.desync('nightly-workflow');
    const ids = (await scheduler.descriptors()).map((d) => d.id);
    expect(ids).not.toContain('schedule:nightly-workflow');
  });
});

describe('Scheduler primitives (deterministic)', () => {
  test('fire payloads are bounded non-empty names', () => {
    expect(scheduleFirePayloadSchema.safeParse({ schedule: '' }).success).toBe(false);
    expect(scheduleFirePayloadSchema.safeParse({ schedule: 'x'.repeat(121) }).success).toBe(false);
    expect(scheduleFirePayloadSchema.safeParse({ schedule: 'every-minute' }).success).toBe(true);
  });

  test('secondsUntilNextRun falls back to 60 for unknown timetables', async () => {
    expect(await scheduler.secondsUntilNextRun('ghost')).toBe(60);
  });

  test('skip locks clamp their TTL to one hour', async () => {
    const name = `ttl-${crypto.randomUUID().slice(0, 8)}`;
    expect(await scheduler.acquire(name, 'skip', 10_000)).toBe(true);
    const ttl = await queue.redis.ttl(`schedule:lock:${name}`);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(3_600);
    await scheduler.release(name);
  });

  test('replace overlap never touches Redis', async () => {
    const name = `replace-${crypto.randomUUID().slice(0, 8)}`;
    expect(await scheduler.acquire(name, 'replace', 60)).toBe(true);
    expect(await scheduler.acquire(name, 'replace', 60)).toBe(true);
    expect(await queue.redis.exists(`schedule:lock:${name}`)).toBe(0);
  });

  test('the fire job rejects unregistered schedules by code', async () => {
    const fire = scheduler.fireJob([everyMinute]);
    const context = {
      jobId: 'direct-invocation',
      attempt: 1,
      scope: null,
      execution: new ExecutionContext('job', 'corr-direct'),
      signal: new AbortController().signal,
      progress: async () => undefined,
    } as unknown as never;
    try {
      await (fire.handle as (payload: { schedule: string }, context: never) => Promise<unknown>)(
        { schedule: 'missing-schedule' },
        context,
      );
      throw new Error('expected a throw');
    } catch (error) {
      expect((error as { code?: string }).code).toBe('SCHEDULE_UNKNOWN');
      expect((error as Error).message).toContain('missing-schedule');
    }
  });
});
