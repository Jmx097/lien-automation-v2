import { describe, expect, it } from 'vitest';

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { collectSchedulerJobSpecs } = require('../../scripts/cloud/scheduler-job-specs.js');

describe('cloud scheduler job specs', () => {
  it('emits 13 jobs including scrape, Maricopa maintenance, and proof export', () => {
    const payload = collectSchedulerJobSpecs({
      JOB_NAME: 'lien-scraper-schedule-run',
      SCHEDULE_CA_SOS_TIMEZONE: 'America/Denver',
      SCHEDULE_CA_SOS_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
      SCHEDULE_CA_SOS_MORNING_RUN_HOUR: '10',
      SCHEDULE_CA_SOS_MORNING_RUN_MINUTE: '0',
      SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR: '14',
      SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE: '0',
      SCHEDULE_CA_SOS_EVENING_RUN_HOUR: '22',
      SCHEDULE_CA_SOS_EVENING_RUN_MINUTE: '0',
      SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES: '180',
      SCHEDULE_MARICOPA_RECORDER_TIMEZONE: 'America/Denver',
      SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
      SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_HOUR: '10',
      SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_MINUTE: '0',
      SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_HOUR: '14',
      SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_MINUTE: '0',
      SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_HOUR: '22',
      SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_MINUTE: '0',
      SCHEDULE_NYC_ACRIS_TIMEZONE: 'America/Denver',
      SCHEDULE_NYC_ACRIS_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
      SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR: '10',
      SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE: '0',
      SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR: '14',
      SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE: '0',
      SCHEDULE_NYC_ACRIS_EVENING_RUN_HOUR: '22',
      SCHEDULE_NYC_ACRIS_EVENING_RUN_MINUTE: '0',
    });

    expect(payload.specs).toHaveLength(13);
    expect(payload.specs).toEqual(expect.arrayContaining([
      expect.objectContaining({
        jobName: 'lien-scraper-schedule-run-ca-sos-morning',
        schedule: '0 7 * * 1,2,3,4,5',
        path: '/schedule/run',
        body: { site: 'ca_sos', slot: 'morning' },
      }),
      expect.objectContaining({
        jobName: 'lien-scraper-schedule-run-maricopa-session-refresh-morning',
        schedule: '30 8 * * 0,1,2,3,4,5,6',
        path: '/maintenance/maricopa/session-refresh',
      }),
      expect.objectContaining({
        jobName: 'lien-scraper-schedule-run-maricopa-discover',
        schedule: '45 8 * * 0,1,2,3,4,5,6',
        path: '/maintenance/maricopa/discover',
      }),
      expect.objectContaining({
        jobName: 'lien-scraper-schedule-run-proof-export-nightly',
        schedule: '15 23 * * 1,2,3,4,5',
        path: '/schedule/proof/export',
      }),
    ]));
  });
});
