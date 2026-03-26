#!/usr/bin/env node

const DAY_MAP = {
  SU: '0',
  MO: '1',
  TU: '2',
  WE: '3',
  TH: '4',
  FR: '5',
  SA: '6',
};

function parseInteger(name, rawValue) {
  const value = Number.parseInt(String(rawValue ?? '').trim(), 10);
  if (!Number.isInteger(value)) {
    throw new Error(`${name} must be an integer, received "${rawValue}"`);
  }
  return value;
}

function normalizeWeeklyDays(rawValue, name) {
  const tokens = String(rawValue ?? '')
    .split(',')
    .map((token) => token.trim().toUpperCase())
    .filter(Boolean);

  if (tokens.length === 0) {
    throw new Error(`${name} must include at least one weekday token`);
  }

  const mapped = tokens.map((token) => {
    const cronDay = DAY_MAP[token];
    if (!cronDay) {
      throw new Error(`${name} contains unsupported weekday token "${token}"`);
    }
    return cronDay;
  });

  return mapped.join(',');
}

function normalizeMinuteOfDay(totalMinutes) {
  const minutesPerDay = 24 * 60;
  return ((totalMinutes % minutesPerDay) + minutesPerDay) % minutesPerDay;
}

function buildCronExpression(hour, minute, weeklyDays) {
  return `${minute} ${hour} * * ${normalizeWeeklyDays(weeklyDays, 'weekly_days')}`;
}

function buildSiteSpec(options) {
  const {
    jobPrefix,
    site,
    siteSlug,
    slot,
    timeZone,
    weeklyDays,
    runHour,
    runMinute,
    triggerLeadMinutes = 0,
  } = options;

  const leadMinutes = parseInteger(`${site}_${slot}_trigger_lead_minutes`, triggerLeadMinutes);
  const targetHour = parseInteger(`${site}_${slot}_run_hour`, runHour);
  const targetMinute = parseInteger(`${site}_${slot}_run_minute`, runMinute);
  const normalizedMinutes = normalizeMinuteOfDay(targetHour * 60 + targetMinute - leadMinutes);
  const scheduledHour = Math.floor(normalizedMinutes / 60);
  const scheduledMinute = normalizedMinutes % 60;

  return {
    site,
    slot,
    jobName: `${jobPrefix}-${siteSlug}-${slot}`,
    schedule: buildCronExpression(scheduledHour, scheduledMinute, weeklyDays),
    timeZone,
    path: '/schedule/run',
    body: { site, slot },
    scheduledHour,
    scheduledMinute,
    targetHour,
    targetMinute,
    triggerLeadMinutes: leadMinutes,
    weeklyDays,
  };
}

function buildMaintenanceSpec(options) {
  const {
    jobPrefix,
    jobSuffix,
    schedule,
    timeZone,
    path,
    body = {},
  } = options;

  return {
    site: 'maricopa_recorder',
    slot: 'maintenance',
    jobName: `${jobPrefix}-${jobSuffix}`,
    schedule,
    timeZone,
    path,
    body,
  };
}

function collectSchedulerJobSpecs(env = process.env) {
  const jobPrefix = env.JOB_NAME ?? 'lien-scraper-schedule-run';
  const maintenanceTimeZone = env.MARICOPA_MAINTENANCE_TIMEZONE ?? 'America/Denver';
  const maintenanceDays = env.MARICOPA_MAINTENANCE_DAYS ?? 'SU,MO,TU,WE,TH,FR,SA';
  const proofTimeZone = env.TRI_SITE_PROOF_TIMEZONE ?? 'America/Denver';
  const proofDays = env.TRI_SITE_PROOF_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR';

  return {
    jobPrefix,
    specs: [
      buildSiteSpec({
        jobPrefix,
        site: 'ca_sos',
        siteSlug: 'ca-sos',
        slot: 'morning',
        timeZone: env.SCHEDULE_CA_SOS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_CA_SOS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_CA_SOS_MORNING_RUN_HOUR ?? '10',
        runMinute: env.SCHEDULE_CA_SOS_MORNING_RUN_MINUTE ?? '0',
        triggerLeadMinutes: env.SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES ?? '180',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'ca_sos',
        siteSlug: 'ca-sos',
        slot: 'afternoon',
        timeZone: env.SCHEDULE_CA_SOS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_CA_SOS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR ?? '14',
        runMinute: env.SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE ?? '0',
        triggerLeadMinutes: env.SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES ?? '180',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'ca_sos',
        siteSlug: 'ca-sos',
        slot: 'evening',
        timeZone: env.SCHEDULE_CA_SOS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_CA_SOS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_CA_SOS_EVENING_RUN_HOUR ?? '22',
        runMinute: env.SCHEDULE_CA_SOS_EVENING_RUN_MINUTE ?? '0',
        triggerLeadMinutes: env.SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES ?? '180',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'maricopa_recorder',
        siteSlug: 'maricopa-recorder',
        slot: 'morning',
        timeZone: env.SCHEDULE_MARICOPA_RECORDER_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_HOUR ?? '10',
        runMinute: env.SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_MINUTE ?? '0',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'maricopa_recorder',
        siteSlug: 'maricopa-recorder',
        slot: 'afternoon',
        timeZone: env.SCHEDULE_MARICOPA_RECORDER_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_HOUR ?? '14',
        runMinute: env.SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_MINUTE ?? '0',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'maricopa_recorder',
        siteSlug: 'maricopa-recorder',
        slot: 'evening',
        timeZone: env.SCHEDULE_MARICOPA_RECORDER_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_HOUR ?? '22',
        runMinute: env.SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_MINUTE ?? '0',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'nyc_acris',
        siteSlug: 'nyc-acris',
        slot: 'morning',
        timeZone: env.SCHEDULE_NYC_ACRIS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR ?? '10',
        runMinute: env.SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE ?? '0',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'nyc_acris',
        siteSlug: 'nyc-acris',
        slot: 'afternoon',
        timeZone: env.SCHEDULE_NYC_ACRIS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR ?? '14',
        runMinute: env.SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE ?? '0',
      }),
      buildSiteSpec({
        jobPrefix,
        site: 'nyc_acris',
        siteSlug: 'nyc-acris',
        slot: 'evening',
        timeZone: env.SCHEDULE_NYC_ACRIS_TIMEZONE ?? 'America/Denver',
        weeklyDays: env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS ?? 'MO,TU,WE,TH,FR',
        runHour: env.SCHEDULE_NYC_ACRIS_EVENING_RUN_HOUR ?? '22',
        runMinute: env.SCHEDULE_NYC_ACRIS_EVENING_RUN_MINUTE ?? '0',
      }),
      buildMaintenanceSpec({
        jobPrefix,
        jobSuffix: 'maricopa-session-refresh-morning',
        schedule: buildCronExpression(
          parseInteger('MARICOPA_SESSION_REFRESH_MORNING_RUN_HOUR', env.MARICOPA_SESSION_REFRESH_MORNING_RUN_HOUR ?? '8'),
          parseInteger('MARICOPA_SESSION_REFRESH_MORNING_RUN_MINUTE', env.MARICOPA_SESSION_REFRESH_MORNING_RUN_MINUTE ?? '30'),
          maintenanceDays,
        ),
        timeZone: maintenanceTimeZone,
        path: '/maintenance/maricopa/session-refresh',
      }),
      buildMaintenanceSpec({
        jobPrefix,
        jobSuffix: 'maricopa-session-refresh-evening',
        schedule: buildCronExpression(
          parseInteger('MARICOPA_SESSION_REFRESH_EVENING_RUN_HOUR', env.MARICOPA_SESSION_REFRESH_EVENING_RUN_HOUR ?? '20'),
          parseInteger('MARICOPA_SESSION_REFRESH_EVENING_RUN_MINUTE', env.MARICOPA_SESSION_REFRESH_EVENING_RUN_MINUTE ?? '30'),
          maintenanceDays,
        ),
        timeZone: maintenanceTimeZone,
        path: '/maintenance/maricopa/session-refresh',
      }),
      buildMaintenanceSpec({
        jobPrefix,
        jobSuffix: 'maricopa-discover',
        schedule: buildCronExpression(
          parseInteger('MARICOPA_DISCOVERY_RUN_HOUR', env.MARICOPA_DISCOVERY_RUN_HOUR ?? '8'),
          parseInteger('MARICOPA_DISCOVERY_RUN_MINUTE', env.MARICOPA_DISCOVERY_RUN_MINUTE ?? '45'),
          maintenanceDays,
        ),
        timeZone: maintenanceTimeZone,
        path: '/maintenance/maricopa/discover',
      }),
      {
        site: 'tri_site',
        slot: 'proof_export',
        jobName: `${jobPrefix}-proof-export-nightly`,
        schedule: buildCronExpression(
          parseInteger('TRI_SITE_PROOF_RUN_HOUR', env.TRI_SITE_PROOF_RUN_HOUR ?? '23'),
          parseInteger('TRI_SITE_PROOF_RUN_MINUTE', env.TRI_SITE_PROOF_RUN_MINUTE ?? '15'),
          proofDays,
        ),
        timeZone: proofTimeZone,
        path: '/schedule/proof/export',
        body: {},
      },
    ],
  };
}

if (require.main === module) {
  process.stdout.write(`${JSON.stringify(collectSchedulerJobSpecs())}\n`);
}

module.exports = {
  collectSchedulerJobSpecs,
  normalizeWeeklyDays,
};
