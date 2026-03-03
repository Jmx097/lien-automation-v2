import { syncMissionControlEvent } from '../monitoring/mission-control-sync';

export function log(event: Record<string, unknown>): void {
  const payload = {
    timestamp: new Date().toISOString(),
    ...event,
  };

  console.log(JSON.stringify(payload));
  syncMissionControlEvent(payload);
}