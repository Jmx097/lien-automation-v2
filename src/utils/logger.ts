export type LogEvent = Record<string, unknown>;

export function log(event: LogEvent) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    ...event,
  }));
}
