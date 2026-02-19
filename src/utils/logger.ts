export function log(event: any) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    ...event
  }));
}