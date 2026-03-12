import { log } from '../utils/logger';

const DEFAULT_LEAD_ALERT_EMAIL_TO = 'antigravity1@timberlinetax.com';

export interface NewLeadsNotification {
  site: string;
  run_id: string;
  idempotency_key: string;
  new_master_row_count: number;
  master_tab_title: string;
  target_spreadsheet_id_suffix?: string;
}

export interface NotificationDeliveryResult {
  attempted: boolean;
  delivered: boolean;
}

function getLeadAlertEmailTo(): string {
  return process.env.LEAD_ALERT_EMAIL_TO?.trim() || DEFAULT_LEAD_ALERT_EMAIL_TO;
}

function getLeadAlertEmailFrom(): string | undefined {
  const value = process.env.LEAD_ALERT_EMAIL_FROM?.trim();
  return value ? value : undefined;
}

function getLeadAlertWebhookUrl(): string | undefined {
  const value = process.env.LEAD_ALERT_WEBHOOK_URL?.trim();
  return value ? value : undefined;
}

function getLeadAlertResendApiKey(): string | undefined {
  const value = process.env.LEAD_ALERT_RESEND_API_KEY?.trim();
  return value ? value : undefined;
}

export function buildLeadNotificationHtml(notification: NewLeadsNotification): string {
  const targetSuffix = notification.target_spreadsheet_id_suffix ?? 'unknown';

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>New leads</title>
  </head>
  <body style="margin:0;padding:24px;background:#f4f1e8;font-family:Georgia,'Times New Roman',serif;color:#1e1b18;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
      <tr>
        <td align="center">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;border-collapse:collapse;background:#fffdf8;border:1px solid #d8d0c2;">
            <tr>
              <td style="padding:28px 32px 12px 32px;background:linear-gradient(135deg,#17324d 0%,#355f7a 100%);color:#f8f4ec;">
                <div style="font-size:12px;letter-spacing:0.18em;text-transform:uppercase;opacity:0.8;">Lien Automation</div>
                <div style="margin-top:12px;font-size:30px;line-height:1.1;font-weight:bold;">New leads!</div>
              </td>
            </tr>
            <tr>
              <td style="padding:28px 32px;">
                <p style="margin:0 0 18px 0;font-size:16px;line-height:1.6;">New director-ready rows were published to <strong>${escapeHtml(notification.master_tab_title)}</strong>.</p>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;margin:0 0 22px 0;">
                  <tr>
                    <td style="padding:18px;background:#f7efe1;border:1px solid #eadbc2;">
                      <div style="font-size:12px;text-transform:uppercase;letter-spacing:0.12em;color:#6a5a45;">New master records</div>
                      <div style="margin-top:8px;font-size:34px;font-weight:bold;color:#17324d;">${notification.new_master_row_count}</div>
                    </td>
                  </tr>
                </table>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;font-size:14px;line-height:1.6;">
                  <tr>
                    <td style="padding:8px 0;color:#6a5a45;width:170px;">Site</td>
                    <td style="padding:8px 0;">${escapeHtml(notification.site)}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0;color:#6a5a45;">Run ID</td>
                    <td style="padding:8px 0;">${escapeHtml(notification.run_id)}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0;color:#6a5a45;">Idempotency Key</td>
                    <td style="padding:8px 0;">${escapeHtml(notification.idempotency_key)}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0;color:#6a5a45;">Target Sheet</td>
                    <td style="padding:8px 0;">...${escapeHtml(targetSuffix)}</td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
}

function buildLeadNotificationText(notification: NewLeadsNotification): string {
  return `New leads!\n\n${notification.new_master_row_count} new Master records were published for ${notification.site}.\nRun ID: ${notification.run_id}\nIdempotency Key: ${notification.idempotency_key}\nTarget Sheet: ...${notification.target_spreadsheet_id_suffix ?? 'unknown'}`;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function sendViaWebhook(notification: NewLeadsNotification): Promise<NotificationDeliveryResult> {
  const webhook = getLeadAlertWebhookUrl();
  if (!webhook) return { attempted: false, delivered: false };

  const payload = {
    to: getLeadAlertEmailTo(),
    subject: 'New leads!',
    text: buildLeadNotificationText(notification),
    html: buildLeadNotificationHtml(notification),
    ...notification,
  };

  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const body = await res.text();
      log({ stage: 'lead_alert_failed', transport: 'webhook', status: res.status, response: body, to: payload.to });
      return { attempted: true, delivered: false };
    }

    return { attempted: true, delivered: true };
  } catch (err: any) {
    log({ stage: 'lead_alert_error', transport: 'webhook', error: String(err?.message ?? err), to: payload.to });
    return { attempted: true, delivered: false };
  }
}

async function sendViaResend(notification: NewLeadsNotification): Promise<NotificationDeliveryResult> {
  const apiKey = getLeadAlertResendApiKey();
  const from = getLeadAlertEmailFrom();
  if (!apiKey || !from) return { attempted: false, delivered: false };

  const payload = {
    from,
    to: [getLeadAlertEmailTo()],
    subject: 'New leads!',
    text: buildLeadNotificationText(notification),
    html: buildLeadNotificationHtml(notification),
  };

  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const body = await res.text();
      log({ stage: 'lead_alert_failed', transport: 'resend', status: res.status, response: body, to: payload.to[0] });
      return { attempted: true, delivered: false };
    }

    return { attempted: true, delivered: true };
  } catch (err: any) {
    log({ stage: 'lead_alert_error', transport: 'resend', error: String(err?.message ?? err), to: payload.to[0] });
    return { attempted: true, delivered: false };
  }
}

export async function sendNewLeadsNotification(notification: NewLeadsNotification): Promise<NotificationDeliveryResult> {
  const webhookResult = await sendViaWebhook(notification);
  if (webhookResult.attempted) return webhookResult;

  const resendResult = await sendViaResend(notification);
  if (resendResult.attempted) return resendResult;

  log({
    stage: 'lead_alert_log_only',
    to: getLeadAlertEmailTo(),
    subject: 'New leads!',
    new_master_row_count: notification.new_master_row_count,
    site: notification.site,
    run_id: notification.run_id,
    idempotency_key: notification.idempotency_key,
  });
  return { attempted: false, delivered: false };
}
