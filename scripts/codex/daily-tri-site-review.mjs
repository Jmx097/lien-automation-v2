#!/usr/bin/env node
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const ANALYSIS_PROMPT =
  'analyze the workspace and diagnose how strong California, NYC Acris and Mericopa are all pulling 3x a day with 95% accuracy';

const model = process.env.OPENAI_MODEL || 'gpt-5.3-codex';
const apiKey = process.env.OPENAI_API_KEY;
const outputDir = process.env.CODEX_REPORT_DIR || path.join(process.cwd(), 'tmp', 'codex-reports');

async function requestCodex(prompt, metadata = {}) {
  if (!apiKey) {
    throw new Error('Missing OPENAI_API_KEY. Set it in the workflow secrets before running this automation.');
  }
  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model,
      input: prompt,
      metadata
    })
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Codex API request failed (${response.status}): ${errorBody}`);
  }

  const payload = await response.json();
  const text = extractResponseText(payload);

  if (!text) {
    const statusSuffix = typeof payload.status === 'string' ? ` Response status: ${payload.status}.` : '';
    throw new Error(`Codex API response did not include any text output.${statusSuffix}`);
  }

  return {
    id: payload.id,
    text,
    raw: payload
  };
}

export function extractResponseText(payload) {
  if (typeof payload?.output_text === 'string') {
    const text = payload.output_text.trim();

    if (text) {
      return text;
    }
  }

  if (!Array.isArray(payload?.output)) {
    return '';
  }

  const parts = [];

  for (const item of payload.output) {
    if (!Array.isArray(item?.content)) {
      continue;
    }

    for (const contentItem of item.content) {
      if (
        (contentItem?.type === 'output_text' || contentItem?.type === 'text') &&
        typeof contentItem.text === 'string'
      ) {
        const text = contentItem.text.trim();

        if (text) {
          parts.push(text);
        }
      }
    }
  }

  return parts.join('\n\n').trim();
}
function buildReviewPrompt(analysisText) {
  return [
    'You are a strict senior code reviewer.',
    'Review the following Codex analysis for technical correctness, unsupported claims, and missing checks.',
    'Return your review with sections: Findings, Risk Level, and Recommended Next Steps.',
    '',
    '=== ANALYSIS OUTPUT TO REVIEW ===',
    analysisText
  ].join('\n');
}

async function main() {
  await mkdir(outputDir, { recursive: true });

  const analysis = await requestCodex(ANALYSIS_PROMPT, {
    automation: 'daily_tri_site_diagnosis',
    stage: 'analysis'
  });

  const review = await requestCodex(buildReviewPrompt(analysis.text), {
    automation: 'daily_tri_site_diagnosis',
    stage: 'review'
  });

  const now = new Date().toISOString();

  const report = {
    generatedAt: now,
    model,
    prompt: ANALYSIS_PROMPT,
    analysisResponseId: analysis.id,
    reviewResponseId: review.id,
    analysis: analysis.text,
    review: review.text
  };

  await writeFile(path.join(outputDir, 'daily-tri-site-report.json'), `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  await writeFile(path.join(outputDir, 'daily-tri-site-analysis.md'), `${analysis.text}\n`, 'utf8');
  await writeFile(path.join(outputDir, 'daily-tri-site-code-review.md'), `${review.text}\n`, 'utf8');

  console.log(`Saved Codex analysis and review to ${outputDir}`);
}

const isDirectExecution =
  typeof process.argv[1] === 'string' && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectExecution) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
