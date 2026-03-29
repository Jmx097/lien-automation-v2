#!/usr/bin/env node
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const ANALYSIS_PROMPT =
  'analyze the workspace and diagnose how strong California, NYC Acris and Mericopa are all pulling 3x a day with 95% accuracy';

const model = process.env.OPENAI_MODEL || 'gpt-5.3-codex';
const apiKey = process.env.OPENAI_API_KEY;
const outputDir = process.env.CODEX_REPORT_DIR || path.join(process.cwd(), 'tmp', 'codex-reports');

if (!apiKey) {
  console.error('Missing OPENAI_API_KEY. Set it in the workflow secrets before running this automation.');
  process.exit(1);
}

async function requestCodex(prompt, metadata = {}) {
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
  const text = payload.output_text?.trim();

  if (!text) {
    throw new Error('Codex API response did not include output_text.');
  }

  return {
    id: payload.id,
    text,
    raw: payload
  };
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

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
