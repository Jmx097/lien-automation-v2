import path from 'node:path';
import { describe, expect, it } from 'vitest';

const scriptModulePromise = import(
  path.resolve(__dirname, '../../scripts/codex/daily-tri-site-review.mjs')
);

describe('daily-tri-site-review', () => {
  it('prefers the convenience output_text field when present', async () => {
    const { extractResponseText } = await scriptModulePromise;

    expect(extractResponseText({ output_text: '  concise answer  ' })).toBe('concise answer');
  });

  it('falls back to assistant message content in the output array', async () => {
    const { extractResponseText } = await scriptModulePromise;

    expect(
      extractResponseText({
        output: [
          { type: 'reasoning', summary: [] },
          {
            type: 'message',
            role: 'assistant',
            content: [
              { type: 'output_text', text: 'First finding' },
              { type: 'output_text', text: 'Second finding' },
            ],
          },
        ],
      })
    ).toBe('First finding\n\nSecond finding');
  });

  it('returns an empty string when the response has no text content', async () => {
    const { extractResponseText } = await scriptModulePromise;

    expect(extractResponseText({ output: [{ type: 'reasoning', summary: [] }] })).toBe('');
  });
});
