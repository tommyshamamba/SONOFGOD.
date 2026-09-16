const Anthropic = require('@anthropic-ai/sdk');

const { aiMode } = require('./env');
const { buildMockResponse } = require('../services/mockAi');

const MODEL = 'claude-sonnet-4-20250514';
const client = aiMode === 'anthropic'
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

async function callAI(prompt, maxTokens = 2000) {
  if (!client) {
    return buildMockResponse(prompt);
  }

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: maxTokens,
    messages: [{ role: 'user', content: prompt }],
  });

  const raw = response.content[0]?.text?.trim() || '';
  const clean = raw.replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();

  try {
    return JSON.parse(clean);
  } catch {
    return { raw };
  }
}

async function streamAI(prompt, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  if (client) {
    const stream = await client.messages.stream({
      model: MODEL,
      max_tokens: 1500,
      messages: [{ role: 'user', content: prompt }],
    });

    for await (const chunk of stream) {
      if (chunk.type === 'content_block_delta' && chunk.delta?.text) {
        res.write(`data: ${JSON.stringify({ text: chunk.delta.text })}\n\n`);
      }
    }
  } else {
    const response = await callAI(prompt, 1500);
    const text = response.answer || response.raw || JSON.stringify(response, null, 2);

    for (const piece of text.split(/\s+/)) {
      if (!piece) {
        continue;
      }

      res.write(`data: ${JSON.stringify({ text: `${piece} ` })}\n\n`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  res.write('data: [DONE]\n\n');
  res.end();
}

module.exports = { callAI, streamAI };
