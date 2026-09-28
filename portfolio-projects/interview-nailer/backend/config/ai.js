const Anthropic = require('@anthropic-ai/sdk');
const { aiMode, anthropicModel } = require('./env');
const { buildMockResponse } = require('../services/mockAi');
const { validateAIResponse, AIResponseError } = require('../services/validation');

function createAI({ mode = aiMode, client, model = anthropicModel } = {}) {
  const provider = mode === 'anthropic'
    ? (client || new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY, timeout: 30000, maxRetries: 1 }))
    : null;

  async function callAI(prompt, maxTokens = 2000, kind) {
    let result;
    if (!provider) {
      result = buildMockResponse(prompt);
    } else {
      let response;
      try {
        response = await provider.messages.create({
          model, max_tokens: maxTokens, messages: [{ role: 'user', content: prompt }],
        });
      } catch {
        // Do not send provider error bodies (or credentials) to clients/logs.
        const error = new Error('The AI service is unavailable. Please try again later.');
        error.status = 503;
        throw error;
      }
      if (response?.stop_reason !== 'end_turn' || !Array.isArray(response.content)) throw new AIResponseError();
      const raw = response.content.filter((block) => block.type === 'text').map((block) => block.text).join('\n').trim();
      const clean = raw.replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/i, '').trim();
      try { result = JSON.parse(clean); } catch { throw new AIResponseError(); }
    }
    return validateAIResponse(kind, result);
  }

  async function streamAI(prompt, res) {
    // Validate the complete structured answer before emitting text. A failed or
    // truncated provider response must not become a successful partial answer.
    const result = await callAI(prompt, 1500, 'answer');
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('X-Accel-Buffering', 'no');
    res.write(`data: ${JSON.stringify({ text: result.answer })}\n\n`);
    res.end('data: [DONE]\n\n');
  }
  return { callAI, streamAI };
}

module.exports = { ...createAI(), createAI };
