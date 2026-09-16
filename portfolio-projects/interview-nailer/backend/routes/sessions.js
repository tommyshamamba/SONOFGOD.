const express = require('express');

const store = require('../services/store');
const auth = require('../middleware/auth');
const { callAI } = require('../config/ai');
const PROMPTS = require('../prompts');

const router = express.Router();

function parseScore(score) {
  if (!score) {
    return null;
  }

  if (typeof score === 'string') {
    try {
      return JSON.parse(score);
    } catch {
      return null;
    }
  }

  return score;
}

router.post('/start', auth, async (req, res) => {
  const { job_role, job_description, resume_id, mode, difficulty } = req.body;
  if (!job_role) {
    return res.status(400).json({ error: 'job_role is required.' });
  }

  try {
    const resume = resume_id
      ? await store.findResumeByIdForUser(resume_id, req.user.id)
      : null;

    const response = await callAI(
      PROMPTS.generateQuestions(job_role, job_description, resume, difficulty),
      2000
    );

    const session = await store.createSession({
      userId: req.user.id,
      resumeId: resume_id,
      jobRole: job_role,
      jobDescription: job_description,
      mode,
    });

    return res.json({ session, questions: response.questions || [] });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to start session.' });
  }
});

router.post('/:id/answer', auth, async (req, res) => {
  const { question, question_type, user_answer, ai_answer, score, audio_url } = req.body;
  const { id: sessionId } = req.params;

  if (!question) {
    return res.status(400).json({ error: 'question is required.' });
  }

  try {
    const session = await store.findSessionByIdForUser(sessionId, req.user.id);
    if (!session) {
      return res.status(404).json({ error: 'Session not found.' });
    }

    const answer = await store.saveAnswer({
      sessionId,
      question,
      questionType: question_type,
      userAnswer: user_answer,
      aiAnswer: ai_answer,
      score,
      audioUrl: audio_url,
    });

    return res.json(answer);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to save answer.' });
  }
});

router.post('/:id/complete', auth, async (req, res) => {
  const { id: sessionId } = req.params;

  try {
    const session = await store.findSessionByIdForUser(sessionId, req.user.id);
    if (!session) {
      return res.status(404).json({ error: 'Session not found.' });
    }

    const answers = await store.listAnswersForSession(sessionId);
    const totals = answers
      .map((answer) => parseScore(answer.score)?.total)
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
    const avg = totals.length ? Number((totals.reduce((sum, value) => sum + value, 0) / totals.length).toFixed(2)) : null;
    const resume = session.resume_id
      ? await store.findResumeById(session.resume_id)
      : null;

    const coaching = await callAI(
      PROMPTS.coachingPlan(answers.slice(0, 12), resume, session.job_role),
      2000
    );

    const updatedSession = await store.updateSessionCompletion(sessionId, {
      completed: true,
      scoreAvg: avg,
      coaching,
    });

    return res.json({ session: updatedSession, coaching, answers });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to complete session.' });
  }
});

router.get('/', auth, async (req, res) => {
  try {
    const sessions = await store.listSessionsForUser(req.user.id);
    return res.json(sessions);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to fetch sessions.' });
  }
});

router.get('/:id', auth, async (req, res) => {
  try {
    const session = await store.findSessionByIdForUser(req.params.id, req.user.id);
    if (!session) {
      return res.status(404).json({ error: 'Session not found.' });
    }

    const answers = await store.listAnswersForSession(req.params.id);
    return res.json({ session, answers });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to fetch session.' });
  }
});

module.exports = router;
