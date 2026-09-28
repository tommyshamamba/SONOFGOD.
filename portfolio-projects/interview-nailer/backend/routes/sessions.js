const express = require('express');
const store = require('../services/store');
const auth = require('../middleware/auth');
const { uuid, text, optionalText, validScore, QUESTION_TYPES } = require('../services/validation');
const PROMPTS = require('../prompts');
const router = express.Router();
router.use(auth);
router.param('id', (req, res, next, id) => uuid(id) ? next() : res.status(404).json({ error: 'Session not found.' }));

router.post('/start', async (req, res, next) => {
  const { job_role, job_description, resume_id, mode = 'mock', difficulty = 'intermediate' } = req.body;
  if (!text(job_role, 200) || !optionalText(job_description, 15000) ||
      (resume_id != null && !uuid(resume_id)) || !['mock', 'practice', 'coaching'].includes(mode) ||
      !['beginner', 'intermediate', 'advanced'].includes(difficulty)) {
    return res.status(400).json({ error: 'Provide a job role and valid session options.' });
  }
  try {
    const resume = resume_id ? await store.findResumeByIdForUser(resume_id, req.user.id) : null;
    if (resume_id && !resume) return res.status(404).json({ error: 'Resume not found.' });
    const response = await req.app.locals.ai.callAI(PROMPTS.generateQuestions(job_role, job_description, resume, difficulty), 2000, 'questions');
    const session = await store.createSession({ userId: req.user.id, resumeId: resume?.id, jobRole: job_role.trim(), jobDescription: job_description, mode });
    return res.json({ session, questions: response.questions });
  } catch (error) { return next(error); }
});

router.post('/:id/answer', async (req, res, next) => {
  const { question, question_type, user_answer, ai_answer, score, audio_url } = req.body;
  if (!text(question, 4000) || !text(user_answer, 15000) || !optionalText(ai_answer, 20000) ||
      !optionalText(audio_url, 2000) || (question_type != null && !QUESTION_TYPES.includes(question_type)) ||
      (score != null && !validScore(score))) {
    return res.status(400).json({ error: 'Provide a question, answer, and valid optional score.' });
  }
  try {
    const session = await store.findSessionByIdForUser(req.params.id, req.user.id);
    if (!session) return res.status(404).json({ error: 'Session not found.' });
    if (session.completed) return res.status(409).json({ error: 'This session is already complete.' });
    const answer = await store.saveAnswer({ sessionId: session.id, question, questionType: question_type, userAnswer: user_answer, aiAnswer: ai_answer, score, audioUrl: audio_url });
    return res.json(answer);
  } catch (error) { return next(error); }
});

router.post('/:id/complete', async (req, res, next) => {
  try {
    const session = await store.findSessionByIdForUser(req.params.id, req.user.id);
    if (!session) return res.status(404).json({ error: 'Session not found.' });
    const resume = session.resume_id ? await store.findResumeByIdForUser(session.resume_id, req.user.id) : null;
    // Fail closed for legacy sessions that were saved with another user's ID.
    if (session.resume_id && !resume) return res.status(404).json({ error: 'Resume not found.' });
    const answers = await store.listAnswersForSession(session.id);
    if (session.completed) return res.json({ session, coaching: session.coaching, answers });
    if (!answers.length) return res.status(422).json({ error: 'Save at least one answer before completing this session.' });
    const totals = answers.map((answer) => answer.score?.total).filter((value) => typeof value === 'number' && Number.isFinite(value) && value >= 1 && value <= 10);
    const avg = totals.length ? Number((totals.reduce((sum, value) => sum + value, 0) / totals.length).toFixed(2)) : null;
    const coaching = await req.app.locals.ai.callAI(PROMPTS.coachingPlan(answers.slice(0, 12), resume, session.job_role), 2000, 'coaching');
    const updatedSession = await store.updateSessionCompletion(session.id, { completed: true, scoreAvg: avg, coaching });
    return res.json({ session: updatedSession, coaching, answers });
  } catch (error) { return next(error); }
});

router.get('/', async (req, res, next) => {
  try { return res.json(await store.listSessionsForUser(req.user.id)); } catch (error) { return next(error); }
});
router.get('/:id', async (req, res, next) => {
  try {
    const session = await store.findSessionByIdForUser(req.params.id, req.user.id);
    if (!session) return res.status(404).json({ error: 'Session not found.' });
    return res.json({ session, answers: await store.listAnswersForSession(session.id) });
  } catch (error) { return next(error); }
});
module.exports = router;
