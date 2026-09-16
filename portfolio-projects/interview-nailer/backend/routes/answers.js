const express = require('express');

const store = require('../services/store');
const auth = require('../middleware/auth');
const { callAI, streamAI } = require('../config/ai');
const PROMPTS = require('../prompts');

const router = express.Router();

router.post('/generate', auth, async (req, res) => {
  const { question, question_type, job_role, resume_id, tone } = req.body;
  if (!question || !job_role) {
    return res.status(400).json({ error: 'question and job_role are required.' });
  }

  try {
    const resume = resume_id
      ? await store.findResumeByIdForUser(resume_id, req.user.id)
      : null;

    const answer = await callAI(
      PROMPTS.generateAnswer(question, question_type || 'behavioral', job_role, resume, tone),
      1500
    );

    return res.json(answer);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Answer generation failed.' });
  }
});

router.post('/generate/stream', auth, async (req, res) => {
  const { question, question_type, job_role, resume_id, tone } = req.body;
  if (!question || !job_role) {
    return res.status(400).json({ error: 'question and job_role are required.' });
  }

  try {
    const resume = resume_id
      ? await store.findResumeByIdForUser(resume_id, req.user.id)
      : null;

    await streamAI(
      PROMPTS.generateAnswer(question, question_type || 'behavioral', job_role, resume, tone),
      res
    );
  } catch (error) {
    console.error(error);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Stream failed.' });
    } else {
      res.end();
    }
  }
});

router.post('/score', auth, async (req, res) => {
  const { question, user_answer, job_role, resume_id } = req.body;
  if (!question || !user_answer || !job_role) {
    return res.status(400).json({ error: 'question, user_answer, and job_role are required.' });
  }

  try {
    const resume = resume_id
      ? await store.findResumeByIdForUser(resume_id, req.user.id)
      : null;

    const score = await callAI(PROMPTS.scoreAnswer(question, user_answer, job_role, resume), 1500);
    return res.json(score);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Scoring failed.' });
  }
});

router.post('/salary', auth, async (req, res) => {
  const { job_role, current_salary, target_salary, location, resume_id } = req.body;
  if (!job_role) {
    return res.status(400).json({ error: 'job_role is required.' });
  }

  try {
    const resume = resume_id
      ? await store.findResumeByIdForUser(resume_id, req.user.id)
      : null;

    const result = await callAI(
      PROMPTS.salaryCoach(job_role, current_salary, target_salary, location, resume),
      1500
    );

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Salary coach failed.' });
  }
});

module.exports = router;
