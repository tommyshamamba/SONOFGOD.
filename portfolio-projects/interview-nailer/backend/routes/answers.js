const express = require('express');
const store = require('../services/store');
const auth = require('../middleware/auth');
const { text, optionalText, uuid, QUESTION_TYPES } = require('../services/validation');
const PROMPTS = require('../prompts');
const router = express.Router();
router.use(auth);
router.use(async (req, res, next) => {
  const { job_role, resume_id, question_type, tone, current_salary, target_salary, location } = req.body;
  if (!text(job_role, 200) || (resume_id != null && !uuid(resume_id)) ||
      (question_type != null && !QUESTION_TYPES.includes(question_type)) || !optionalText(tone, 200) ||
      !optionalText(current_salary, 100) || !optionalText(target_salary, 100) || !optionalText(location, 200)) {
    return res.status(400).json({ error: 'Provide a job role and valid answer options.' });
  }
  try {
    req.resume = resume_id ? await store.findResumeByIdForUser(resume_id, req.user.id) : null;
    if (resume_id && !req.resume) return res.status(404).json({ error: 'Resume not found.' });
    return next();
  } catch (error) { return next(error); }
});

router.post(['/generate', '/generate/stream'], async (req, res, next) => {
  const { question, question_type, job_role, tone } = req.body;
  if (!text(question, 4000)) return res.status(400).json({ error: 'question is required (maximum 4000 characters).' });
  try {
    const prompt = PROMPTS.generateAnswer(question, question_type || 'behavioral', job_role, req.resume, tone);
    if (req.path.endsWith('/stream')) return await req.app.locals.ai.streamAI(prompt, res);
    return res.json(await req.app.locals.ai.callAI(prompt, 1500, 'answer'));
  } catch (error) { return next(error); }
});
router.post('/score', async (req, res, next) => {
  const { question, user_answer, job_role } = req.body;
  if (!text(question, 4000) || !text(user_answer, 15000)) return res.status(400).json({ error: 'A question and answer are required.' });
  try { return res.json(await req.app.locals.ai.callAI(PROMPTS.scoreAnswer(question, user_answer, job_role, req.resume), 1500, 'score')); }
  catch (error) { return next(error); }
});
router.post('/salary', async (req, res, next) => {
  const { job_role, current_salary, target_salary, location } = req.body;
  try { return res.json(await req.app.locals.ai.callAI(PROMPTS.salaryCoach(job_role, current_salary, target_salary, location, req.resume), 1500, 'salary')); }
  catch (error) { return next(error); }
});
module.exports = router;
