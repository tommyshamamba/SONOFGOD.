const express = require('express');
const multer = require('multer');
const pdfParse = require('pdf-parse');
const path = require('path');
const store = require('../services/store');
const auth = require('../middleware/auth');
const { text, optionalText, uuid } = require('../services/validation');
const PROMPTS = require('../prompts');
const router = express.Router();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024, files: 1, fields: 0, parts: 1 },
  fileFilter(req, file, callback) {
    if (['.pdf', '.txt'].includes(path.extname(file.originalname).toLowerCase())) return callback(null, true);
    const error = new Error('Only PDF and TXT files are allowed.'); error.status = 415;
    return callback(error);
  },
});
router.use(auth);
router.post('/upload', upload.single('resume'), async (req, res, next) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded.' });
  let rawText;
  try {
    if (path.extname(req.file.originalname).toLowerCase() === '.pdf') {
      if (req.file.buffer.subarray(0, 5).toString('ascii') !== '%PDF-') return res.status(422).json({ error: 'Invalid PDF file.' });
      rawText = (await pdfParse(req.file.buffer)).text;
    } else { rawText = req.file.buffer.toString('utf8'); }
  } catch { return res.status(422).json({ error: 'Could not read the uploaded document.' }); }
  if (!text(rawText, 50000) || rawText.trim().length < 100 || rawText.includes('\u0000')) {
    return res.status(422).json({ error: 'Upload readable text with between 100 and 50000 characters.' });
  }
  try {
    const extracted = await req.app.locals.ai.callAI(PROMPTS.resumeExtraction(rawText), 2500, 'resume');
    const resume = await store.createResume({ userId: req.user.id, filePath: null, rawText, extracted });
    return res.json({ resume, extracted });
  } catch (error) { return next(error); }
});
router.post('/match', async (req, res, next) => {
  const { resume_id, job_role, job_description } = req.body;
  if (!uuid(resume_id) || !text(job_role, 200) || !optionalText(job_description, 15000)) return res.status(400).json({ error: 'A valid resume_id and job_role are required.' });
  try {
    const resume = await store.findResumeByIdForUser(resume_id, req.user.id);
    if (!resume) return res.status(404).json({ error: 'Resume not found.' });
    return res.json(await req.app.locals.ai.callAI(PROMPTS.skillMatch(resume, job_role, job_description), 1500, 'match'));
  } catch (error) { return next(error); }
});
router.get('/', async (req, res, next) => {
  try { return res.json(await store.getLatestResumeForUser(req.user.id)); } catch (error) { return next(error); }
});
module.exports = router;
