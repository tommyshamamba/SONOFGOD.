const express = require('express');
const multer = require('multer');
const pdfParse = require('pdf-parse');
const path = require('path');
const fs = require('fs');

const store = require('../services/store');
const auth = require('../middleware/auth');
const { callAI } = require('../config/ai');
const PROMPTS = require('../prompts');

const router = express.Router();

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = path.join(__dirname, '../uploads');
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    cb(null, `${req.user.id}-${Date.now()}${path.extname(file.originalname)}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (['.pdf', '.txt'].includes(ext)) {
      cb(null, true);
      return;
    }

    cb(new Error('Only PDF and TXT files are allowed.'));
  },
});

router.post('/upload', auth, upload.single('resume'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  try {
    let rawText = '';
    const ext = path.extname(req.file.originalname).toLowerCase();

    if (ext === '.pdf') {
      const buffer = fs.readFileSync(req.file.path);
      const parsed = await pdfParse(buffer);
      rawText = parsed.text;
    } else {
      rawText = fs.readFileSync(req.file.path, 'utf8');
    }

    if (!rawText || rawText.trim().length < 100) {
      return res.status(422).json({ error: 'Could not extract enough text from the uploaded file.' });
    }

    const extracted = await callAI(PROMPTS.resumeExtraction(rawText), 2500);
    const resume = await store.createResume({
      userId: req.user.id,
      filePath: req.file.path,
      rawText,
      extracted,
    });

    return res.json({ resume, extracted });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Resume processing failed.', detail: error.message });
  }
});

router.post('/match', auth, async (req, res) => {
  const { resume_id, job_role, job_description } = req.body;
  if (!resume_id || !job_role) {
    return res.status(400).json({ error: 'resume_id and job_role are required.' });
  }

  try {
    const resume = await store.findResumeByIdForUser(resume_id, req.user.id);
    if (!resume) {
      return res.status(404).json({ error: 'Resume not found.' });
    }

    const match = await callAI(PROMPTS.skillMatch(resume, job_role, job_description), 1500);
    return res.json(match);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Match analysis failed.' });
  }
});

router.get('/', auth, async (req, res) => {
  try {
    const resume = await store.getLatestResumeForUser(req.user.id);
    return res.json(resume);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Failed to fetch resume.' });
  }
});

module.exports = router;
