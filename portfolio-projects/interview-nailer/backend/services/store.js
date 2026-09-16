const fs = require('fs/promises');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const db = require('../config/db');
const { storageMode, storeFile } = require('../config/env');

const EMPTY_STORE = {
  users: [],
  resumes: [],
  sessions: [],
  answers: [],
};

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function now() {
  return new Date().toISOString();
}

function normalizeJson(value) {
  if (value == null) {
    return value;
  }

  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }

  return value;
}

function normalizeResume(record) {
  if (!record) {
    return null;
  }

  return {
    ...record,
    skills: normalizeJson(record.skills) || [],
    technical_skills: normalizeJson(record.technical_skills) || [],
    soft_skills: normalizeJson(record.soft_skills) || [],
    certifications: normalizeJson(record.certifications) || [],
    experience: normalizeJson(record.experience) || [],
    achievements: normalizeJson(record.achievements) || [],
    metrics: normalizeJson(record.metrics) || [],
    education: normalizeJson(record.education) || [],
    strengths: normalizeJson(record.strengths) || [],
    gaps: normalizeJson(record.gaps) || [],
  };
}

function normalizeSession(record) {
  if (!record) {
    return null;
  }

  return {
    ...record,
    completed: Boolean(record.completed),
    coaching: normalizeJson(record.coaching) || null,
  };
}

function normalizeAnswer(record) {
  if (!record) {
    return null;
  }

  return {
    ...record,
    score: normalizeJson(record.score) || null,
  };
}

async function ensureStoreFile() {
  await fs.mkdir(path.dirname(storeFile), { recursive: true });

  try {
    await fs.access(storeFile);
  } catch {
    await fs.writeFile(storeFile, JSON.stringify(EMPTY_STORE, null, 2));
  }
}

async function readStore() {
  await ensureStoreFile();
  const raw = await fs.readFile(storeFile, 'utf8');
  return raw ? JSON.parse(raw) : clone(EMPTY_STORE);
}

async function writeStore(store) {
  await fs.writeFile(storeFile, JSON.stringify(store, null, 2));
}

async function withStore(callback) {
  const store = await readStore();
  const result = await callback(store);
  await writeStore(store);
  return result;
}

function serializeJson(value) {
  return JSON.stringify(value ?? null);
}

async function initialize() {
  if (storageMode === 'file') {
    await ensureStoreFile();
    return;
  }

  await db.query('SELECT 1');
}

async function createUser({ email, passwordHash, fullName }) {
  const normalizedEmail = email.toLowerCase();

  if (storageMode === 'postgres') {
    const result = await db.query(
      `
        INSERT INTO users (email, password_hash, full_name)
        VALUES ($1, $2, $3)
        RETURNING id, email, full_name, created_at, updated_at
      `,
      [normalizedEmail, passwordHash, fullName || null]
    );

    return result.rows[0];
  }

  return withStore((store) => {
    const existing = store.users.find((user) => user.email === normalizedEmail);
    if (existing) {
      const error = new Error('Email already registered');
      error.code = '23505';
      throw error;
    }

    const user = {
      id: uuidv4(),
      email: normalizedEmail,
      password_hash: passwordHash,
      full_name: fullName || '',
      created_at: now(),
      updated_at: now(),
    };

    store.users.push(user);

    return {
      id: user.id,
      email: user.email,
      full_name: user.full_name,
      created_at: user.created_at,
      updated_at: user.updated_at,
    };
  });
}

async function findUserByEmail(email) {
  const normalizedEmail = email.toLowerCase();

  if (storageMode === 'postgres') {
    const result = await db.query('SELECT * FROM users WHERE email = $1', [normalizedEmail]);
    return result.rows[0] || null;
  }

  const store = await readStore();
  return store.users.find((user) => user.email === normalizedEmail) || null;
}

async function createResume({ userId, filePath, rawText, extracted }) {
  const payload = {
    user_id: userId,
    file_url: filePath,
    raw_text: rawText,
    title: extracted.title || '',
    summary: extracted.summary || '',
    skills: extracted.skills || [],
    technical_skills: extracted.technical_skills || [],
    soft_skills: extracted.soft_skills || [],
    certifications: extracted.certifications || [],
    experience: extracted.experience || [],
    achievements: extracted.achievements || [],
    metrics: extracted.metrics || [],
    education: extracted.education || [],
    strengths: extracted.strengths || [],
    gaps: extracted.gaps || [],
  };

  if (storageMode === 'postgres') {
    const result = await db.query(
      `
        INSERT INTO resumes (
          user_id, file_url, raw_text, title, summary, skills, technical_skills,
          soft_skills, certifications, experience, achievements, metrics,
          education, strengths, gaps
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb)
        RETURNING *
      `,
      [
        payload.user_id,
        payload.file_url,
        payload.raw_text,
        payload.title,
        payload.summary,
        serializeJson(payload.skills),
        serializeJson(payload.technical_skills),
        serializeJson(payload.soft_skills),
        serializeJson(payload.certifications),
        serializeJson(payload.experience),
        serializeJson(payload.achievements),
        serializeJson(payload.metrics),
        serializeJson(payload.education),
        serializeJson(payload.strengths),
        serializeJson(payload.gaps),
      ]
    );

    return normalizeResume(result.rows[0]);
  }

  return withStore((store) => {
    const resume = {
      id: uuidv4(),
      ...payload,
      created_at: now(),
    };

    store.resumes.push(resume);
    return normalizeResume(resume);
  });
}

async function findResumeByIdForUser(resumeId, userId) {
  if (!resumeId) {
    return null;
  }

  if (storageMode === 'postgres') {
    const result = await db.query('SELECT * FROM resumes WHERE id = $1 AND user_id = $2', [resumeId, userId]);
    return normalizeResume(result.rows[0] || null);
  }

  const store = await readStore();
  const resume = store.resumes.find((item) => item.id === resumeId && item.user_id === userId) || null;
  return normalizeResume(resume);
}

async function findResumeById(resumeId) {
  if (!resumeId) {
    return null;
  }

  if (storageMode === 'postgres') {
    const result = await db.query('SELECT * FROM resumes WHERE id = $1', [resumeId]);
    return normalizeResume(result.rows[0] || null);
  }

  const store = await readStore();
  const resume = store.resumes.find((item) => item.id === resumeId) || null;
  return normalizeResume(resume);
}

async function getLatestResumeForUser(userId) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      'SELECT * FROM resumes WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1',
      [userId]
    );
    return normalizeResume(result.rows[0] || null);
  }

  const store = await readStore();
  const resume = store.resumes
    .filter((item) => item.user_id === userId)
    .sort((left, right) => new Date(right.created_at) - new Date(left.created_at))[0] || null;

  return normalizeResume(resume);
}

async function createSession({ userId, resumeId, jobRole, jobDescription, mode }) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      `
        INSERT INTO sessions (user_id, resume_id, job_role, job_description, mode)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
      `,
      [userId, resumeId || null, jobRole, jobDescription || null, mode || 'mock']
    );

    return normalizeSession(result.rows[0]);
  }

  return withStore((store) => {
    const session = {
      id: uuidv4(),
      user_id: userId,
      resume_id: resumeId || null,
      job_role: jobRole,
      job_description: jobDescription || null,
      mode: mode || 'mock',
      score_avg: null,
      completed: false,
      coaching: null,
      created_at: now(),
    };

    store.sessions.push(session);
    return normalizeSession(session);
  });
}

async function findSessionByIdForUser(sessionId, userId) {
  if (storageMode === 'postgres') {
    const result = await db.query('SELECT * FROM sessions WHERE id = $1 AND user_id = $2', [sessionId, userId]);
    return normalizeSession(result.rows[0] || null);
  }

  const store = await readStore();
  const session = store.sessions.find((item) => item.id === sessionId && item.user_id === userId) || null;
  return normalizeSession(session);
}

async function listSessionsForUser(userId) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      'SELECT * FROM sessions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 20',
      [userId]
    );
    return result.rows.map(normalizeSession);
  }

  const store = await readStore();
  return store.sessions
    .filter((item) => item.user_id === userId)
    .sort((left, right) => new Date(right.created_at) - new Date(left.created_at))
    .slice(0, 20)
    .map(normalizeSession);
}

async function saveAnswer({ sessionId, question, questionType, userAnswer, aiAnswer, score, audioUrl }) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      `
        INSERT INTO answers (session_id, question, question_type, user_answer, ai_answer, score, audio_url)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        RETURNING *
      `,
      [sessionId, question, questionType || null, userAnswer || null, aiAnswer || null, serializeJson(score), audioUrl || null]
    );

    return normalizeAnswer(result.rows[0]);
  }

  return withStore((store) => {
    const answer = {
      id: uuidv4(),
      session_id: sessionId,
      question,
      question_type: questionType || null,
      user_answer: userAnswer || null,
      ai_answer: aiAnswer || null,
      score: score || null,
      audio_url: audioUrl || null,
      created_at: now(),
    };

    store.answers.push(answer);
    return normalizeAnswer(answer);
  });
}

async function listAnswersForSession(sessionId) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      'SELECT * FROM answers WHERE session_id = $1 ORDER BY created_at ASC',
      [sessionId]
    );
    return result.rows.map(normalizeAnswer);
  }

  const store = await readStore();
  return store.answers
    .filter((item) => item.session_id === sessionId)
    .sort((left, right) => new Date(left.created_at) - new Date(right.created_at))
    .map(normalizeAnswer);
}

async function updateSessionCompletion(sessionId, { completed, scoreAvg, coaching }) {
  if (storageMode === 'postgres') {
    const result = await db.query(
      `
        UPDATE sessions
        SET completed = $1, score_avg = $2, coaching = $3::jsonb
        WHERE id = $4
        RETURNING *
      `,
      [completed, scoreAvg, serializeJson(coaching), sessionId]
    );

    return normalizeSession(result.rows[0]);
  }

  return withStore((store) => {
    const session = store.sessions.find((item) => item.id === sessionId);
    if (!session) {
      return null;
    }

    session.completed = completed;
    session.score_avg = scoreAvg;
    session.coaching = coaching || null;

    return normalizeSession(session);
  });
}

module.exports = {
  initialize,
  createUser,
  findUserByEmail,
  createResume,
  findResumeByIdForUser,
  findResumeById,
  getLatestResumeForUser,
  createSession,
  findSessionByIdForUser,
  listSessionsForUser,
  saveAnswer,
  listAnswersForSession,
  updateSessionCompletion,
  mode: storageMode,
};
