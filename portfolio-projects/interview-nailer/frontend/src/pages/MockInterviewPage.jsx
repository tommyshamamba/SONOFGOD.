import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { startSession, saveAnswer, completeSession, scoreAnswer, generateAnswer, getResume, getApiErrorMessage } from '../api';
import toast from 'react-hot-toast';

const STAGES = { SETUP: 'setup', INTRO: 'intro', QUESTION: 'question', RESULT: 'result', COMPLETE: 'complete' };
const DIFFICULTIES = ['beginner', 'intermediate', 'advanced'];
const MODES = [
  { id: 'mock',     label: '🎯 Full Mock',    desc: '12 questions, timed, scored live' },
  { id: 'practice', label: '💬 Practice',     desc: 'Answer at your own pace, no timer' },
  { id: 'coaching', label: '🧑‍🏫 Coaching',    desc: 'AI gives detailed feedback on every answer' },
];

export default function MockInterviewPage() {
  const navigate  = useNavigate();
  const timerRef  = useRef(null);

  const [stage,      setStage]      = useState(STAGES.SETUP);
  const [resume,     setResume]     = useState(null);
  const [setup,      setSetup]      = useState({ job_role: '', job_desc: '', difficulty: 'intermediate', mode: 'mock' });
  const [session,    setSession]    = useState(null);
  const [questions,  setQuestions]  = useState([]);
  const [qIndex,     setQIndex]     = useState(0);
  const [userAnswer, setUserAnswer] = useState('');
  const [aiAnswer,   setAiAnswer]   = useState(null);
  const [scoreData,  setScoreData]  = useState(null);
  const [seconds,    setSeconds]    = useState(120);
  const [loading,    setLoading]    = useState(false);
  const [results,    setResults]    = useState([]);

  useEffect(() => {
    getResume().then(r => setResume(r.data)).catch(() => {});
  }, []);

  // Timer
  useEffect(() => {
    if (stage === STAGES.QUESTION && setup.mode === 'mock') {
      setSeconds(120);
      timerRef.current = setInterval(() => setSeconds(s => { if (s <= 1) { clearInterval(timerRef.current); return 0; } return s - 1; }), 1000);
    }
    return () => clearInterval(timerRef.current);
  }, [stage, qIndex]);

  const startMock = async () => {
    if (!setup.job_role) { toast.error('Enter a job role first'); return; }
    setLoading(true);
    try {
      const { data } = await startSession({
        job_role: setup.job_role, job_description: setup.job_desc,
        resume_id: resume?.id, mode: setup.mode, difficulty: setup.difficulty
      });
      setSession(data.session);
      setQuestions(data.questions);
      setStage(STAGES.INTRO);
    } catch (err) { toast.error(getApiErrorMessage(err, 'Failed to start session')); }
    finally { setLoading(false); }
  };

  const submitAnswer = async () => {
    if (!userAnswer.trim()) { toast.error('Type your answer first'); return; }
    clearInterval(timerRef.current);
    setLoading(true);
    const q = questions[qIndex];
    try {
      // Score answer
      const { data: score } = await scoreAnswer({
        question: q.question, user_answer: userAnswer,
        job_role: setup.job_role, resume_id: resume?.id, session_id: session?.id
      });
      // Get AI model answer
      const { data: ai } = await generateAnswer({
        question: q.question, question_type: q.type,
        job_role: setup.job_role, resume_id: resume?.id
      });
      setScoreData(score);
      setAiAnswer(ai);
      // Save to session
      await saveAnswer(session.id, {
        question: q.question, question_type: q.type,
        user_answer: userAnswer, ai_answer: ai.answer,
        score: score
      });
      setResults(prev => [...prev, { question: q, userAnswer, score, aiAnswer: ai }]);
      setStage(STAGES.RESULT);
    } catch (err) { toast.error(getApiErrorMessage(err, 'Scoring failed')); }
    finally { setLoading(false); }
  };

  const nextQuestion = () => {
    if (qIndex + 1 >= questions.length) { finishSession(); return; }
    setQIndex(i => i + 1);
    setUserAnswer('');
    setScoreData(null);
    setAiAnswer(null);
    setStage(STAGES.QUESTION);
  };

  const finishSession = async () => {
    try {
      await completeSession(session.id);
      navigate(`/coaching/${session.id}`);
    } catch (err) { toast.error(getApiErrorMessage(err, 'Error completing session')); }
  };

  const q = questions[qIndex];
  const typeColor = { behavioral:'#58A6FF', technical:'#3FB950', leadership:'#BC8CFF', culture_fit:'#F5A623', salary:'#F85149', situational:'#FF9F43' };

  // ── SETUP SCREEN ──────────────────────────────────────────────────────────
  if (stage === STAGES.SETUP) return (
    <div style={s.page}>
      <div style={s.container}>
        <h2 style={s.title}>🎯 Mock Interview</h2>
        <p style={s.sub}>Configure your session and face real interview questions</p>

        <div style={s.card}>
          <div style={s.field}>
            <label style={s.label}>Target Job Role *</label>
            <input style={s.input} value={setup.job_role}
              onChange={e => setSetup({ ...setup, job_role: e.target.value })}
              placeholder="e.g. Network Engineer, Data Center Technician, NOC Analyst" />
          </div>
          <div style={s.field}>
            <label style={s.label}>Job Description (optional but recommended)</label>
            <textarea style={s.textarea} value={setup.job_desc} rows={4}
              onChange={e => setSetup({ ...setup, job_desc: e.target.value })}
              placeholder="Paste the job description to get questions tailored to this exact role..." />
          </div>
          <div style={s.field}>
            <label style={s.label}>Difficulty</label>
            <div style={s.tabs}>
              {DIFFICULTIES.map(d => (
                <button key={d} onClick={() => setSetup({ ...setup, difficulty: d })}
                  style={{ ...s.tab, ...(setup.difficulty === d ? s.tabActive : {}) }}>
                  {d.charAt(0).toUpperCase() + d.slice(1)}
                </button>
              ))}
            </div>
          </div>
          <div style={s.field}>
            <label style={s.label}>Mode</label>
            <div style={s.modeGrid}>
              {MODES.map(m => (
                <div key={m.id} onClick={() => setSetup({ ...setup, mode: m.id })}
                  style={{ ...s.modeCard, ...(setup.mode === m.id ? s.modeCardActive : {}) }}>
                  <div style={s.modeLabel}>{m.label}</div>
                  <div style={s.modeDesc}>{m.desc}</div>
                </div>
              ))}
            </div>
          </div>
          {resume ? (
            <p style={s.resumeNote}>✅ Resume loaded — answers will be personalized to your background</p>
          ) : (
            <p style={s.resumeWarn}>⚠️ No resume uploaded — <a href="/resume" style={{ color: '#F5A623' }}>upload one</a> for personalized answers</p>
          )}
          <button onClick={startMock} disabled={loading} style={s.startBtn}>
            {loading ? '⏳ Building your interview...' : '🚀 Start Interview'}
          </button>
        </div>
      </div>
    </div>
  );

  // ── INTRO SCREEN ──────────────────────────────────────────────────────────
  if (stage === STAGES.INTRO) return (
    <div style={s.page}>
      <div style={{ ...s.container, textAlign: 'center', paddingTop: 80 }}>
        <span style={{ fontSize: 64 }}>🎯</span>
        <h2 style={{ ...s.title, marginTop: 16 }}>{setup.job_role} Interview</h2>
        <p style={s.sub}>{questions.length} questions · {setup.mode} mode · {setup.difficulty} difficulty</p>
        <div style={s.introCard}>
          <h3 style={{ margin: '0 0 12px', color: '#E6EDF3' }}>Before you begin:</h3>
          <p style={{ color: '#8B949E', margin: '0 0 8px' }}>✅ Use the STAR method: Situation → Task → Action → Result</p>
          <p style={{ color: '#8B949E', margin: '0 0 8px' }}>✅ Aim for 60–90 second answers</p>
          <p style={{ color: '#8B949E', margin: 0 }}>✅ Be specific — vague answers score lower</p>
        </div>
        <button onClick={() => setStage(STAGES.QUESTION)} style={s.startBtn}>
          I'm Ready — Start Question 1 →
        </button>
      </div>
    </div>
  );

  // ── QUESTION SCREEN ───────────────────────────────────────────────────────
  if (stage === STAGES.QUESTION && q) return (
    <div style={s.page}>
      <div style={s.container}>
        {/* Progress bar */}
        <div style={s.progressWrap}>
          <div style={s.progressBar}>
            <div style={{ ...s.progressFill, width: `${((qIndex) / questions.length) * 100}%` }} />
          </div>
          <span style={s.progressText}>{qIndex + 1} of {questions.length}</span>
        </div>

        <div style={s.card}>
          {/* Question header */}
          <div style={s.qHeader}>
            <span style={{ ...s.qTypeBadge, background: typeColor[q.type] || '#58A6FF' }}>
              {q.type?.replace('_', ' ')}
            </span>
            <span style={s.qDifficulty}>{q.difficulty}</span>
            {setup.mode === 'mock' && (
              <span style={{ ...s.timer, color: seconds <= 30 ? '#F85149' : seconds <= 60 ? '#F5A623' : '#3FB950' }}>
                ⏱ {Math.floor(seconds / 60)}:{String(seconds % 60).padStart(2, '0')}
              </span>
            )}
          </div>

          <h3 style={s.questionText}>{q.question}</h3>

          {setup.mode === 'coaching' && (
            <div style={s.hint}>💡 Hint: {q.hint}</div>
          )}

          <textarea style={s.answerBox} value={userAnswer} rows={10}
            onChange={e => setUserAnswer(e.target.value)}
            placeholder="Type your answer here using the STAR method...&#10;&#10;Situation: (set the context)&#10;Task: (what was your responsibility)&#10;Action: (what specifically did YOU do)&#10;Result: (what was the outcome)" />

          <div style={s.qActions}>
            <span style={s.wordCount}>{userAnswer.split(/\s+/).filter(Boolean).length} words</span>
            <button onClick={submitAnswer} disabled={loading || !userAnswer.trim()} style={s.submitBtn}>
              {loading ? '⏳ Scoring...' : 'Submit Answer →'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );

  // ── RESULT SCREEN ─────────────────────────────────────────────────────────
  if (stage === STAGES.RESULT && scoreData) return (
    <div style={s.page}>
      <div style={s.container}>
        <div style={s.card}>
          {/* Score header */}
          <div style={s.scoreHeader}>
            <div style={s.bigScore}>
              <span style={{ fontSize: 36, fontWeight: 800, color: scoreData.total >= 7 ? '#3FB950' : scoreData.total >= 5 ? '#F5A623' : '#F85149' }}>
                {scoreData.total?.toFixed(1)}
              </span>
              <span style={{ fontSize: 16, color: '#8B949E' }}>/10</span>
              <span style={{ fontSize: 28, marginLeft: 8 }}>{scoreData.grade}</span>
            </div>
            <p style={s.oneLiner}>{scoreData.one_liner_feedback}</p>
          </div>

          {/* Score breakdown */}
          <div style={s.scoreGrid}>
            {Object.entries(scoreData.scores || {}).map(([k, v]) => (
              <div key={k} style={s.scoreItem}>
                <div style={s.scoreItemLabel}>{k}</div>
                <div style={s.scoreBar}>
                  <div style={{ ...s.scoreBarFill, width: `${v * 10}%`, background: v >= 7 ? '#3FB950' : v >= 5 ? '#F5A623' : '#F85149' }} />
                </div>
                <div style={s.scoreItemVal}>{v}/10</div>
              </div>
            ))}
          </div>

          {/* Feedback */}
          <div style={s.feedbackGrid}>
            <div style={s.feedbackCol}>
              <h4 style={{ ...s.feedHead, color: '#3FB950' }}>✅ What Worked</h4>
              {scoreData.what_worked?.map((w, i) => <p key={i} style={s.feedItem}>• {w}</p>)}
            </div>
            <div style={s.feedbackCol}>
              <h4 style={{ ...s.feedHead, color: '#F85149' }}>🔧 Fix This</h4>
              {scoreData.what_to_fix?.map((w, i) => <p key={i} style={s.feedItem}>• {w}</p>)}
            </div>
          </div>

          {/* Improved version */}
          {aiAnswer && (
            <div style={s.improvedBox}>
              <h4 style={s.improvedHead}>🤖 How a 9/10 Answer Sounds</h4>
              <p style={s.improvedText}>{aiAnswer.answer}</p>
              {aiAnswer.key_phrase && (
                <p style={s.keyPhrase}>🎯 Key phrase: "{aiAnswer.key_phrase}"</p>
              )}
            </div>
          )}

          <div style={s.resultActions}>
            <button onClick={nextQuestion} style={s.nextBtn}>
              {qIndex + 1 >= questions.length ? '🏁 Finish & Get Coaching Plan' : `Next Question (${qIndex + 2}/${questions.length}) →`}
            </button>
          </div>
        </div>
      </div>
    </div>
  );

  return null;
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623' };
const s = {
  page:          { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: 'system-ui, sans-serif', padding: '40px 24px' },
  container:     { maxWidth: 820, margin: '0 auto' },
  title:         { fontSize: 28, fontWeight: 700, margin: '0 0 8px' },
  sub:           { color: C.muted, margin: '0 0 28px' },
  card:          { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 14, padding: 32 },
  field:         { marginBottom: 22 },
  label:         { display: 'block', fontSize: 13, fontWeight: 600, marginBottom: 8, color: C.text },
  input:         { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 15, boxSizing: 'border-box' },
  textarea:      { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 14, resize: 'vertical', boxSizing: 'border-box' },
  tabs:          { display: 'flex', gap: 8 },
  tab:           { padding: '8px 18px', borderRadius: 20, border: `1px solid ${C.border}`, background: 'transparent', color: C.muted, cursor: 'pointer', fontSize: 14 },
  tabActive:     { background: C.gold, border: `1px solid ${C.gold}`, color: '#000', fontWeight: 600 },
  modeGrid:      { display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12 },
  modeCard:      { background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 10, padding: 16, cursor: 'pointer' },
  modeCardActive:{ border: `1px solid ${C.gold}` },
  modeLabel:     { fontWeight: 600, fontSize: 14, marginBottom: 4, color: C.text },
  modeDesc:      { fontSize: 12, color: C.muted },
  resumeNote:    { fontSize: 13, color: '#3FB950', margin: '0 0 20px' },
  resumeWarn:    { fontSize: 13, color: '#F5A623', margin: '0 0 20px' },
  startBtn:      { width: '100%', background: C.gold, border: 'none', borderRadius: 8, padding: 14, color: '#000', fontWeight: 700, fontSize: 16, cursor: 'pointer', marginTop: 8 },
  introCard:     { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 28, maxWidth: 480, margin: '0 auto 32px', textAlign: 'left' },
  progressWrap:  { display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24 },
  progressBar:   { flex: 1, height: 6, background: '#21262D', borderRadius: 3, overflow: 'hidden' },
  progressFill:  { height: '100%', background: C.gold, borderRadius: 3, transition: 'width 0.4s' },
  progressText:  { fontSize: 13, color: C.muted, whiteSpace: 'nowrap' },
  qHeader:       { display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 },
  qTypeBadge:    { padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 700, color: '#000' },
  qDifficulty:   { fontSize: 12, color: C.muted, textTransform: 'capitalize' },
  timer:         { marginLeft: 'auto', fontSize: 18, fontWeight: 700 },
  questionText:  { fontSize: 20, fontWeight: 600, lineHeight: 1.5, margin: '0 0 20px' },
  hint:          { background: '#1A2332', border: `1px solid #1E3A5F`, borderRadius: 8, padding: 12, fontSize: 13, color: '#58A6FF', marginBottom: 16 },
  answerBox:     { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: 16, color: C.text, fontSize: 15, lineHeight: 1.7, resize: 'vertical', boxSizing: 'border-box', marginBottom: 16 },
  qActions:      { display: 'flex', alignItems: 'center', justifyContent: 'space-between' },
  wordCount:     { fontSize: 13, color: C.muted },
  submitBtn:     { background: C.gold, border: 'none', borderRadius: 8, padding: '12px 28px', color: '#000', fontWeight: 700, fontSize: 15, cursor: 'pointer' },
  scoreHeader:   { textAlign: 'center', marginBottom: 28 },
  bigScore:      { display: 'flex', alignItems: 'baseline', justifyContent: 'center', gap: 4, marginBottom: 8 },
  oneLiner:      { color: C.muted, fontSize: 15, margin: 0 },
  scoreGrid:     { display: 'flex', flexDirection: 'column', gap: 10, marginBottom: 24 },
  scoreItem:     { display: 'flex', alignItems: 'center', gap: 12 },
  scoreItemLabel:{ width: 90, fontSize: 13, color: C.muted, textTransform: 'capitalize' },
  scoreBar:      { flex: 1, height: 8, background: '#21262D', borderRadius: 4, overflow: 'hidden' },
  scoreBarFill:  { height: '100%', borderRadius: 4, transition: 'width 0.6s' },
  scoreItemVal:  { width: 40, fontSize: 13, fontWeight: 700, color: C.text, textAlign: 'right' },
  feedbackGrid:  { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 },
  feedbackCol:   { background: '#0D1117', borderRadius: 8, padding: 16 },
  feedHead:      { margin: '0 0 10px', fontSize: 14, fontWeight: 700 },
  feedItem:      { margin: '0 0 6px', fontSize: 13, color: C.muted, lineHeight: 1.5 },
  improvedBox:   { background: '#0F1F0F', border: `1px solid #1A3A1A`, borderRadius: 10, padding: 20, marginBottom: 24 },
  improvedHead:  { margin: '0 0 12px', fontSize: 15, fontWeight: 600, color: '#3FB950' },
  improvedText:  { margin: '0 0 12px', fontSize: 14, color: C.text, lineHeight: 1.8 },
  keyPhrase:     { margin: 0, fontSize: 13, color: '#3FB950', fontStyle: 'italic' },
  resultActions: { textAlign: 'center' },
  nextBtn:       { background: C.gold, border: 'none', borderRadius: 8, padding: '13px 32px', color: '#000', fontWeight: 700, fontSize: 15, cursor: 'pointer' },
};
