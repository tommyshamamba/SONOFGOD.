import { useState, useEffect } from 'react';
import { generateAnswer, streamAnswer, getResume, getApiErrorMessage } from '../api';
import toast from 'react-hot-toast';

const QUESTION_TYPES = ['behavioral','technical','leadership','culture_fit','salary','situational'];
const QUESTION_BANKS = {
  behavioral:  ['Tell me about yourself.', 'Describe a time you overcame a major challenge.', 'Give an example of a time you showed leadership.', 'Tell me about a time you failed and what you learned.'],
  technical:   ['Walk me through how you would troubleshoot a network outage.', 'Explain the difference between TCP and UDP.', 'How do you approach a server that keeps going down?', 'What is VLAN and why is it used?'],
  leadership:  ['How do you handle conflict in a team?', 'Describe a time you had to influence someone without authority.', 'Tell me about a time you mentored someone.'],
  culture_fit: ['Why do you want to work here?', 'Where do you see yourself in 5 years?', 'What motivates you most in your work?'],
  salary:      ['What are your salary expectations?', 'How do you respond if the offer is below your expectation?'],
  situational: ['What would you do if a critical server went down 10 minutes before a big presentation?', 'How would you handle a situation where your manager is wrong?'],
};

export default function PracticePage() {
  const [resume,      setResume]      = useState(null);
  const [jobRole,     setJobRole]     = useState('');
  const [question,    setQuestion]    = useState('');
  const [qType,       setQType]       = useState('behavioral');
  const [answer,      setAnswer]      = useState(null);
  const [streaming,   setStreaming]   = useState('');
  const [loading,     setLoading]     = useState(false);

  useEffect(() => {
    getResume().then(r => setResume(r.data)).catch(() => {});
  }, []);

  const handleGenerate = async () => {
    if (!question.trim() || !jobRole.trim()) {
      toast.error('Enter a job role and question first');
      return;
    }
    setAnswer(null);
    setLoading(true);
    setStreaming('');

    try {
      const { data } = await generateAnswer({
        question, question_type: qType, job_role: jobRole,
        resume_id: resume?.id
      });
      setAnswer(data);
    } catch (err) {
      toast.error(getApiErrorMessage(err, 'Failed to generate answer'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <h2 style={styles.title}>💬 Practice Answers</h2>
        <p style={styles.sub}>Generate a personalized STAR answer for any interview question</p>

        {/* Job Role */}
        <div style={styles.field}>
          <label style={styles.label}>Target Job Role</label>
          <input style={styles.input} value={jobRole} onChange={e => setJobRole(e.target.value)}
            placeholder="e.g. Network Engineer, Data Center Technician, NOC Analyst" />
        </div>

        {/* Question Type */}
        <div style={styles.field}>
          <label style={styles.label}>Question Type</label>
          <div style={styles.tabs}>
            {QUESTION_TYPES.map(t => (
              <button key={t} onClick={() => setQType(t)}
                style={{ ...styles.tab, ...(qType === t ? styles.tabActive : {}) }}>
                {t.replace('_', ' ')}
              </button>
            ))}
          </div>
        </div>

        {/* Quick Pick */}
        <div style={styles.field}>
          <label style={styles.label}>Quick Pick a Question</label>
          <div style={styles.quickPick}>
            {QUESTION_BANKS[qType].map(q => (
              <button key={q} onClick={() => setQuestion(q)} style={styles.qBtn}>{q}</button>
            ))}
          </div>
        </div>

        {/* Custom Question */}
        <div style={styles.field}>
          <label style={styles.label}>Or Type Your Own Question</label>
          <textarea style={styles.textarea} value={question}
            onChange={e => setQuestion(e.target.value)} rows={3}
            placeholder="Paste any interview question here..." />
        </div>

        <button onClick={handleGenerate} disabled={loading} style={styles.btn}>
          {loading ? '⏳ Generating...' : '🚀 Generate STAR Answer'}
        </button>

        {/* Answer Output */}
        {answer && (
          <div style={styles.answerBox}>
            <h3 style={styles.answerTitle}>✅ Your STAR Answer</h3>
            <div style={styles.starBlock}>
              {['situation','task','action','result'].map(k => (
                <div key={k} style={styles.starSection}>
                  <span style={styles.starLabel}>{k.toUpperCase()}</span>
                  <p style={styles.starText}>{answer[k]}</p>
                </div>
              ))}
            </div>
            <div style={styles.keyPhrase}>
              <strong>🎯 Closing line:</strong> {answer.key_phrase}
            </div>
            <div style={styles.tips}>
              {answer.tips?.map((tip, i) => <p key={i} style={styles.tip}>💡 {tip}</p>)}
            </div>
            <p style={styles.duration}>⏱ Estimated speak time: ~{answer.duration_estimate}s</p>
          </div>
        )}
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623', green: '#3FB950' };
const styles = {
  page:        { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: 'system-ui, sans-serif', padding: '40px 24px' },
  container:   { maxWidth: 800, margin: '0 auto' },
  title:       { fontSize: 28, fontWeight: 700, margin: '0 0 8px' },
  sub:         { color: C.muted, margin: '0 0 32px' },
  field:       { marginBottom: 24 },
  label:       { display: 'block', fontSize: 14, fontWeight: 600, marginBottom: 8, color: C.text },
  input:       { width: '100%', background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: '12px 16px', color: C.text, fontSize: 15, boxSizing: 'border-box' },
  textarea:    { width: '100%', background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: '12px 16px', color: C.text, fontSize: 15, resize: 'vertical', boxSizing: 'border-box' },
  tabs:        { display: 'flex', gap: 8, flexWrap: 'wrap' },
  tab:         { padding: '6px 14px', borderRadius: 20, border: `1px solid ${C.border}`, background: 'transparent', color: C.muted, cursor: 'pointer', fontSize: 13 },
  tabActive:   { background: C.gold, border: `1px solid ${C.gold}`, color: '#000', fontWeight: 600 },
  quickPick:   { display: 'flex', flexDirection: 'column', gap: 8 },
  qBtn:        { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: '10px 14px', color: C.text, cursor: 'pointer', textAlign: 'left', fontSize: 14 },
  btn:         { width: '100%', background: C.gold, border: 'none', borderRadius: 8, padding: '14px', color: '#000', fontWeight: 700, fontSize: 16, cursor: 'pointer', marginBottom: 32 },
  answerBox:   { background: C.surface, border: `1px solid ${C.green}`, borderRadius: 12, padding: 28 },
  answerTitle: { margin: '0 0 20px', fontSize: 20, color: C.green },
  starBlock:   { display: 'flex', flexDirection: 'column', gap: 16, marginBottom: 20 },
  starSection: { background: '#0D1117', borderRadius: 8, padding: 16 },
  starLabel:   { display: 'inline-block', background: C.gold, color: '#000', fontSize: 11, fontWeight: 700, padding: '2px 8px', borderRadius: 4, marginBottom: 8 },
  starText:    { margin: 0, lineHeight: 1.7, color: C.text, fontSize: 15 },
  keyPhrase:   { background: '#1C2A1C', borderRadius: 8, padding: 14, marginBottom: 16, color: C.green, fontSize: 14 },
  tips:        { marginBottom: 12 },
  tip:         { margin: '0 0 8px', color: C.muted, fontSize: 14 },
  duration:    { color: C.muted, fontSize: 13, margin: 0 },
};
