import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getSessions } from '../api';

const gradeColor = { A:'#3FB950', B:'#58A6FF', C:'#F5A623', D:'#FF9F43', F:'#F85149' };

export default function HistoryPage() {
  const navigate           = useNavigate();
  const [sessions, setSessions] = useState([]);
  const [loading,  setLoading]  = useState(true);

  useEffect(() => {
    getSessions().then(r => setSessions(r.data)).catch(() => {}).finally(() => setLoading(false));
  }, []);

  const avgScore = sessions.filter(s => s.score_avg).reduce((acc, s, _, arr) => acc + Number(s.score_avg) / arr.length, 0);

  if (loading) return (
    <div style={{ ...s.page, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <p style={{ color: '#8B949E' }}>Loading your history...</p>
    </div>
  );

  return (
    <div style={s.page}>
      <div style={s.container}>
        <h2 style={s.title}>📊 Interview History</h2>
        <p style={s.sub}>Track your progress and see how far you've come</p>

        {/* Stats */}
        {sessions.length > 0 && (
          <div style={s.statsRow}>
            <div style={s.statCard}>
              <span style={s.statNum}>{sessions.length}</span>
              <span style={s.statLabel}>Sessions</span>
            </div>
            <div style={s.statCard}>
              <span style={s.statNum}>{sessions.filter(s => s.completed).length}</span>
              <span style={s.statLabel}>Completed</span>
            </div>
            <div style={s.statCard}>
              <span style={{ ...s.statNum, color: avgScore >= 7 ? '#3FB950' : avgScore >= 5 ? '#F5A623' : '#F85149' }}>
                {avgScore ? avgScore.toFixed(1) : '—'}
              </span>
              <span style={s.statLabel}>Avg Score</span>
            </div>
            <div style={s.statCard}>
              <span style={s.statNum}>{[...new Set(sessions.map(s => s.job_role))].length}</span>
              <span style={s.statLabel}>Roles Practiced</span>
            </div>
          </div>
        )}

        {/* Session list */}
        {sessions.length === 0 ? (
          <div style={s.empty}>
            <span style={{ fontSize: 56 }}>🎯</span>
            <h3 style={s.emptyHead}>No sessions yet</h3>
            <p style={s.emptySub}>Complete your first mock interview to see your history here</p>
            <button onClick={() => navigate('/mock')} style={s.startBtn}>Start a Mock Interview →</button>
          </div>
        ) : (
          <div style={s.list}>
            {sessions.map(session => {
              const score = session.score_avg ? Number(session.score_avg) : null;
              const grade = score >= 9 ? 'A' : score >= 7 ? 'B' : score >= 5 ? 'C' : score >= 3 ? 'D' : score ? 'F' : null;
              return (
                <div key={session.id} style={s.sessionCard}
                  onClick={() => session.completed && navigate(`/coaching/${session.id}`)}>
                  <div style={s.sessionLeft}>
                    <div style={s.sessionRole}>{session.job_role}</div>
                    <div style={s.sessionMeta}>
                      <span style={s.modeBadge}>{session.mode}</span>
                      <span style={s.sessionDate}>{new Date(session.created_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</span>
                      {session.completed ? <span style={s.completedBadge}>✅ Completed</span> : <span style={s.pendingBadge}>⏳ In Progress</span>}
                    </div>
                  </div>
                  <div style={s.sessionRight}>
                    {score && (
                      <div style={{ ...s.gradeCircle, borderColor: gradeColor[grade] }}>
                        <span style={{ color: gradeColor[grade], fontSize: 22, fontWeight: 800 }}>{grade}</span>
                        <span style={{ color: '#8B949E', fontSize: 11 }}>{score.toFixed(1)}/10</span>
                      </div>
                    )}
                    {session.completed && <span style={s.viewBtn}>View Report →</span>}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623' };
const s = {
  page:           { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: 'system-ui, sans-serif', padding: '40px 24px' },
  container:      { maxWidth: 820, margin: '0 auto' },
  title:          { fontSize: 28, fontWeight: 700, margin: '0 0 8px' },
  sub:            { color: C.muted, margin: '0 0 28px' },
  statsRow:       { display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 16, marginBottom: 32 },
  statCard:       { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: '20px 24px', textAlign: 'center', display: 'flex', flexDirection: 'column', gap: 4 },
  statNum:        { fontSize: 32, fontWeight: 800, color: C.gold },
  statLabel:      { fontSize: 12, color: C.muted, textTransform: 'uppercase', letterSpacing: 1 },
  list:           { display: 'flex', flexDirection: 'column', gap: 12 },
  sessionCard:    { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: '20px 24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: 'pointer', transition: 'border-color 0.2s' },
  sessionLeft:    {},
  sessionRole:    { fontSize: 18, fontWeight: 600, marginBottom: 8, color: C.text },
  sessionMeta:    { display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' },
  modeBadge:      { background: '#1C2A3A', color: '#58A6FF', padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600, textTransform: 'capitalize' },
  sessionDate:    { color: C.muted, fontSize: 13 },
  completedBadge: { color: '#3FB950', fontSize: 13 },
  pendingBadge:   { color: '#F5A623', fontSize: 13 },
  sessionRight:   { display: 'flex', alignItems: 'center', gap: 16 },
  gradeCircle:    { width: 64, height: 64, borderRadius: '50%', border: '3px solid', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' },
  viewBtn:        { fontSize: 13, color: C.gold, whiteSpace: 'nowrap' },
  empty:          { textAlign: 'center', padding: '80px 24px', background: C.surface, border: `1px solid ${C.border}`, borderRadius: 14 },
  emptyHead:      { fontSize: 20, margin: '16px 0 8px', color: C.text },
  emptySub:       { color: C.muted, margin: '0 0 28px' },
  startBtn:       { background: C.gold, border: 'none', borderRadius: 8, padding: '12px 28px', color: '#000', fontWeight: 700, fontSize: 15, cursor: 'pointer' },
};
