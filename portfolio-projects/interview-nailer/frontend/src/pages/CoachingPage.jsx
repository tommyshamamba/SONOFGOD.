import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import toast from 'react-hot-toast';

import { getSession, getApiErrorMessage } from '../api';
import { asArray, averageAnswerScore, parseMaybeJson } from '../utils/data';

const FALLBACK_PLAN = [
  { day: 'Day 1', focus: 'STAR structure', exercise: 'Rewrite three stories into Situation, Task, Action, Result bullets.', duration: '30 minutes' },
  { day: 'Day 2', focus: 'Specificity', exercise: 'Add one metric or business outcome to each answer you plan to use again.', duration: '30 minutes' },
  { day: 'Day 3', focus: 'Technical depth', exercise: 'Practice two troubleshooting answers out loud and explain your process step by step.', duration: '40 minutes' },
  { day: 'Day 4', focus: 'Delivery', exercise: 'Record yourself answering five questions and tighten weak openings and endings.', duration: '30 minutes' },
  { day: 'Day 5', focus: 'Mock repeat', exercise: 'Take another mock interview and target a one-point score improvement.', duration: '45 minutes' },
];

function gradeFor(total) {
  if (total >= 9) {
    return 'A';
  }
  if (total >= 7) {
    return 'B';
  }
  if (total >= 5) {
    return 'C';
  }
  if (total >= 3) {
    return 'D';
  }
  return 'F';
}

const gradeColor = { A: '#3FB950', B: '#58A6FF', C: '#F5A623', D: '#FF9F43', F: '#F85149' };

export default function CoachingPage() {
  const { sessionId } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState('overview');

  useEffect(() => {
    getSession(sessionId)
      .then((response) => setData(response.data))
      .catch((err) => toast.error(getApiErrorMessage(err, 'Could not load session')))
      .finally(() => setLoading(false));
  }, [sessionId]);

  const derived = useMemo(() => {
    const session = data?.session || null;
    const answers = asArray(data?.answers);
    const coaching = parseMaybeJson(session?.coaching);
    const avgScore = averageAnswerScore(answers);
    const grade = gradeFor(avgScore);

    return {
      session,
      answers,
      coaching,
      avgScore,
      grade,
      readinessScore: coaching?.readiness_score ?? Math.round(avgScore * 10),
      weeklyPlan: asArray(coaching?.weekly_plan).length ? coaching.weekly_plan : FALLBACK_PLAN,
      readyToInterview: coaching?.ready_to_interview ?? avgScore >= 7,
      questionsToMaster: asArray(coaching?.questions_to_master),
    };
  }, [data]);

  if (loading) {
    return (
      <div style={{ ...s.page, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <p style={{ color: '#8B949E' }}>Loading your coaching report...</p>
      </div>
    );
  }

  if (!derived.session) {
    return null;
  }

  return (
    <div style={s.page}>
      <div style={s.container}>
        <div style={s.header}>
          <div>
            <h2 style={s.title}>Coaching Report</h2>
            <p style={s.sub}>
              {derived.session.job_role} · {new Date(derived.session.created_at).toLocaleDateString()}
            </p>
          </div>
          <button onClick={() => navigate('/mock')} style={s.primaryButton}>Start another mock</button>
        </div>

        <div style={s.hero}>
          <div style={s.gradeCard}>
            <div style={{ ...s.gradeBadge, borderColor: gradeColor[derived.grade] }}>
              <span style={{ ...s.gradeLetter, color: gradeColor[derived.grade] }}>{derived.grade}</span>
              <span style={s.gradeScore}>{derived.avgScore.toFixed(1)}/10</span>
            </div>
            <div>
              <p style={s.heroLabel}>Interview readiness</p>
              <p style={s.heroValue}>{derived.readinessScore}%</p>
            </div>
          </div>

          <div style={s.readinessCard}>
            <p style={s.readinessHeadline}>
              {derived.readyToInterview ? 'You are in a strong position to interview.' : 'A little more focused practice will raise your floor fast.'}
            </p>
            <p style={s.readinessBody}>
              {derived.coaching?.overall_assessment || 'Your report is based on the answers saved in this session. Focus on structure, specificity, and confidence to keep improving.'}
            </p>
          </div>
        </div>

        <div style={s.tabs}>
          {['overview', 'answers', 'plan'].map((value) => (
            <button
              key={value}
              onClick={() => setTab(value)}
              style={{ ...s.tab, ...(tab === value ? s.tabActive : {}) }}
            >
              {value === 'overview' ? 'Overview' : value === 'answers' ? `Answers (${derived.answers.length})` : 'Plan'}
            </button>
          ))}
        </div>

        {tab === 'overview' && (
          <div style={s.panel}>
            <div style={s.grid}>
              <div style={s.infoCard}>
                <p style={s.cardLabel}>Top strength</p>
                <p style={s.cardValue}>{derived.coaching?.top_strength || 'You are building a credible interview foundation.'}</p>
              </div>
              <div style={s.infoCard}>
                <p style={s.cardLabel}>Critical weakness</p>
                <p style={s.cardValue}>{derived.coaching?.critical_weakness || 'The next jump comes from sharper, more specific answers.'}</p>
              </div>
            </div>

            <div style={s.analysisCard}>
              <h3 style={s.sectionTitle}>Pattern analysis</h3>
              <p style={s.analysisText}>
                {derived.coaching?.pattern_analysis || 'Review where your answers got generic, where the STAR flow was tight, and where you can add one more proof point.'}
              </p>
            </div>

            <div style={s.analysisCard}>
              <h3 style={s.sectionTitle}>Question-by-question breakdown</h3>
              {derived.answers.map((answer, index) => {
                const score = parseMaybeJson(answer.score);
                return (
                  <div key={answer.id} style={s.answerRow}>
                    <div style={s.answerMeta}>
                      <span style={s.answerIndex}>Q{index + 1}</span>
                      <div>
                        <p style={s.answerQuestion}>{answer.question}</p>
                        <p style={s.answerFeedback}>{score?.one_liner_feedback || 'Review this answer for more specific proof points.'}</p>
                      </div>
                    </div>
                    <div style={{ ...s.answerGrade, color: gradeColor[score?.grade || derived.grade] }}>
                      {score?.grade || derived.grade}
                    </div>
                  </div>
                );
              })}
            </div>

            {derived.questionsToMaster.length > 0 && (
              <div style={s.analysisCard}>
                <h3 style={s.sectionTitle}>Questions to master next</h3>
                <div style={s.tagWrap}>
                  {derived.questionsToMaster.map((question) => (
                    <span key={question} style={s.tag}>{question}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {tab === 'answers' && (
          <div style={s.panel}>
            {derived.answers.map((answer, index) => {
              const score = parseMaybeJson(answer.score);
              return (
                <div key={answer.id} style={s.fullAnswerCard}>
                  <div style={s.fullAnswerHeader}>
                    <span style={s.fullAnswerIndex}>Q{index + 1}</span>
                    <span style={s.fullAnswerType}>{answer.question_type || 'general'}</span>
                    <span style={{ color: gradeColor[score?.grade || derived.grade], fontWeight: 700 }}>
                      {score?.grade || derived.grade} · {Number(score?.total || 0).toFixed(1)}/10
                    </span>
                  </div>
                  <p style={s.fullAnswerQuestion}>{answer.question}</p>

                  <div style={s.compareGrid}>
                    <div style={s.compareCard}>
                      <p style={s.compareLabel}>Your answer</p>
                      <p style={s.compareText}>{answer.user_answer || 'No answer saved.'}</p>
                    </div>
                    <div style={s.compareCardAlt}>
                      <p style={s.compareLabel}>Model answer</p>
                      <p style={s.compareText}>{answer.ai_answer || 'No model answer saved.'}</p>
                    </div>
                  </div>

                  {score?.scores && (
                    <div style={s.scoreGrid}>
                      {Object.entries(score.scores).map(([label, value]) => (
                        <div key={label} style={s.scoreChip}>
                          <span style={s.scoreChipLabel}>{label}</span>
                          <span style={s.scoreChipValue}>{value}/10</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {tab === 'plan' && (
          <div style={s.panel}>
            <div style={{ ...s.banner, ...(derived.readyToInterview ? s.bannerPositive : s.bannerWarning) }}>
              <p style={s.bannerTitle}>
                {derived.readyToInterview ? 'Ready to interview' : 'Practice recommended'}
              </p>
              <p style={s.bannerBody}>
                {derived.readyToInterview
                  ? 'Your latest session shows enough strength to apply and keep sharpening in parallel.'
                  : 'Focus on the plan below, then retake a mock to confirm the gains.'}
              </p>
            </div>

            {derived.weeklyPlan.map((item) => (
              <div key={item.day} style={s.planRow}>
                <div>
                  <p style={s.planDay}>{item.day}</p>
                  <p style={s.planFocus}>{item.focus}</p>
                </div>
                <p style={s.planExercise}>{item.exercise}</p>
                <p style={s.planDuration}>{item.duration}</p>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623' };
const s = {
  page: { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: '"Segoe UI", "Trebuchet MS", sans-serif', padding: '40px 24px' },
  container: { maxWidth: 960, margin: '0 auto' },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 20, marginBottom: 24 },
  title: { margin: '0 0 6px', fontSize: 30, fontWeight: 800 },
  sub: { margin: 0, color: C.muted },
  primaryButton: { background: C.gold, border: 'none', borderRadius: 10, padding: '12px 20px', color: '#000', fontWeight: 700, cursor: 'pointer' },
  hero: { display: 'grid', gridTemplateColumns: '280px 1fr', gap: 16, marginBottom: 24 },
  gradeCard: { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 16, padding: 24, display: 'flex', alignItems: 'center', gap: 18 },
  gradeBadge: { width: 108, height: 108, borderRadius: '50%', border: '4px solid', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' },
  gradeLetter: { fontSize: 38, fontWeight: 900 },
  gradeScore: { fontSize: 13, color: C.muted },
  heroLabel: { margin: '0 0 6px', color: C.muted, fontSize: 13, textTransform: 'uppercase', letterSpacing: 1 },
  heroValue: { margin: 0, fontSize: 32, fontWeight: 800 },
  readinessCard: { background: 'linear-gradient(135deg, rgba(245, 166, 35, 0.12), rgba(88, 166, 255, 0.08))', border: `1px solid ${C.border}`, borderRadius: 16, padding: 24 },
  readinessHeadline: { margin: '0 0 10px', fontSize: 20, fontWeight: 700 },
  readinessBody: { margin: 0, color: C.muted, lineHeight: 1.7 },
  tabs: { display: 'flex', gap: 8, marginBottom: 18 },
  tab: { padding: '10px 18px', borderRadius: 999, border: `1px solid ${C.border}`, background: 'transparent', color: C.muted, cursor: 'pointer' },
  tabActive: { background: C.gold, color: '#000', border: `1px solid ${C.gold}`, fontWeight: 700 },
  panel: { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 16, padding: 24 },
  grid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 },
  infoCard: { background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 14, padding: 18 },
  cardLabel: { margin: '0 0 8px', color: C.muted, fontSize: 12, textTransform: 'uppercase', letterSpacing: 1 },
  cardValue: { margin: 0, lineHeight: 1.6 },
  analysisCard: { background: '#0D1117', borderRadius: 14, padding: 20, marginBottom: 16 },
  sectionTitle: { margin: '0 0 16px', fontSize: 18, fontWeight: 700 },
  analysisText: { margin: 0, color: C.muted, lineHeight: 1.7 },
  answerRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, padding: '14px 0', borderBottom: `1px solid ${C.border}` },
  answerMeta: { display: 'flex', gap: 14, flex: 1 },
  answerIndex: { width: 34, height: 34, borderRadius: 10, background: '#1C2A3A', color: '#58A6FF', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, flexShrink: 0 },
  answerQuestion: { margin: '0 0 6px', fontWeight: 600, lineHeight: 1.5 },
  answerFeedback: { margin: 0, color: C.muted, fontSize: 14, lineHeight: 1.5 },
  answerGrade: { fontSize: 26, fontWeight: 900, flexShrink: 0 },
  tagWrap: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  tag: { background: '#1C2A3A', border: `1px solid #1E3A5F`, borderRadius: 999, padding: '7px 12px', fontSize: 13, color: '#58A6FF' },
  fullAnswerCard: { background: '#0D1117', borderRadius: 14, padding: 20, marginBottom: 16 },
  fullAnswerHeader: { display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap', marginBottom: 12 },
  fullAnswerIndex: { background: '#21262D', borderRadius: 10, padding: '4px 10px', fontWeight: 700, fontSize: 12 },
  fullAnswerType: { color: C.muted, textTransform: 'capitalize', fontSize: 13 },
  fullAnswerQuestion: { margin: '0 0 16px', fontWeight: 700, lineHeight: 1.5 },
  compareGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 16 },
  compareCard: { background: '#111A26', borderRadius: 12, padding: 16 },
  compareCardAlt: { background: '#102117', borderRadius: 12, padding: 16, border: '1px solid #1A3A1A' },
  compareLabel: { margin: '0 0 10px', color: C.muted, fontSize: 12, textTransform: 'uppercase', letterSpacing: 1 },
  compareText: { margin: 0, lineHeight: 1.7, whiteSpace: 'pre-wrap' },
  scoreGrid: { display: 'flex', flexWrap: 'wrap', gap: 10 },
  scoreChip: { background: C.surface, borderRadius: 10, padding: '10px 12px', display: 'flex', gap: 10, minWidth: 130, justifyContent: 'space-between' },
  scoreChipLabel: { color: C.muted, textTransform: 'capitalize', fontSize: 13 },
  scoreChipValue: { fontWeight: 700 },
  banner: { borderRadius: 14, padding: 18, marginBottom: 18, border: '1px solid transparent' },
  bannerPositive: { background: '#102117', borderColor: '#1A3A1A' },
  bannerWarning: { background: '#2A1C0F', borderColor: '#5A3A1A' },
  bannerTitle: { margin: '0 0 6px', fontWeight: 700, fontSize: 18 },
  bannerBody: { margin: 0, color: C.muted, lineHeight: 1.6 },
  planRow: { display: 'grid', gridTemplateColumns: '160px 1fr 120px', gap: 16, alignItems: 'start', background: '#0D1117', borderRadius: 12, padding: 18, marginBottom: 12 },
  planDay: { margin: '0 0 6px', color: C.gold, fontWeight: 700 },
  planFocus: { margin: 0, color: C.text },
  planExercise: { margin: 0, color: C.muted, lineHeight: 1.6 },
  planDuration: { margin: 0, color: C.text, fontWeight: 700, textAlign: 'right' },
};
