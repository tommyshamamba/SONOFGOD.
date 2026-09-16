import { useState, useEffect } from 'react';
import { getSalaryCoach, getResume, getApiErrorMessage } from '../api';
import toast from 'react-hot-toast';

export default function SalaryPage() {
  const [resume,  setResume]  = useState(null);
  const [form,    setForm]    = useState({ job_role: '', current_salary: '', target_salary: '', location: '' });
  const [result,  setResult]  = useState(null);
  const [loading, setLoading] = useState(false);
  const [tab,     setTab]     = useState('scripts');

  useEffect(() => { getResume().then(r => setResume(r.data)).catch(() => {}); }, []);

  const handle = async () => {
    if (!form.job_role) { toast.error('Enter the job role first'); return; }
    setLoading(true);
    try {
      const { data } = await getSalaryCoach({ ...form, resume_id: resume?.id });
      setResult(data);
      toast.success('Salary strategy ready 💰');
    } catch (err) { toast.error(getApiErrorMessage(err, 'Failed to generate strategy')); }
    finally { setLoading(false); }
  };

  const f = (field) => ({
    value: form[field],
    onChange: e => setForm({ ...form, [field]: e.target.value })
  });

  return (
    <div style={s.page}>
      <div style={s.container}>
        <h2 style={s.title}>💰 Salary Negotiation Coach</h2>
        <p style={s.sub}>Get exact scripts to negotiate your salary with confidence</p>

        <div style={s.card}>
          <div style={s.grid2}>
            <div style={s.field}>
              <label style={s.label}>Target Job Role *</label>
              <input style={s.input} placeholder="e.g. Network Engineer" {...f('job_role')} />
            </div>
            <div style={s.field}>
              <label style={s.label}>Location</label>
              <input style={s.input} placeholder="e.g. Dallas, TX" {...f('location')} />
            </div>
            <div style={s.field}>
              <label style={s.label}>Current Salary (optional)</label>
              <input style={s.input} placeholder="e.g. $65,000" {...f('current_salary')} />
            </div>
            <div style={s.field}>
              <label style={s.label}>Target Salary (optional)</label>
              <input style={s.input} placeholder="e.g. $85,000" {...f('target_salary')} />
            </div>
          </div>
          <button onClick={handle} disabled={loading} style={s.btn}>
            {loading ? '⏳ Building your strategy...' : '🚀 Generate Salary Strategy'}
          </button>
        </div>

        {result && (
          <>
            {/* Market range banner */}
            <div style={s.rangeBanner}>
              <div style={s.rangeItem}>
                <span style={s.rangeLabel}>Market Range</span>
                <span style={s.rangeVal}>{result.market_range}</span>
              </div>
              <div style={s.rangeDivider} />
              <div style={s.rangeItem}>
                <span style={s.rangeLabel}>You Should Ask For</span>
                <span style={{ ...s.rangeVal, color: '#3FB950' }}>{result.recommended_ask?.split(' ')[0]}</span>
              </div>
              <div style={s.rangeDivider} />
              <div style={s.rangeItem}>
                <span style={s.rangeLabel}>Walk Away Number</span>
                <span style={{ ...s.rangeVal, color: '#F85149' }}>{result.walk_away_number?.split(' ')[0]}</span>
              </div>
            </div>

            {/* Tabs */}
            <div style={s.tabs}>
              {['scripts', 'anchors', 'benefits', 'donts'].map(t => (
                <button key={t} onClick={() => setTab(t)}
                  style={{ ...s.tab, ...(tab === t ? s.tabActive : {}) }}>
                  {t === 'scripts' ? '🗣 Scripts' : t === 'anchors' ? '💪 Why You Deserve It' : t === 'benefits' ? '🎁 Benefits to Negotiate' : '🚫 Never Say This'}
                </button>
              ))}
            </div>

            <div style={s.panel}>
              {tab === 'scripts' && (
                <>
                  <div style={s.scriptBlock}>
                    <h4 style={s.scriptHead}>📌 When They Ask "What Are Your Salary Expectations?"</h4>
                    <div style={s.script}>{result.opening_script}</div>
                  </div>
                  <div style={s.scriptBlock}>
                    <h4 style={s.scriptHead}>📌 When Their Offer Comes In Low</h4>
                    <div style={s.script}>{result.counter_script}</div>
                  </div>
                  <div style={s.recAsk}>
                    <strong>💡 Strategy:</strong> {result.recommended_ask}
                  </div>
                </>
              )}
              {tab === 'anchors' && (
                <>
                  <p style={s.panelIntro}>Use these specific points when defending your number:</p>
                  {result.anchoring_points?.map((p, i) => (
                    <div key={i} style={s.anchorPoint}>
                      <span style={s.anchorNum}>{i + 1}</span>
                      <p style={s.anchorText}>{p}</p>
                    </div>
                  ))}
                </>
              )}
              {tab === 'benefits' && (
                <>
                  <p style={s.panelIntro}>If salary is fixed, negotiate these instead:</p>
                  {result.benefits_to_negotiate?.map((b, i) => (
                    <div key={i} style={s.benefitItem}>
                      <span style={s.benefitCheck}>✅</span>
                      <span>{b}</span>
                    </div>
                  ))}
                </>
              )}
              {tab === 'donts' && (
                <>
                  <p style={s.panelIntro}>Say any of these and you lose leverage instantly:</p>
                  {result.never_say?.map((n, i) => (
                    <div key={i} style={s.neverItem}>
                      <span style={s.neverX}>🚫</span>
                      <span style={s.neverText}>{n}</span>
                    </div>
                  ))}
                </>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623' };
const s = {
  page:         { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: 'system-ui, sans-serif', padding: '40px 24px' },
  container:    { maxWidth: 820, margin: '0 auto' },
  title:        { fontSize: 28, fontWeight: 700, margin: '0 0 8px' },
  sub:          { color: C.muted, margin: '0 0 28px' },
  card:         { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 14, padding: 28, marginBottom: 28 },
  grid2:        { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 20 },
  field:        { display: 'flex', flexDirection: 'column', gap: 6 },
  label:        { fontSize: 13, fontWeight: 600, color: C.text },
  input:        { background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 15 },
  btn:          { width: '100%', background: C.gold, border: 'none', borderRadius: 8, padding: 13, color: '#000', fontWeight: 700, fontSize: 16, cursor: 'pointer' },
  rangeBanner:  { display: 'flex', background: C.surface, border: `1px solid ${C.border}`, borderRadius: 14, padding: 24, marginBottom: 24, justifyContent: 'space-around' },
  rangeItem:    { textAlign: 'center', display: 'flex', flexDirection: 'column', gap: 6 },
  rangeLabel:   { fontSize: 12, color: C.muted, textTransform: 'uppercase', letterSpacing: 1 },
  rangeVal:     { fontSize: 22, fontWeight: 800, color: C.gold },
  rangeDivider: { width: 1, background: C.border },
  tabs:         { display: 'flex', gap: 4, borderBottom: `1px solid ${C.border}`, marginBottom: 24 },
  tab:          { padding: '10px 16px', background: 'transparent', border: 'none', color: C.muted, cursor: 'pointer', fontSize: 13, borderBottom: '2px solid transparent', whiteSpace: 'nowrap' },
  tabActive:    { color: C.gold, borderBottom: `2px solid ${C.gold}` },
  panel:        { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 28 },
  scriptBlock:  { marginBottom: 28 },
  scriptHead:   { margin: '0 0 12px', fontSize: 14, fontWeight: 700, color: C.text },
  script:       { background: '#0D1117', borderRadius: 10, padding: 20, borderLeft: `4px solid ${C.gold}`, fontSize: 15, lineHeight: 1.8, color: C.text, fontStyle: 'italic' },
  recAsk:       { background: '#1A2A0A', border: `1px solid #2A4A1A`, borderRadius: 8, padding: 16, fontSize: 14, color: '#3FB950', lineHeight: 1.6 },
  panelIntro:   { color: C.muted, fontSize: 14, margin: '0 0 20px' },
  anchorPoint:  { display: 'flex', gap: 16, alignItems: 'flex-start', marginBottom: 16, background: '#0D1117', borderRadius: 8, padding: 16 },
  anchorNum:    { background: C.gold, color: '#000', borderRadius: '50%', width: 28, height: 28, display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, fontSize: 14, flexShrink: 0 },
  anchorText:   { margin: 0, color: C.text, fontSize: 14, lineHeight: 1.6 },
  benefitItem:  { display: 'flex', gap: 12, alignItems: 'flex-start', padding: '12px 0', borderBottom: `1px solid ${C.border}`, fontSize: 14, color: C.text },
  benefitCheck: { fontSize: 18 },
  neverItem:    { display: 'flex', gap: 12, background: '#1A0A0A', border: `1px solid #3A1A1A`, borderRadius: 8, padding: 16, marginBottom: 12 },
  neverX:       { fontSize: 18, flexShrink: 0 },
  neverText:    { color: '#F85149', fontSize: 14, lineHeight: 1.6 },
};
