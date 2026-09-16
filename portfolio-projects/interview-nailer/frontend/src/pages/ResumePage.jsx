import { useState, useEffect, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { uploadResume, matchResume, getResume, getApiErrorMessage } from '../api';
import toast from 'react-hot-toast';

export default function ResumePage() {
  const [resume,    setResume]    = useState(null);
  const [extracted, setExtracted] = useState(null);
  const [match,     setMatch]     = useState(null);
  const [jobRole,   setJobRole]   = useState('');
  const [jobDesc,   setJobDesc]   = useState('');
  const [loading,   setLoading]   = useState(false);
  const [matching,  setMatching]  = useState(false);
  const [tab,       setTab]       = useState('skills'); // skills | experience | match

  useEffect(() => {
    getResume().then(r => {
      if (r.data) {
        setResume(r.data);
        setExtracted(r.data);
      }
    }).catch(() => {});
  }, []);

  const onDrop = useCallback(async (files) => {
    if (!files[0]) return;
    setLoading(true);
    const fd = new FormData();
    fd.append('resume', files[0]);
    try {
      const { data } = await uploadResume(fd);
      setResume(data.resume);
      setExtracted(data.extracted);
      toast.success('Resume analyzed! ✅');
      setTab('skills');
    } catch (err) {
      toast.error(getApiErrorMessage(err, 'Upload failed'));
    } finally {
      setLoading(false);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop, accept: { 'application/pdf': ['.pdf'], 'text/plain': ['.txt'] }, maxFiles: 1
  });

  const handleMatch = async () => {
    if (!resume || !jobRole) { toast.error('Upload a resume and enter a job role first'); return; }
    setMatching(true);
    try {
      const { data } = await matchResume({ resume_id: resume.id, job_role: jobRole, job_description: jobDesc });
      setMatch(data);
      setTab('match');
      toast.success('Match analysis complete!');
    } catch (err) { toast.error(getApiErrorMessage(err, 'Match analysis failed')); }
    finally { setMatching(false); }
  };

  const skills = [...new Set([...(extracted?.skills || []), ...(extracted?.technical_skills || [])])];
  const softSkills = extracted?.soft_skills || [];
  const experience = extracted?.experience || [];

  return (
    <div style={s.page}>
      <div style={s.container}>
        <h2 style={s.title}>📄 Resume Analyzer</h2>
        <p style={s.sub}>Upload your resume — AI will extract your skills and match them to any job</p>

        {/* Drop Zone */}
        <div {...getRootProps()} style={{ ...s.dropzone, ...(isDragActive ? s.dropzoneActive : {}) }}>
          <input {...getInputProps()} />
          {loading ? (
            <div style={s.dropContent}>
              <span style={s.dropIcon}>⏳</span>
              <p style={s.dropText}>Analyzing your resume with AI...</p>
            </div>
          ) : resume ? (
            <div style={s.dropContent}>
              <span style={s.dropIcon}>✅</span>
              <p style={s.dropText}>Resume loaded — drop a new file to replace</p>
              <p style={s.dropHint}>Uploaded on {new Date(resume.created_at).toLocaleDateString()}</p>
            </div>
          ) : (
            <div style={s.dropContent}>
              <span style={s.dropIcon}>📎</span>
              <p style={s.dropText}>{isDragActive ? 'Drop it!' : 'Drag & drop your resume here'}</p>
              <p style={s.dropHint}>PDF or TXT — max 5MB</p>
            </div>
          )}
        </div>

        {/* Summary Card */}
        {extracted?.summary && (
          <div style={s.summaryCard}>
            <p style={s.summaryLabel}>🤖 AI Profile Summary</p>
            <p style={s.summaryText}>{extracted.summary}</p>
          </div>
        )}

        {/* Job Match Section */}
        {resume && (
          <div style={s.matchSection}>
            <h3 style={s.matchTitle}>🎯 Match to a Job</h3>
            <div style={s.matchRow}>
              <input style={{ ...s.input, flex: 1 }} value={jobRole}
                onChange={e => setJobRole(e.target.value)}
                placeholder="Job role (e.g. Network Engineer)" />
              <button onClick={handleMatch} disabled={matching} style={s.matchBtn}>
                {matching ? '⏳' : 'Analyze Match'}
              </button>
            </div>
            <textarea style={s.textarea} value={jobDesc}
              onChange={e => setJobDesc(e.target.value)} rows={3}
              placeholder="Paste the job description for a more precise match (optional)" />
          </div>
        )}

        {/* Tabs */}
        {extracted && (
          <>
            <div style={s.tabs}>
              {['skills','experience','match'].map(t => (
                <button key={t} onClick={() => setTab(t)}
                  style={{ ...s.tab, ...(tab === t ? s.tabActive : {}) }}>
                  {t === 'skills' ? `Skills (${skills.length})` : t === 'experience' ? `Experience (${experience.length})` : 'Job Match'}
                </button>
              ))}
            </div>

            {/* Skills Tab */}
            {tab === 'skills' && (
              <div style={s.panel}>
                <div style={s.tagCloud}>
                  {skills.map(skill => <span key={skill} style={s.tag}>{skill}</span>)}
                </div>
                {softSkills.length > 0 && (
                  <>
                    <h4 style={s.panelSubhead}>Soft Skills</h4>
                    <div style={s.tagCloud}>
                      {softSkills.map(skill => <span key={skill} style={s.certTag}>{skill}</span>)}
                    </div>
                  </>
                )}
                {extracted.certifications?.length > 0 && (
                  <>
                    <h4 style={s.panelSubhead}>🏆 Certifications</h4>
                    <div style={s.tagCloud}>
                      {extracted.certifications.map(c => <span key={c} style={s.certTag}>{c}</span>)}
                    </div>
                  </>
                )}
                {extracted.achievements?.length > 0 && (
                  <>
                    <h4 style={s.panelSubhead}>⭐ Key Achievements</h4>
                    {extracted.achievements.map((a, i) => (
                      <p key={i} style={s.achievement}>• {a}</p>
                    ))}
                  </>
                )}
              </div>
            )}

            {/* Experience Tab */}
            {tab === 'experience' && (
              <div style={s.panel}>
                {experience.map((job, i) => (
                  <div key={i} style={s.jobCard}>
                    <div style={s.jobHeader}>
                      <span style={s.jobTitle}>{job.title}</span>
                      <span style={s.jobDates}>{job.dates}</span>
                    </div>
                    <span style={s.jobCompany}>{job.company}</span>
                    <ul style={s.jobBullets}>
                      {job.bullets?.map((b, j) => <li key={j} style={s.jobBullet}>{b}</li>)}
                    </ul>
                  </div>
                ))}
              </div>
            )}

            {/* Match Tab */}
            {tab === 'match' && match && (
              <div style={s.panel}>
                {/* Score Ring */}
                <div style={s.scoreWrap}>
                  <div style={{ ...s.scoreRing, borderColor: match.match_score >= 70 ? '#3FB950' : match.match_score >= 50 ? '#F5A623' : '#F85149' }}>
                    <span style={s.scoreNum}>{match.match_score}</span>
                    <span style={s.scorePct}>/ 100</span>
                  </div>
                  <div>
                    <p style={s.recommendedTitle}>Best Title to Apply For:</p>
                    <p style={s.recommendedValue}>{match.recommended_title}</p>
                    <p style={s.salaryRange}>💰 {match.salary_range}</p>
                  </div>
                </div>

                <div style={s.matchGrid}>
                  <div style={s.matchCol}>
                    <h4 style={{ ...s.matchColHead, color: '#3FB950' }}>✅ Matched Skills</h4>
                    {match.matched_skills?.map(sk => <p key={sk} style={s.matchItem}>• {sk}</p>)}
                  </div>
                  <div style={s.matchCol}>
                    <h4 style={{ ...s.matchColHead, color: '#F85149' }}>❌ Missing Skills</h4>
                    {match.missing_skills?.map(sk => <p key={sk} style={s.matchItem}>• {sk}</p>)}
                  </div>
                </div>

                <h4 style={s.panelSubhead}>🚀 Talking Points for This Role</h4>
                {match.talking_points?.map((p, i) => (
                  <div key={i} style={s.talkingPoint}>{p}</div>
                ))}
              </div>
            )}
            {tab === 'match' && !match && (
              <div style={s.panel}>
                <p style={{ color: '#8B949E', textAlign: 'center', padding: 40 }}>Enter a job role above and click "Analyze Match" to see your compatibility score.</p>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623', green: '#3FB950' };
const s = {
  page:           { minHeight: '100vh', background: C.bg, color: C.text, fontFamily: 'system-ui, sans-serif', padding: '40px 24px' },
  container:      { maxWidth: 860, margin: '0 auto' },
  title:          { fontSize: 28, fontWeight: 700, margin: '0 0 8px' },
  sub:            { color: C.muted, margin: '0 0 28px' },
  dropzone:       { border: `2px dashed ${C.border}`, borderRadius: 12, padding: 48, cursor: 'pointer', textAlign: 'center', transition: 'border-color 0.2s', marginBottom: 24 },
  dropzoneActive: { borderColor: C.gold },
  dropContent:    {},
  dropIcon:       { fontSize: 40 },
  dropText:       { margin: '12px 0 4px', fontSize: 16, color: C.text },
  dropHint:       { margin: 0, fontSize: 13, color: C.muted },
  summaryCard:    { background: '#1A2332', border: `1px solid #1E3A5F`, borderRadius: 10, padding: 20, marginBottom: 24 },
  summaryLabel:   { margin: '0 0 8px', fontSize: 13, fontWeight: 700, color: '#58A6FF' },
  summaryText:    { margin: 0, color: C.text, lineHeight: 1.7, fontSize: 15 },
  matchSection:   { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 24, marginBottom: 28 },
  matchTitle:     { margin: '0 0 16px', fontSize: 18, fontWeight: 600 },
  matchRow:       { display: 'flex', gap: 12, marginBottom: 12 },
  input:          { background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 15 },
  matchBtn:       { background: C.gold, border: 'none', borderRadius: 8, padding: '11px 20px', color: '#000', fontWeight: 700, cursor: 'pointer', whiteSpace: 'nowrap' },
  textarea:       { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 14, resize: 'vertical', boxSizing: 'border-box' },
  tabs:           { display: 'flex', gap: 4, borderBottom: `1px solid ${C.border}`, marginBottom: 24 },
  tab:            { padding: '10px 20px', background: 'transparent', border: 'none', color: C.muted, cursor: 'pointer', fontSize: 14, borderBottom: '2px solid transparent' },
  tabActive:      { color: C.gold, borderBottom: `2px solid ${C.gold}` },
  panel:          { background: C.surface, borderRadius: 12, padding: 24 },
  tagCloud:       { display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 20 },
  tag:            { background: '#1C2A3A', border: `1px solid #1E3A5F`, borderRadius: 20, padding: '5px 12px', fontSize: 13, color: '#58A6FF' },
  certTag:        { background: '#2A1C0F', border: `1px solid #5A3A1A`, borderRadius: 20, padding: '5px 12px', fontSize: 13, color: C.gold },
  panelSubhead:   { margin: '20px 0 12px', fontSize: 15, fontWeight: 600, color: C.text },
  achievement:    { margin: '0 0 8px', color: C.text, fontSize: 14, lineHeight: 1.6 },
  jobCard:        { background: '#0D1117', borderRadius: 10, padding: 18, marginBottom: 16 },
  jobHeader:      { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 },
  jobTitle:       { fontWeight: 700, fontSize: 15, color: C.text },
  jobDates:       { fontSize: 12, color: C.muted },
  jobCompany:     { fontSize: 13, color: C.gold, display: 'block', marginBottom: 10 },
  jobBullets:     { margin: 0, paddingLeft: 16 },
  jobBullet:      { fontSize: 13, color: C.muted, marginBottom: 4, lineHeight: 1.5 },
  scoreWrap:      { display: 'flex', alignItems: 'center', gap: 28, marginBottom: 28 },
  scoreRing:      { width: 100, height: 100, borderRadius: '50%', border: '6px solid', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flexShrink: 0 },
  scoreNum:       { fontSize: 28, fontWeight: 800, color: C.text },
  scorePct:       { fontSize: 11, color: C.muted },
  recommendedTitle:{ margin: '0 0 4px', fontSize: 12, color: C.muted },
  recommendedValue:{ margin: '0 0 6px', fontSize: 18, fontWeight: 700, color: C.gold },
  salaryRange:    { margin: 0, fontSize: 14, color: C.green },
  matchGrid:      { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 },
  matchCol:       { background: '#0D1117', borderRadius: 8, padding: 16 },
  matchColHead:   { margin: '0 0 12px', fontSize: 14, fontWeight: 700 },
  matchItem:      { margin: '0 0 6px', fontSize: 13, color: C.muted, lineHeight: 1.5 },
  talkingPoint:   { background: '#0D1117', borderRadius: 8, padding: 14, marginBottom: 10, fontSize: 14, color: C.text, lineHeight: 1.6, borderLeft: `3px solid ${C.gold}` },
};
