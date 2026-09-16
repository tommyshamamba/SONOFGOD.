import { Link } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';

const CARDS = [
  { to: '/resume',   emoji: '📄', label: 'Upload Resume',      desc: 'Let AI extract your skills & experience' },
  { to: '/practice', emoji: '💬', label: 'Practice Answers',   desc: 'Generate STAR answers for any question' },
  { to: '/mock',     emoji: '🎯', label: 'Mock Interview',      desc: '12 real questions, scored live' },
  { to: '/salary',   emoji: '💰', label: 'Salary Negotiation', desc: 'Exact scripts to get paid more' },
  { to: '/history',  emoji: '📊', label: 'My History',         desc: 'Track progress across all sessions' },
];

export default function DashboardPage() {
  const { user, logout } = useAuth();

  return (
    <div style={styles.page}>
      <header style={styles.header}>
        <div>
          <h1 style={styles.logo}>🎯 Interview Nailer</h1>
          <p style={styles.sub}>by AIsystemEngineering[InterviewPro]</p>
        </div>
        <div style={styles.headerRight}>
          <span style={styles.greeting}>Welcome, {user?.full_name?.split(' ')[0] || 'Candidate'}</span>
          <button onClick={logout} style={styles.logoutBtn}>Log out</button>
        </div>
      </header>

      <main style={styles.main}>
        <h2 style={styles.headline}>What do you want to work on today?</h2>
        <div style={styles.grid}>
          {CARDS.map(card => (
            <Link key={card.to} to={card.to} style={styles.card}>
              <span style={styles.cardEmoji}>{card.emoji}</span>
              <h3 style={styles.cardTitle}>{card.label}</h3>
              <p style={styles.cardDesc}>{card.desc}</p>
            </Link>
          ))}
        </div>
      </main>
    </div>
  );
}

const styles = {
  page:        { minHeight: '100vh', background: '#0D1117', color: '#E6EDF3', fontFamily: 'system-ui, sans-serif' },
  header:      { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '24px 40px', borderBottom: '1px solid #21262D' },
  logo:        { margin: 0, fontSize: 24, color: '#F5A623' },
  sub:         { margin: '2px 0 0', fontSize: 12, color: '#8B949E' },
  headerRight: { display: 'flex', alignItems: 'center', gap: 16 },
  greeting:    { color: '#8B949E', fontSize: 14 },
  logoutBtn:   { background: 'transparent', border: '1px solid #30363D', color: '#8B949E', padding: '6px 16px', borderRadius: 6, cursor: 'pointer', fontSize: 14 },
  main:        { maxWidth: 900, margin: '60px auto', padding: '0 24px' },
  headline:    { fontSize: 28, fontWeight: 700, marginBottom: 32, color: '#E6EDF3' },
  grid:        { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 20 },
  card:        { display: 'block', background: '#161B22', border: '1px solid #21262D', borderRadius: 12, padding: 28, textDecoration: 'none', color: 'inherit', transition: 'border-color 0.2s, transform 0.2s' },
  cardEmoji:   { fontSize: 36, display: 'block', marginBottom: 12 },
  cardTitle:   { margin: '0 0 8px', fontSize: 18, fontWeight: 600, color: '#F0F6FC' },
  cardDesc:    { margin: 0, fontSize: 14, color: '#8B949E', lineHeight: 1.5 },
};
