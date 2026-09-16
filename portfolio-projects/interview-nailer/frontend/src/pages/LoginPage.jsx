import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { getApiErrorMessage } from '../api';
import { useAuth } from '../hooks/useAuth';
import toast from 'react-hot-toast';

export default function LoginPage() {
  const { login }    = useAuth();
  const navigate     = useNavigate();
  const [form, setForm]     = useState({ email: '', password: '' });
  const [loading, setLoading] = useState(false);

  const handle = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      await login(form.email, form.password);
      toast.success('Welcome back!');
      navigate('/');
    } catch (err) {
      toast.error(getApiErrorMessage(err, 'Login failed'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.page}>
      <div style={styles.card}>
        <div style={styles.logoWrap}>
          <span style={styles.logo}>🎯</span>
          <h1 style={styles.brand}>Interview Nailer</h1>
          <p style={styles.tagline}>by AIsystemEngineering[InterviewPro]</p>
        </div>

        <h2 style={styles.heading}>Sign in to your account</h2>

        <form onSubmit={handle}>
          <div style={styles.field}>
            <label style={styles.label}>Email</label>
            <input style={styles.input} type="email" required
              value={form.email} onChange={e => setForm({ ...form, email: e.target.value })}
              placeholder="you@email.com" />
          </div>
          <div style={styles.field}>
            <label style={styles.label}>Password</label>
            <input style={styles.input} type="password" required
              value={form.password} onChange={e => setForm({ ...form, password: e.target.value })}
              placeholder="••••••••" />
          </div>
          <button type="submit" disabled={loading} style={styles.btn}>
            {loading ? '⏳ Signing in...' : 'Sign In →'}
          </button>
        </form>

        <p style={styles.switchText}>
          Don't have an account?{' '}
          <Link to="/register" style={styles.link}>Create one free</Link>
        </p>
      </div>
    </div>
  );
}

const C = { bg: '#0D1117', surface: '#161B22', border: '#21262D', text: '#E6EDF3', muted: '#8B949E', gold: '#F5A623' };
const styles = {
  page:      { minHeight: '100vh', background: C.bg, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 },
  card:      { background: C.surface, border: `1px solid ${C.border}`, borderRadius: 16, padding: '40px 36px', width: '100%', maxWidth: 420 },
  logoWrap:  { textAlign: 'center', marginBottom: 32 },
  logo:      { fontSize: 48 },
  brand:     { margin: '8px 0 4px', fontSize: 24, fontWeight: 700, color: C.gold },
  tagline:   { margin: 0, color: C.muted, fontSize: 13 },
  heading:   { fontSize: 18, fontWeight: 600, color: C.text, margin: '0 0 24px', fontFamily: 'system-ui, sans-serif' },
  field:     { marginBottom: 18 },
  label:     { display: 'block', fontSize: 13, fontWeight: 600, color: C.text, marginBottom: 6, fontFamily: 'system-ui, sans-serif' },
  input:     { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 15, boxSizing: 'border-box', fontFamily: 'system-ui, sans-serif' },
  btn:       { width: '100%', background: C.gold, border: 'none', borderRadius: 8, padding: 13, color: '#000', fontWeight: 700, fontSize: 16, cursor: 'pointer', marginTop: 8, fontFamily: 'system-ui, sans-serif' },
  switchText:{ marginTop: 20, textAlign: 'center', color: C.muted, fontSize: 14, fontFamily: 'system-ui, sans-serif' },
  link:      { color: C.gold, textDecoration: 'none', fontWeight: 600 },
};
