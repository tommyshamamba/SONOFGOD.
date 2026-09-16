import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { getApiErrorMessage } from '../api';
import { useAuth } from '../hooks/useAuth';
import toast from 'react-hot-toast';

export default function RegisterPage() {
  const { register } = useAuth();
  const navigate     = useNavigate();
  const [form, setForm]     = useState({ full_name: '', email: '', password: '', confirm: '' });
  const [loading, setLoading] = useState(false);

  const handle = async (e) => {
    e.preventDefault();
    if (form.password !== form.confirm) { toast.error('Passwords do not match'); return; }
    if (form.password.length < 8)       { toast.error('Password must be at least 8 characters'); return; }
    setLoading(true);
    try {
      await register(form.email, form.password, form.full_name);
      toast.success('Account created! Let\'s nail those interviews 🎯');
      navigate('/');
    } catch (err) {
      toast.error(getApiErrorMessage(err, 'Registration failed'));
    } finally {
      setLoading(false);
    }
  };

  const f = (field) => ({ value: form[field], onChange: e => setForm({ ...form, [field]: e.target.value }) });

  return (
    <div style={styles.page}>
      <div style={styles.card}>
        <div style={styles.logoWrap}>
          <span style={styles.logo}>🎯</span>
          <h1 style={styles.brand}>Interview Nailer</h1>
          <p style={styles.tagline}>by AIsystemEngineering[InterviewPro]</p>
        </div>
        <h2 style={styles.heading}>Create your free account</h2>
        <form onSubmit={handle}>
          <div style={styles.field}>
            <label style={styles.label}>Full Name</label>
            <input style={styles.input} required placeholder="Fiston Matandi" {...f('full_name')} />
          </div>
          <div style={styles.field}>
            <label style={styles.label}>Email</label>
            <input style={styles.input} type="email" required placeholder="you@email.com" {...f('email')} />
          </div>
          <div style={styles.field}>
            <label style={styles.label}>Password</label>
            <input style={styles.input} type="password" required placeholder="Min. 8 characters" {...f('password')} />
          </div>
          <div style={styles.field}>
            <label style={styles.label}>Confirm Password</label>
            <input style={styles.input} type="password" required placeholder="••••••••" {...f('confirm')} />
          </div>
          <button type="submit" disabled={loading} style={styles.btn}>
            {loading ? '⏳ Creating account...' : 'Create Account →'}
          </button>
        </form>
        <p style={styles.switchText}>
          Already have an account?{' '}
          <Link to="/login" style={styles.link}>Sign in</Link>
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
  field:     { marginBottom: 16 },
  label:     { display: 'block', fontSize: 13, fontWeight: 600, color: C.text, marginBottom: 6, fontFamily: 'system-ui, sans-serif' },
  input:     { width: '100%', background: '#0D1117', border: `1px solid ${C.border}`, borderRadius: 8, padding: '11px 14px', color: C.text, fontSize: 15, boxSizing: 'border-box', fontFamily: 'system-ui, sans-serif' },
  btn:       { width: '100%', background: C.gold, border: 'none', borderRadius: 8, padding: 13, color: '#000', fontWeight: 700, fontSize: 16, cursor: 'pointer', marginTop: 8, fontFamily: 'system-ui, sans-serif' },
  switchText:{ marginTop: 20, textAlign: 'center', color: C.muted, fontSize: 14, fontFamily: 'system-ui, sans-serif' },
  link:      { color: C.gold, textDecoration: 'none', fontWeight: 600 },
};
