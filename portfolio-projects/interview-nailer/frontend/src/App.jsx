import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import { useEffect, useState } from 'react';
import { getServiceStatus } from './api';

import { AuthProvider, useAuth } from './hooks/useAuth';
import LoginPage from './pages/LoginPage';
import RegisterPage from './pages/RegisterPage';
import DashboardPage from './pages/DashboardPage';
import ResumePage from './pages/ResumePage';
import PracticePage from './pages/PracticePage';
import MockInterviewPage from './pages/MockInterviewPage';
import HistoryPage from './pages/HistoryPage';
import SalaryPage from './pages/SalaryPage';
import CoachingPage from './pages/CoachingPage';

function PrivateRoute({ children }) {
  const { user } = useAuth();
  return user ? children : <Navigate to="/login" replace />;
}

function App() {
  const [mode, setMode] = useState('checking');
  useEffect(() => {
    let active = true;
    getServiceStatus().then(({ data }) => { if (active) setMode(data.ai_mode); })
      .catch(() => { if (active) setMode('unavailable'); });
    return () => { active = false; };
  }, []);
  return (
    <BrowserRouter>
      <AuthProvider>
        {mode !== 'anthropic' && <div role="status" style={{ background: '#fff1cc', color: '#302000', padding: '10px 20px', font: '14px system-ui', textAlign: 'center' }}>
          {mode === 'mock' ? 'Demo mode: sample responses and scores. No live AI provider is called.' : mode === 'checking' ? 'Checking service mode…' : 'Service connection unavailable. Check that the backend is running.'}
        </div>}
        <Toaster position="top-right" toastOptions={{ duration: 4000 }} />
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />

          <Route path="/" element={<PrivateRoute><DashboardPage /></PrivateRoute>} />
          <Route path="/resume" element={<PrivateRoute><ResumePage /></PrivateRoute>} />
          <Route path="/practice" element={<PrivateRoute><PracticePage /></PrivateRoute>} />
          <Route path="/mock" element={<PrivateRoute><MockInterviewPage /></PrivateRoute>} />
          <Route path="/history" element={<PrivateRoute><HistoryPage /></PrivateRoute>} />
          <Route path="/salary" element={<PrivateRoute><SalaryPage /></PrivateRoute>} />
          <Route path="/coaching/:sessionId" element={<PrivateRoute><CoachingPage /></PrivateRoute>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;
