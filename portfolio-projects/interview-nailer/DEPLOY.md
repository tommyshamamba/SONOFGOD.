# ============================================================
#  KCA INTERVIEW NAILER — COMPLETE DEPLOYMENT GUIDE
# ============================================================

## STACK OVERVIEW
  Frontend  → Vercel (free tier)
  Backend   → Railway.app (free tier / $5/mo)
  Database  → Neon.tech (free PostgreSQL) or Supabase
  Files     → Local for MVP, S3 for production

---

## STEP 1 — DATABASE SETUP (Neon.tech — Free)

1. Go to https://neon.tech → Create project "interview-nailer"
2. Copy the connection string (looks like: postgresql://user:pass@ep-xxx.neon.tech/neondb)
3. Open Neon SQL editor → paste and run: backend/config/schema.sql
4. Done — your DB is live

---

## STEP 2 — BACKEND DEPLOY (Railway.app)

1. Push your project to GitHub
2. Go to https://railway.app → New Project → Deploy from GitHub
3. Select your repo → Set root directory to "backend"
4. Add these environment variables in Railway dashboard:

   ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxx
   DATABASE_URL=postgresql://...your neon url...
   JWT_SECRET=pick-a-long-random-string-here
   JWT_EXPIRES_IN=7d
   NODE_ENV=production
   CLIENT_URL=https://your-app.vercel.app

5. Railway auto-detects Node.js and runs: npm start
6. Copy your Railway URL (e.g. https://interview-nailer-backend.railway.app)

---

## STEP 3 — FRONTEND DEPLOY (Vercel)

1. Go to https://vercel.com → Import your GitHub repo
2. Set root directory to "frontend"
3. Add environment variable:
   REACT_APP_API_URL=https://interview-nailer-backend.railway.app
4. Update frontend/src/api/index.js:
   Change: baseURL: '/api'
   To:     baseURL: process.env.REACT_APP_API_URL + '/api'
5. Deploy → Vercel gives you a .vercel.app URL

---

## STEP 4 — CONNECT THEM

Update Railway env var:
  CLIENT_URL=https://your-frontend.vercel.app

Update Vercel env var:
  REACT_APP_API_URL=https://your-backend.railway.app

Redeploy both. Done. ✅

---

## DOCKER (optional local dev)

```yaml
# docker-compose.yml
version: '3.8'
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: interview_nailer
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./backend/config/schema.sql:/docker-entrypoint-initdb.d/schema.sql

  backend:
    build: ./backend
    ports:
      - "5000:5000"
    environment:
      DATABASE_URL: postgresql://user:password@db:5432/interview_nailer
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      JWT_SECRET: localsecret
      NODE_ENV: development
      CLIENT_URL: http://localhost:3000
    depends_on:
      - db

  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    environment:
      REACT_APP_API_URL: http://localhost:5000
    depends_on:
      - backend

volumes:
  pgdata:
```

Run with: docker-compose up

---

## QUICK START (local dev without Docker)

# Terminal 1 — Database
# Make sure PostgreSQL is running locally or use Neon connection string

# Terminal 2 — Backend
cd backend
cp .env.example .env
# Fill in your ANTHROPIC_API_KEY and DATABASE_URL
npm install
npm run dev
# → Running on http://localhost:5000

# Terminal 3 — Frontend
cd frontend
npm install
npm start
# → Running on http://localhost:3000

---

## PRODUCTION CHECKLIST

[ ] ANTHROPIC_API_KEY is set and valid
[ ] DATABASE_URL points to live DB
[ ] JWT_SECRET is a strong random string (not "secret")
[ ] CLIENT_URL matches your actual frontend domain (CORS)
[ ] Rate limiting is active (already configured in server.js)
[ ] Schema.sql has been run against production DB
[ ] Test: POST /api/auth/register → should return token
[ ] Test: POST /api/resume/upload with PDF → should return skills
[ ] Test: POST /api/sessions/start → should return 12 questions
