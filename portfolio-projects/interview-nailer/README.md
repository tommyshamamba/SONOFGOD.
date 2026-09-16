# Interview Nailer

Interview Nailer is a full-stack interview prep app with:

- resume upload and parsing
- job match analysis
- STAR answer generation
- mock interview sessions
- coaching reports
- salary negotiation guidance

## Local quick start

### 1. Install dependencies

```powershell
cd c:\Users\USER\banking-backend\banking-backend\interview-nailer
npm run install:backend
npm run install:frontend
```

### 2. Start backend

```powershell
cd backend
Copy-Item .env.example .env
npm run dev
```

### 3. Start frontend

```powershell
cd ..\frontend
Copy-Item .env.example .env
npm start
```

Open `http://localhost:3000`.

## Local modes

The default local setup uses:

- `STORAGE_MODE=file`
- `AI_MODE=mock`

That means you can run and demo the app without PostgreSQL or Anthropic.

## Free hosting on Render

This repo is prepared for a single public app URL on Render using:

- one free Render web service
- one free Render Postgres database
- mock AI mode so no Anthropic key is required

### What Render will do

- build the React frontend
- start the Express backend
- serve the frontend and API from the same `onrender.com` URL
- store users, sessions, and coaching data in Postgres

### Deploy steps

1. Push this `interview-nailer` folder to a GitHub repo.
2. Sign in to Render.
3. In Render, choose `New` -> `Blueprint`.
4. Connect the GitHub repo.
5. Render will detect [render.yaml](c:\Users\USER\banking-backend\banking-backend\interview-nailer\render.yaml).
6. Approve creation of:
   - `interview-nailer` web service
   - `interview-nailer-db` Postgres database
7. Wait for the first deploy to finish.
8. Open the generated `https://<your-app>.onrender.com` link.

### Important free-tier notes

According to Render’s official docs, free web services are available, but they spin down after 15 minutes of inactivity and may take about a minute to wake up again. Free Postgres is available too, but expires after 30 days unless upgraded.

Sources:

- Render free hosting docs: https://render.com/docs/free
- Render web services docs: https://render.com/docs/web-services
- Render blueprint docs: https://render.com/docs/blueprint-spec
- Render default environment variables: https://render.com/docs/environment-variables

## Real services later

If you want to switch from mock AI to Anthropic later, set:

```env
AI_MODE=anthropic
ANTHROPIC_API_KEY=your_key_here
```

If you want to run PostgreSQL locally instead of file mode, set:

```env
STORAGE_MODE=postgres
DATABASE_URL=postgresql://user:password@host:5432/interview_nailer
```
