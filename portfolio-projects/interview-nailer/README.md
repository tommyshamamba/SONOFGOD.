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
cd portfolio-projects/interview-nailer
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

## Hosting configuration

The included [render.yaml](render.yaml) needs service-root configuration for this multi-project repository. Review the provider's current plans, persistence and schema initialization before deploying. No hosted deployment has been verified in this review. Consult [Render's configuration documentation](https://render.com/docs/blueprint-spec) for current requirements.

## Validation

Run `npm run build:frontend` from this folder to build the UI. There is no automated behavioral test command in the current manifest. The default mock AI mode does not establish real model inference or production agent experience. Review authentication, data isolation and provider integration before internet deployment.

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
