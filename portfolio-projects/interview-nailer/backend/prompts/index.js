// ============================================================
//  KCA INTERVIEW NAILER — ALL AI PROMPTS
//  Every prompt used across the application
// ============================================================

const PROMPTS = {

  // ── 1. RESUME EXTRACTION ──────────────────────────────────
  resumeExtraction: (resumeText) => `
You are an elite career coach and resume analyst.

Analyze this resume and extract ALL useful data. Be thorough — miss nothing.

RESUME:
"""
${resumeText}
"""

Return ONLY a valid JSON object with this exact structure (no markdown, no explanation):
{
  "full_name": "string",
  "title": "string (current or target job title)",
  "summary": "string (2-3 sentence professional profile you write based on their background)",
  "skills": ["skill1", "skill2", ...],
  "technical_skills": ["skill1", "skill2", ...],
  "soft_skills": ["skill1", "skill2", ...],
  "certifications": ["cert1", "cert2", ...],
  "experience": [
    {
      "company": "string",
      "title": "string",
      "dates": "string",
      "bullets": ["bullet1", "bullet2", ...]
    }
  ],
  "achievements": ["achievement1", "achievement2", ...],
  "metrics": ["any numbers, percentages, counts found", ...],
  "education": [
    { "degree": "string", "school": "string", "year": "string" }
  ],
  "strengths": ["top 3-5 professional strengths inferred from the resume"],
  "gaps": ["any obvious skill gaps for senior roles in their field"]
}`,

  // ── 2. SKILL-TO-JOB MATCH ────────────────────────────────
  skillMatch: (resumeData, jobRole, jobDescription) => `
You are a senior technical recruiter.

Analyze how well this candidate matches the target role.

CANDIDATE PROFILE:
${JSON.stringify(resumeData, null, 2)}

TARGET ROLE: ${jobRole}
JOB DESCRIPTION:
"""
${jobDescription || 'Not provided — use standard requirements for ' + jobRole}
"""

Return ONLY valid JSON (no markdown):
{
  "match_score": number (0-100),
  "matched_skills": ["skills they have that match"],
  "missing_skills": ["skills the job needs that they lack"],
  "strong_points": ["their top 3 selling points for this role"],
  "talking_points": ["3 specific things they should emphasize in interviews"],
  "red_flags": ["anything a recruiter might question"],
  "recommended_title": "best job title they should apply for right now",
  "salary_range": "realistic range for this candidate/role/location"
}`,

  // ── 3. STAR ANSWER GENERATION ────────────────────────────
  generateAnswer: (question, questionType, jobRole, resumeData, tone) => `
You are an elite interview coach who has helped thousands of candidates land jobs at top companies.

Generate a POWERFUL, AUTHENTIC interview answer using the STAR method.

CANDIDATE BACKGROUND:
${JSON.stringify(resumeData, null, 2)}

TARGET ROLE: ${jobRole}
QUESTION TYPE: ${questionType}
QUESTION: "${question}"
TONE: ${tone || 'confident and professional'}

Rules:
- Use REAL details from their resume — make it feel personal, not generic
- STAR format: Situation → Task → Action → Result
- Length: 90-150 words (spoken in ~45-75 seconds)
- End with a strong closing line that ties back to the target role
- Never sound rehearsed — sound genuine
- Include a specific metric or outcome whenever possible

Return ONLY valid JSON (no markdown):
{
  "answer": "the full STAR answer",
  "situation": "the situation paragraph only",
  "task": "the task paragraph only",
  "action": "the action paragraph only",
  "result": "the result paragraph only",
  "key_phrase": "one memorable closing sentence",
  "duration_estimate": "estimated speaking time in seconds",
  "tips": ["2-3 delivery tips specific to this question"]
}`,

  // ── 4. MOCK INTERVIEW QUESTION GENERATOR ─────────────────
  generateQuestions: (jobRole, jobDescription, resumeData, difficulty) => `
You are a senior hiring manager at a top tech company.

Generate a realistic, challenging mock interview question set for this candidate.

CANDIDATE PROFILE:
${JSON.stringify({ skills: resumeData?.skills, experience: resumeData?.experience?.map(e => e.title), certifications: resumeData?.certifications }, null, 2)}

TARGET ROLE: ${jobRole}
JOB DESCRIPTION: ${jobDescription || 'Standard ' + jobRole + ' role'}
DIFFICULTY: ${difficulty || 'intermediate'}

Generate exactly 12 questions — the real questions this person WILL face.

Return ONLY valid JSON (no markdown):
{
  "questions": [
    {
      "id": number,
      "question": "string",
      "type": "behavioral|technical|leadership|culture_fit|salary|situational",
      "difficulty": "easy|medium|hard",
      "why_asked": "one sentence on what the interviewer is really testing",
      "hint": "one subtle tip to answer this well"
    }
  ]
}`,

  // ── 5. ANSWER SCORING ENGINE ─────────────────────────────
  scoreAnswer: (question, userAnswer, jobRole, resumeData) => `
You are a brutally honest but constructive interview coach.

Score this interview answer with precision.

QUESTION: "${question}"
TARGET ROLE: ${jobRole}
CANDIDATE BACKGROUND SUMMARY: ${resumeData?.summary || 'Not provided'}

CANDIDATE'S ANSWER:
"""
${userAnswer}
"""

Score each dimension from 1-10. Be specific — vague feedback helps no one.

Return ONLY valid JSON (no markdown):
{
  "scores": {
    "clarity": number,
    "structure": number,
    "relevance": number,
    "confidence": number,
    "specificity": number
  },
  "total": number (average of all scores),
  "grade": "A|B|C|D|F",
  "what_worked": ["specific things they did well"],
  "what_to_fix": ["specific things to improve"],
  "missing_elements": ["what they forgot to include"],
  "improved_version": "a rewritten version of their answer that scores 9+/10",
  "one_liner_feedback": "one punchy sentence summarizing their performance"
}`,

  // ── 6. SALARY NEGOTIATION COACH ──────────────────────────
  salaryCoach: (jobRole, currentSalary, targetSalary, location, resumeData) => `
You are a salary negotiation expert who has coached hundreds of professionals.

Build a complete salary negotiation strategy for this candidate.

ROLE: ${jobRole}
CURRENT SALARY: ${currentSalary || 'Not specified'}
TARGET SALARY: ${targetSalary || 'Not specified'}
LOCATION: ${location || 'Not specified'}
CANDIDATE STRENGTHS: ${resumeData?.strengths?.join(', ') || 'Not provided'}
CERTIFICATIONS: ${resumeData?.certifications?.join(', ') || 'None listed'}

Return ONLY valid JSON (no markdown):
{
  "market_range": "realistic range for this role/location",
  "recommended_ask": "specific number to ask for and why",
  "opening_script": "exact words to say when asked about salary",
  "counter_script": "exact words if they come in low",
  "anchoring_points": ["3 specific reasons you deserve this salary"],
  "never_say": ["3 things that kill salary negotiations"],
  "benefits_to_negotiate": ["if salary is fixed, negotiate these instead"],
  "walk_away_number": "the minimum they should accept and why"
}`,

  // ── 7. PERSONALIZED COACHING PLAN ────────────────────────
  coachingPlan: (sessionHistory, resumeData, jobRole) => `
You are a world-class career coach reviewing a candidate's interview practice history.

Analyze their performance patterns and build a personalized improvement plan.

CANDIDATE: ${resumeData?.full_name}
TARGET ROLE: ${jobRole}
SESSION HISTORY (recent answers and scores):
${JSON.stringify(sessionHistory, null, 2)}

Return ONLY valid JSON (no markdown):
{
  "overall_assessment": "2-3 sentence honest assessment",
  "top_strength": "their single biggest interview asset",
  "critical_weakness": "the one thing holding them back most",
  "pattern_analysis": "what patterns you see across their answers",
  "weekly_plan": [
    { "day": "Day 1", "focus": "string", "exercise": "specific practice exercise", "duration": "X minutes" },
    { "day": "Day 2", "focus": "string", "exercise": "string", "duration": "X minutes" },
    { "day": "Day 3", "focus": "string", "exercise": "string", "duration": "X minutes" },
    { "day": "Day 4", "focus": "string", "exercise": "string", "duration": "X minutes" },
    { "day": "Day 5", "focus": "string", "exercise": "string", "duration": "X minutes" }
  ],
  "questions_to_master": ["top 5 questions they must nail for this role"],
  "ready_to_interview": boolean,
  "readiness_score": number (0-100)
}`

};

module.exports = PROMPTS;
