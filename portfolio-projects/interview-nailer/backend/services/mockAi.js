const ROLE_SKILL_MAP = {
  network: ['routing', 'switching', 'tcp/ip', 'dns', 'dhcp', 'firewall', 'vlan', 'troubleshooting'],
  cloud: ['aws', 'azure', 'terraform', 'linux', 'automation', 'containers', 'networking', 'security'],
  datacenter: ['racking', 'cabling', 'power', 'cooling', 'inventory', 'troubleshooting', 'networking'],
  data: ['sql', 'python', 'analytics', 'reporting', 'visualization', 'troubleshooting'],
  support: ['ticketing', 'customer service', 'troubleshooting', 'documentation', 'communication'],
  default: ['troubleshooting', 'communication', 'documentation', 'ownership', 'collaboration'],
};

const KNOWN_SKILLS = [
  'aws',
  'azure',
  'gcp',
  'linux',
  'windows',
  'python',
  'javascript',
  'typescript',
  'react',
  'node.js',
  'node',
  'express',
  'postgresql',
  'sql',
  'docker',
  'kubernetes',
  'terraform',
  'ansible',
  'networking',
  'tcp/ip',
  'dns',
  'dhcp',
  'bgp',
  'ospf',
  'switching',
  'routing',
  'firewall',
  'vlan',
  'vpn',
  'monitoring',
  'incident response',
  'active directory',
  'virtualization',
  'vmware',
  'citrix',
  'security',
  'scripting',
];

const SOFT_SKILLS = [
  'communication',
  'leadership',
  'mentoring',
  'collaboration',
  'problem solving',
  'ownership',
  'adaptability',
  'documentation',
  'stakeholder management',
  'customer service',
];

function average(values) {
  if (!values.length) {
    return 0;
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function unique(values) {
  return [...new Set(values.filter(Boolean).map((value) => String(value).trim()).filter(Boolean))];
}

function titleCase(value) {
  return String(value || '')
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

function extractBlock(prompt, startMarker, endMarker) {
  const startIndex = prompt.indexOf(startMarker);
  if (startIndex === -1) {
    return '';
  }

  const contentStart = startIndex + startMarker.length;
  const endIndex = endMarker ? prompt.indexOf(endMarker, contentStart) : -1;
  const slice = endIndex === -1 ? prompt.slice(contentStart) : prompt.slice(contentStart, endIndex);
  return slice.trim();
}

function extractLine(prompt, label) {
  const match = prompt.match(new RegExp(`${label}\\s*(.+)`));
  return match ? match[1].trim() : '';
}

function extractQuotedValue(prompt, label) {
  const match = prompt.match(new RegExp(`${label}\\s*"([^"]+)"`));
  return match ? match[1].trim() : '';
}

function parseJsonBlock(raw) {
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function inferRoleCategory(jobRole = '') {
  const value = jobRole.toLowerCase();

  if (value.includes('network')) {
    return 'network';
  }

  if (value.includes('cloud')) {
    return 'cloud';
  }

  if (value.includes('data center') || value.includes('datacenter')) {
    return 'datacenter';
  }

  if (value.includes('analyst') || value.includes('data')) {
    return 'data';
  }

  if (value.includes('support') || value.includes('help desk')) {
    return 'support';
  }

  return 'default';
}

function inferRoleSkills(jobRole = '', jobDescription = '') {
  const category = inferRoleCategory(`${jobRole} ${jobDescription}`);
  return ROLE_SKILL_MAP[category] || ROLE_SKILL_MAP.default;
}

function findMatches(text, keywords) {
  const lower = text.toLowerCase();
  return unique(
    keywords.filter((keyword) => {
      const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      return new RegExp(`\\b${escaped}\\b`, 'i').test(lower);
    })
  );
}

function extractMetrics(text) {
  const matches = text.match(/\b\d+(?:\.\d+)?(?:%|\+|k|K|m|M)?\b/g) || [];
  return unique(matches).slice(0, 8);
}

function extractAchievements(text) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.replace(/^[\s\-*]+/, '').trim())
    .filter(Boolean);

  const numericLines = lines.filter((line) => /\d/.test(line));
  return (numericLines.length ? numericLines : lines).slice(0, 5);
}

function extractCertifications(text) {
  const certs = [];
  const patterns = [
    /aws certified [^\n,.;]*/gi,
    /azure [^\n,.;]*certified[^\n,.;]*/gi,
    /ccna[^\n,.;]*/gi,
    /ccnp[^\n,.;]*/gi,
    /comptia [^\n,.;]*/gi,
    /itil[^\n,.;]*/gi,
    /security\+[^\n,.;]*/gi,
  ];

  for (const pattern of patterns) {
    const found = text.match(pattern) || [];
    certs.push(...found.map((item) => item.trim()));
  }

  return unique(certs).slice(0, 6);
}

function extractExperience(text, titleHint) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const bulletLines = lines
    .filter((line) => /^[-*]/.test(line) || /\b(improved|reduced|delivered|led|built|managed|supported)\b/i.test(line))
    .map((line) => line.replace(/^[-*]\s*/, '').trim())
    .slice(0, 4);

  return [
    {
      company: 'Recent Employer',
      title: titleHint || 'Technical Professional',
      dates: 'Most recent role',
      bullets: bulletLines.length ? bulletLines : ['Delivered reliable support, troubleshooting, and project execution across core systems.'],
    },
  ];
}

function buildSummary(name, titleHint, skills) {
  const lead = name ? `${name} is a` : 'This candidate is a';
  const skillText = skills.length ? skills.slice(0, 4).join(', ') : 'technical troubleshooting and delivery';
  return `${lead} ${titleHint || 'technology professional'} with hands-on experience in ${skillText}. They show strong ownership, practical problem solving, and a track record of supporting production environments.`;
}

function normalizeScore(score) {
  if (!score) {
    return null;
  }

  if (typeof score === 'string') {
    try {
      return JSON.parse(score);
    } catch {
      return null;
    }
  }

  return score;
}

function gradeFor(total) {
  if (total >= 9) {
    return 'A';
  }
  if (total >= 7) {
    return 'B';
  }
  if (total >= 5) {
    return 'C';
  }
  if (total >= 3) {
    return 'D';
  }
  return 'F';
}

function mockResumeExtraction(prompt) {
  const resumeText = extractBlock(prompt, 'RESUME:\n"""\n', '\n"""');
  const lines = resumeText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const fullName = lines.find((line) => !/@/.test(line) && !/\d{3}/.test(line)) || 'Candidate';
  const titleHint = lines.find((line) => /engineer|administrator|analyst|technician|specialist/i.test(line)) || 'IT Professional';
  const skills = findMatches(resumeText, KNOWN_SKILLS);
  const technicalSkills = skills.filter((skill) => !SOFT_SKILLS.includes(skill));
  const softSkills = findMatches(resumeText, SOFT_SKILLS);
  const certifications = extractCertifications(resumeText);
  const achievements = extractAchievements(resumeText);
  const metrics = extractMetrics(resumeText);
  const strengths = unique([...softSkills.slice(0, 3), technicalSkills[0], technicalSkills[1]]).slice(0, 5);

  return {
    full_name: titleCase(fullName),
    title: titleCase(titleHint),
    summary: buildSummary(titleCase(fullName), titleCase(titleHint), skills),
    skills,
    technical_skills: technicalSkills,
    soft_skills: softSkills,
    certifications,
    experience: extractExperience(resumeText, titleCase(titleHint)),
    achievements,
    metrics,
    education: [],
    strengths,
    gaps: ['Quantified impact in every answer', 'Clear STAR storytelling under time pressure'],
  };
}

function mockSkillMatch(prompt) {
  const resumeData = parseJsonBlock(extractBlock(prompt, 'CANDIDATE PROFILE:\n', '\n\nTARGET ROLE:')) || {};
  const jobRole = extractLine(prompt, 'TARGET ROLE:');
  const jobDescription = extractBlock(prompt, 'JOB DESCRIPTION:\n"""\n', '\n"""') || extractLine(prompt, 'JOB DESCRIPTION:');
  const candidateSkills = unique([
    ...(resumeData.skills || []),
    ...(resumeData.technical_skills || []),
    ...(resumeData.soft_skills || []),
  ]).map((value) => value.toLowerCase());
  const roleSkills = inferRoleSkills(jobRole, jobDescription);
  const matchedSkills = roleSkills.filter((skill) => candidateSkills.includes(skill));
  const missingSkills = roleSkills.filter((skill) => !candidateSkills.includes(skill));
  const matchScore = clamp(Math.round(55 + matchedSkills.length * 7 - missingSkills.length * 2), 35, 96);

  return {
    match_score: matchScore,
    matched_skills: matchedSkills.map(titleCase),
    missing_skills: missingSkills.map(titleCase),
    strong_points: [
      'Hands-on troubleshooting experience that maps well to the role',
      'Transferable technical foundation across infrastructure and support work',
      'Clear potential to grow quickly with focused interview preparation',
    ],
    talking_points: [
      `Show how your experience with ${(matchedSkills[0] || roleSkills[0] || 'core infrastructure')} helped solve real production problems.`,
      'Use metrics and concrete outcomes in every interview story.',
      `Connect your background directly to what teams hiring ${jobRole} care about most: ownership, speed, and reliability.`,
    ],
    red_flags: missingSkills.length
      ? [`Be ready to address your plan for closing gaps in ${titleCase(missingSkills[0])}.`]
      : [],
    recommended_title: titleCase(jobRole || 'Technical Specialist'),
    salary_range: inferRoleCategory(jobRole) === 'cloud' ? '$95,000 - $130,000' : '$70,000 - $105,000',
  };
}

function mockGenerateAnswer(prompt) {
  const resumeData = parseJsonBlock(extractBlock(prompt, 'CANDIDATE BACKGROUND:\n', '\n\nTARGET ROLE:')) || {};
  const jobRole = extractLine(prompt, 'TARGET ROLE:');
  const questionType = extractLine(prompt, 'QUESTION TYPE:') || 'behavioral';
  const question = extractQuotedValue(prompt, 'QUESTION:');
  const experience = Array.isArray(resumeData.experience) ? resumeData.experience[0] || {} : {};
  const skills = unique([...(resumeData.skills || []), ...(resumeData.technical_skills || [])]).slice(0, 3);
  const metric = (resumeData.metrics || [])[0] || 'a measurable improvement in reliability and turnaround time';
  const company = experience.company || 'my previous team';
  const title = experience.title || resumeData.title || 'technical role';
  const actionSkills = skills.length ? skills.join(', ') : 'structured troubleshooting and clear communication';

  const situation = `In my recent ${title} role at ${company}, I faced a ${questionType.replace('_', ' ')} challenge that is very similar to "${question}".`;
  const task = 'My goal was to stabilize the situation quickly, keep stakeholders informed, and deliver a result the team could trust.';
  const action = `I broke the issue into clear steps, prioritized the highest-risk work first, collaborated with the right people, and used my background in ${actionSkills} to move from diagnosis to execution without losing momentum.`;
  const result = `That approach led to ${metric}, and it reinforced the disciplined, outcome-focused style I would bring to a ${jobRole} position.`;
  const answer = [situation, task, action, result].join(' ');

  return {
    answer,
    situation,
    task,
    action,
    result,
    key_phrase: 'I focus on solving the problem fast and leaving the system stronger than I found it.',
    duration_estimate: '60',
    tips: [
      'Slow down on the Action section so your specific decisions are clear.',
      'Anchor the Result with one concrete metric or business outcome.',
      `Close by tying the story back to why it matters for a ${jobRole} role.`,
    ],
  };
}

function buildQuestion(question, type, difficulty, whyAsked, hint, id) {
  return { id, question, type, difficulty, why_asked: whyAsked, hint };
}

function mockQuestions(prompt) {
  const resumeData = parseJsonBlock(extractBlock(prompt, 'CANDIDATE PROFILE:\n', '\n\nTARGET ROLE:')) || {};
  const jobRole = extractLine(prompt, 'TARGET ROLE:');
  const jobDescription = extractLine(prompt, 'JOB DESCRIPTION:');
  const difficulty = extractLine(prompt, 'DIFFICULTY:') || 'intermediate';
  const topSkill = (resumeData.skills || [])[0] || inferRoleSkills(jobRole, jobDescription)[0] || 'troubleshooting';
  const roleLabel = titleCase(jobRole || 'Technical Role');

  const questions = [
    buildQuestion(`Tell me about yourself and why ${roleLabel} is the right next step for you.`, 'behavioral', difficulty, 'They want a concise narrative and role fit.', 'Lead with your background, strengths, and what you want next.', 1),
    buildQuestion(`Describe a time you solved a high-pressure issue involving ${topSkill}.`, 'behavioral', difficulty, 'They are testing composure and ownership.', 'Use STAR and make the result measurable.', 2),
    buildQuestion(`Walk me through how you would troubleshoot a major outage in a ${roleLabel.toLowerCase()} environment.`, 'technical', difficulty, 'They want to hear your process under pressure.', 'Start with impact, then isolate, test, and communicate.', 3),
    buildQuestion('Tell me about a time you had to learn a new tool or technology quickly.', 'behavioral', difficulty, 'They are testing adaptability.', 'Show how you learned fast and applied it to real work.', 4),
    buildQuestion('How do you prioritize when several important tickets or incidents hit at once?', 'situational', difficulty, 'They care about judgment and stakeholder management.', 'Talk about severity, business impact, and communication.', 5),
    buildQuestion(`What would your manager say is your strongest contribution on a ${roleLabel.toLowerCase()} team?`, 'culture_fit', difficulty, 'They want evidence of self-awareness and value.', 'Use a real example from recent work.', 6),
    buildQuestion(`Give me an example of a time you improved a process, system, or workflow related to ${topSkill}.`, 'leadership', difficulty, 'They are testing initiative and impact.', 'Focus on what changed because of your action.', 7),
    buildQuestion('How do you document your work so other engineers can move quickly after you?', 'technical', difficulty, 'They want to hear about clarity and collaboration.', 'Explain your standards, not just that you document.', 8),
    buildQuestion('Describe a time you had to explain a technical issue to a non-technical stakeholder.', 'culture_fit', difficulty, 'They are testing communication range.', 'Make the translation process part of the story.', 9),
    buildQuestion(`What metrics would you track to know you're succeeding in a ${roleLabel.toLowerCase()} role?`, 'technical', difficulty, 'They are testing business awareness.', 'Mention uptime, response time, quality, and stakeholder trust.', 10),
    buildQuestion('Tell me about a mistake you made and how you handled it afterward.', 'behavioral', difficulty, 'They want accountability and maturity.', 'Own it fully and show the prevention step.', 11),
    buildQuestion('If we made you an offer, what would convince you this is the right team to join?', 'salary', difficulty, 'They want to understand your priorities and motivation.', 'Balance growth, impact, and compensation.', 12),
  ];

  return { questions };
}

function mockScore(prompt) {
  const question = extractQuotedValue(prompt, 'QUESTION:');
  const userAnswer = extractBlock(prompt, "CANDIDATE'S ANSWER:\n\"\"\"\n", '\n"""');
  const wordCount = userAnswer.trim().split(/\s+/).filter(Boolean).length;
  const hasStarWords = /(situation|task|action|result|first|then|finally)/i.test(userAnswer);
  const hasMetric = /\d/.test(userAnswer);
  const soundsConfident = /\bI\b/.test(userAnswer) && !/\bmaybe\b/i.test(userAnswer);

  const clarity = clamp(Math.round(wordCount / 25) + 3, 2, 9);
  const structure = hasStarWords ? 8 : wordCount > 80 ? 6 : 4;
  const relevance = question && userAnswer ? 7 : 5;
  const confidence = soundsConfident ? 7 : 5;
  const specificity = hasMetric ? 8 : wordCount > 100 ? 6 : 4;
  const total = Number(average([clarity, structure, relevance, confidence, specificity]).toFixed(1));

  return {
    scores: {
      clarity,
      structure,
      relevance,
      confidence,
      specificity,
    },
    total,
    grade: gradeFor(total),
    what_worked: [
      wordCount > 70 ? 'You provided enough detail to understand the story.' : 'Your answer stayed concise and easy to follow.',
      soundsConfident ? 'You used ownership language and spoke from direct experience.' : 'You stayed focused on the question and avoided rambling.',
    ],
    what_to_fix: [
      hasStarWords ? 'Tighten the Action section so your strongest decision stands out.' : 'Make the STAR structure more explicit so the answer lands cleanly.',
      hasMetric ? 'Push the business impact even harder in the closing sentence.' : 'Add one concrete metric or observable outcome.',
    ],
    missing_elements: [
      hasMetric ? 'A stronger closing line tied to the target role.' : 'A measurable result.',
      hasStarWords ? 'A sharper summary sentence at the end.' : 'A clearer distinction between the situation and your action.',
    ],
    improved_version: 'A stronger version would clearly frame the challenge, show the exact action you took, and finish with a quantified result tied to the value you would bring in the role.',
    one_liner_feedback: total >= 7 ? 'Solid answer with good substance; tighten the structure to sound more senior.' : 'Good foundation, but it needs sharper structure and more specific proof points.',
  };
}

function mockSalaryCoach(prompt) {
  const jobRole = extractLine(prompt, 'ROLE:');
  const currentSalary = extractLine(prompt, 'CURRENT SALARY:');
  const targetSalary = extractLine(prompt, 'TARGET SALARY:');
  const location = extractLine(prompt, 'LOCATION:');
  const range = inferRoleCategory(jobRole) === 'cloud' ? '$95,000 - $130,000' : '$70,000 - $105,000';
  const ask = targetSalary && !targetSalary.toLowerCase().includes('not specified') ? targetSalary : inferRoleCategory(jobRole) === 'cloud' ? '$118,000' : '$92,000';

  return {
    market_range: `${range} in ${location && !location.toLowerCase().includes('not specified') ? location : 'most major U.S. markets'}`,
    recommended_ask: `Anchor at ${ask} and tie it to your ability to deliver quickly in a ${jobRole} role.`,
    opening_script: `Based on the scope of the ${jobRole} role and the value I can bring, I am targeting a package around ${ask}.`,
    counter_script: `I appreciate the offer. Given my background and the impact expected in this role, is there room to move closer to ${ask}?`,
    anchoring_points: [
      'You can shorten ramp time because your background already maps to the core work.',
      'You bring practical troubleshooting and execution experience, not just theory.',
      'You are positioning yourself as someone who improves reliability and team velocity.',
    ],
    never_say: [
      `I will take anything above ${currentSalary || 'my current salary'}.`,
      'Money does not matter to me.',
      'I do not really know what the market is.',
    ],
    benefits_to_negotiate: ['Sign-on bonus', 'Professional development budget', 'Remote flexibility', 'Additional PTO'],
    walk_away_number: inferRoleCategory(jobRole) === 'cloud' ? '$102,000 if the role has strong growth potential' : '$80,000 if the scope and growth path are strong',
  };
}

function mockCoachingPlan(prompt) {
  const sessionHistory = parseJsonBlock(extractBlock(prompt, 'SESSION HISTORY (recent answers and scores):\n', '\n\nReturn ONLY valid JSON')) || [];
  const scores = sessionHistory
    .map((entry) => normalizeScore(entry.score))
    .filter(Boolean);
  const totals = scores.map((score) => Number(score.total)).filter((value) => Number.isFinite(value));
  const avg = totals.length ? average(totals) : 6.4;
  const readinessScore = Math.round(avg * 10);
  const categoryTotals = {};

  for (const score of scores) {
    for (const [key, value] of Object.entries(score.scores || {})) {
      if (!categoryTotals[key]) {
        categoryTotals[key] = [];
      }
      categoryTotals[key].push(Number(value));
    }
  }

  const rankedCategories = Object.entries(categoryTotals)
    .map(([key, values]) => [key, average(values)])
    .sort((a, b) => b[1] - a[1]);
  const topStrength = rankedCategories[0]?.[0] || 'clarity';
  const criticalWeakness = rankedCategories[rankedCategories.length - 1]?.[0] || 'specificity';

  return {
    overall_assessment: 'You already have a credible foundation, but your answers will land much harder with tighter structure and more explicit impact. The next step is turning good experience into memorable interview stories.',
    top_strength: `Your biggest asset right now is ${topStrength}.`,
    critical_weakness: `The main thing holding you back is ${criticalWeakness}.`,
    pattern_analysis: 'Across your answers, the strongest moments come when you explain what you personally did. The weaker moments are where the story stays generic or the result is not quantified.',
    weekly_plan: [
      { day: 'Day 1', focus: 'STAR structure', exercise: 'Rewrite three recent stories into strict Situation, Task, Action, Result bullets.', duration: '30 minutes' },
      { day: 'Day 2', focus: 'Specificity', exercise: 'Add one metric, tool, or business outcome to each of your best answers.', duration: '35 minutes' },
      { day: 'Day 3', focus: 'Technical depth', exercise: 'Practice two troubleshooting answers out loud and explain your process step by step.', duration: '40 minutes' },
      { day: 'Day 4', focus: 'Delivery', exercise: 'Record five spoken answers and remove filler words, long setup, and weak endings.', duration: '30 minutes' },
      { day: 'Day 5', focus: 'Mock repetition', exercise: 'Take another mock interview and aim to improve your average score by at least one point.', duration: '45 minutes' },
    ],
    questions_to_master: [
      'Tell me about yourself.',
      'Describe a time you solved a high-pressure issue.',
      'Walk me through your troubleshooting process.',
      'Tell me about a time you improved a process.',
      'Why are you a fit for this role?',
    ],
    ready_to_interview: readinessScore >= 70,
    readiness_score: readinessScore,
  };
}

function buildMockResponse(prompt) {
  if (prompt.includes('Analyze this resume and extract ALL useful data')) {
    return mockResumeExtraction(prompt);
  }

  if (prompt.includes('Analyze how well this candidate matches the target role')) {
    return mockSkillMatch(prompt);
  }

  if (prompt.includes('Generate a POWERFUL, AUTHENTIC interview answer')) {
    return mockGenerateAnswer(prompt);
  }

  if (prompt.includes('Generate exactly 12 questions')) {
    return mockQuestions(prompt);
  }

  if (prompt.includes('Score this interview answer with precision')) {
    return mockScore(prompt);
  }

  if (prompt.includes('Build a complete salary negotiation strategy')) {
    return mockSalaryCoach(prompt);
  }

  if (prompt.includes('build a personalized improvement plan')) {
    return mockCoachingPlan(prompt);
  }

  return { raw: 'Mock AI mode could not classify the prompt.' };
}

module.exports = {
  buildMockResponse,
};
