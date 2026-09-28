const QUESTION_TYPES = ['behavioral', 'technical', 'leadership', 'culture_fit', 'salary', 'situational'];
const uuid = (value) => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
const text = (value, max = 20000) => typeof value === 'string' && value.trim().length > 0 && value.length <= max;
const optionalText = (value, max = 20000) => value == null || (typeof value === 'string' && value.length <= max);
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const number = (value, min, max) => typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
const list = (value, check = (item) => text(item, 5000)) => Array.isArray(value) && value.length <= 100 && value.every(check);
const strings = (data, keys) => keys.every((key) => text(data[key]));
const lists = (data, keys) => keys.every((key) => list(data[key]));

function validScore(data) {
  return object(data) && object(data.scores) &&
    ['clarity', 'structure', 'relevance', 'confidence', 'specificity'].every((key) => number(data.scores[key], 1, 10)) &&
    number(data.total, 1, 10) && ['A', 'B', 'C', 'D', 'F'].includes(data.grade) &&
    strings(data, ['improved_version', 'one_liner_feedback']) &&
    lists(data, ['what_worked', 'what_to_fix', 'missing_elements']);
}

class AIResponseError extends Error {
  constructor() { super('The AI service returned an invalid response. Please try again.'); this.status = 502; }
}

function validateAIResponse(kind, data) {
  let valid = object(data);
  if (valid) {
    switch (kind) {
      case 'questions':
        valid = list(data.questions, (item) => object(item) && Number.isInteger(item.id) && text(item.question) &&
          QUESTION_TYPES.includes(item.type) && ['easy', 'medium', 'hard'].includes(item.difficulty) && strings(item, ['hint', 'why_asked'])) &&
          data.questions.length > 0 && data.questions.length <= 30 && new Set(data.questions.map((item) => item.id)).size === data.questions.length;
        break;
      case 'resume':
        valid = strings(data, ['title', 'summary']) && lists(data, ['skills', 'technical_skills', 'soft_skills', 'certifications', 'achievements', 'metrics', 'strengths', 'gaps']) &&
          list(data.experience, (item) => object(item) && strings(item, ['company', 'title']) && optionalText(item.dates) && list(item.bullets)) &&
          list(data.education, (item) => object(item) && strings(item, ['degree', 'school']) && optionalText(item.year));
        break;
      case 'answer':
        valid = strings(data, ['answer', 'situation', 'task', 'action', 'result', 'key_phrase', 'duration_estimate']) && list(data.tips);
        break;
      case 'score': valid = validScore(data); break;
      case 'match':
        valid = number(data.match_score, 0, 100) && strings(data, ['recommended_title', 'salary_range']) &&
          lists(data, ['matched_skills', 'missing_skills', 'strong_points', 'talking_points', 'red_flags']);
        break;
      case 'salary':
        valid = strings(data, ['market_range', 'recommended_ask', 'opening_script', 'counter_script', 'walk_away_number']) &&
          lists(data, ['anchoring_points', 'never_say', 'benefits_to_negotiate']);
        break;
      case 'coaching':
        valid = strings(data, ['overall_assessment', 'top_strength', 'critical_weakness', 'pattern_analysis']) &&
          list(data.weekly_plan, (item) => object(item) && strings(item, ['day', 'focus', 'exercise', 'duration'])) &&
          data.weekly_plan.length > 0 && list(data.questions_to_master) && typeof data.ready_to_interview === 'boolean' && number(data.readiness_score, 0, 100);
        break;
      default: valid = false;
    }
  }
  if (!valid) throw new AIResponseError();
  return data;
}

module.exports = { QUESTION_TYPES, uuid, text, optionalText, object, validScore, validateAIResponse, AIResponseError };
