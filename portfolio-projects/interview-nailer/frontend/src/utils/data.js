export function parseMaybeJson(value) {
  if (value == null) {
    return null;
  }

  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  return value;
}

export function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function averageAnswerScore(answers) {
  const scores = asArray(answers)
    .map((answer) => Number(parseMaybeJson(answer.score)?.total))
    .filter((value) => Number.isFinite(value));

  if (!scores.length) {
    return 0;
  }

  return scores.reduce((sum, value) => sum + value, 0) / scores.length;
}
