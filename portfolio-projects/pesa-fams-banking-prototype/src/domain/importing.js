function parseCsv(text) {
  const source = String(text || "").replace(/^\uFEFF/, "");
  const rows = [];
  let field = "";
  let row = [];
  let inQuotes = false;

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    const next = source[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(field);
      field = "";
      if (row.some((cell) => String(cell).trim() !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    field += char;
  }

  row.push(field);
  if (row.some((cell) => String(cell).trim() !== "")) {
    rows.push(row);
  }

  return rows;
}

function normalizeHeader(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function csvRowsToObjects(text) {
  const rows = parseCsv(text);
  if (!rows.length) return { headers: [], items: [] };

  const headers = rows[0].map(normalizeHeader);
  const items = rows.slice(1).map((row, rowIndex) => {
    const payload = {};
    headers.forEach((header, index) => {
      payload[header] = String(row[index] || "").trim();
    });
    return {
      rowNumber: rowIndex + 2,
      payload
    };
  });

  return { headers, items };
}

module.exports = {
  parseCsv,
  normalizeHeader,
  csvRowsToObjects
};
