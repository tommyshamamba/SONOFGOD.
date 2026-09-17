const test = require("node:test");
const assert = require("node:assert/strict");

const { parseCsv, csvRowsToObjects } = require("../src/domain/importing");

test("parseCsv handles quoted commas and line endings", () => {
  const rows = parseCsv('asset_id,name,description\r\nPESA-1,"Router, Branch","Primary router"\r\nPESA-2,Printer,"Line one"\n');

  assert.equal(rows.length, 3);
  assert.deepEqual(rows[1], ["PESA-1", "Router, Branch", "Primary router"]);
});

test("csvRowsToObjects normalizes headers and row numbers", () => {
  const payload = csvRowsToObjects("Asset ID,Tag Code,Branch Code\nPESA-1,TAG-001,BR-GOM\nPESA-2,TAG-002,BR-LUB");

  assert.deepEqual(payload.headers, ["asset_id", "tag_code", "branch_code"]);
  assert.equal(payload.items[0].rowNumber, 2);
  assert.equal(payload.items[1].payload.branch_code, "BR-LUB");
});
