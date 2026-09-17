const test = require("node:test");
const assert = require("node:assert/strict");

const { reportCatalog } = require("../src/data/referenceData");
const { buildAssets, buildReconciliation } = require("../src/data/generator");
const { buildReportPack, buildReportCsv, buildReportExcelXml, buildPrintableHtml } = require("../src/domain/reporting");

const assets = buildAssets(64);

test("IAS 16 report pack builds preview and summary", () => {
  const report = reportCatalog.find((item) => item.id === "ias16");
  const pack = buildReportPack(report, assets, { generatedBy: "Test User", scopeLabel: "All branches" });

  assert.equal(pack.report.id, "ias16");
  assert.ok(pack.summary.length > 0);
  assert.equal(pack.preview.columns.length, 8);
  assert.ok(pack.preview.rows.length > 0);
  assert.equal(pack.exportData.rows.length, pack.preview.rows.length);
});

test("GL reconciliation report pack uses reconciliation input", () => {
  const report = reportCatalog.find((item) => item.id === "gl-reconciliation");
  const reconciliation = buildReconciliation(assets);
  const pack = buildReportPack(report, assets, { reconciliation, generatedBy: "Auditor" });

  assert.equal(pack.preview.rows.length, reconciliation.accounts.length);
  assert.equal(pack.summary[0].label, "Period");
});

test("report csv includes metadata and header row", () => {
  const report = reportCatalog.find((item) => item.id === "branch");
  const pack = buildReportPack(report, assets, { generatedBy: "Ops User" });
  const csv = buildReportCsv(pack);

  assert.ok(csv.startsWith("\uFEFF"));
  assert.ok(csv.includes('"Report","Assets by Branch"'));
  assert.ok(csv.includes('"Branch","City","Assets","Active","Pending Actions","Verification Coverage"'));
});

test("excel xml and printable html include the report title", () => {
  const report = reportCatalog.find((item) => item.id === "fully-depreciated");
  const pack = buildReportPack(report, assets, { generatedBy: "Finance User" });

  const xml = buildReportExcelXml(pack);
  const html = buildPrintableHtml(pack);

  assert.ok(xml.includes("<Workbook"));
  assert.ok(xml.includes(report.title));
  assert.ok(html.includes("<!doctype html>"));
  assert.ok(html.includes(report.title));
  assert.ok(html.includes("Prepared By"));
  assert.ok(html.includes("Approval Stamp"));
});
