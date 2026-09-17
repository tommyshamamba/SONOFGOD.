const { categoryProfiles, statusPalette } = require("../data/referenceData");

function round(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function formatInteger(value) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value || 0));
}

function formatCurrency(value, currency) {
  const locale = currency === "CDF" ? "fr-CD" : "en-US";
  const prefix = currency === "CDF" ? "CDF " : "$";
  return `${prefix}${new Intl.NumberFormat(locale, { maximumFractionDigits: 0 }).format(Number(value || 0))}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeXml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function toCdf(value, currency, rateToCdf) {
  return currency === "CDF" ? round(value) : round(Number(value || 0) * Number(rateToCdf || 1));
}

function summarizeAssets(assets) {
  return {
    totalAssets: assets.length,
    grossCostUSD: round(assets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + Number(asset.acquisitionCost || asset.acquisition_cost || 0), 0)),
    grossCostCDF: round(assets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + Number(asset.acquisitionCost || asset.acquisition_cost || 0), 0)),
    nbvUSD: round(assets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + Number(asset.netBookValue || asset.net_book_value || 0), 0)),
    nbvCDF: round(assets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + Number(asset.netBookValue || asset.net_book_value || 0), 0))
  };
}

function buildCategoryRows(assets) {
  return categoryProfiles.map((category) => {
    const categoryAssets = assets.filter((asset) => asset.categoryKey === category.key || asset.category_key === category.key);
    return {
      category: category.label,
      method: category.method,
      usefulLifeMonths: category.usefulLifeMonths,
      assetCount: categoryAssets.length,
      grossCostUSD: round(categoryAssets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + Number(asset.acquisitionCost || asset.acquisition_cost || 0), 0)),
      grossCostCDF: round(categoryAssets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + Number(asset.acquisitionCost || asset.acquisition_cost || 0), 0)),
      accumulatedDepreciationUSD: round(categoryAssets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + Number(asset.accumulatedDepreciation || asset.accumulated_depreciation || 0), 0)),
      accumulatedDepreciationCDF: round(categoryAssets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + Number(asset.accumulatedDepreciation || asset.accumulated_depreciation || 0), 0)),
      nbvUSD: round(categoryAssets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + Number(asset.netBookValue || asset.net_book_value || 0), 0)),
      nbvCDF: round(categoryAssets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + Number(asset.netBookValue || asset.net_book_value || 0), 0))
    };
  }).filter((row) => row.assetCount > 0);
}

function buildBranchRows(assets) {
  const branchMap = new Map();

  for (const asset of assets) {
    const key = asset.branchCode || asset.branch_code || "UNKNOWN";
    if (!branchMap.has(key)) {
      branchMap.set(key, {
        branchCode: key,
        branchName: asset.branchName || asset.branch_name || key,
        city: asset.city || "",
        assetCount: 0,
        pendingActions: 0,
        activeCount: 0,
        verificationCoverage: 0
      });
    }

    const branch = branchMap.get(key);
    branch.assetCount += 1;
    if ((asset.status || "").toUpperCase() === "ACTIVE") branch.activeCount += 1;
    if (["PENDING", "TRANSFERRED", "HELD_FOR_SALE", "IMPAIRED"].includes((asset.status || "").toUpperCase())) branch.pendingActions += 1;
    const verifiedAt = asset.lastVerifiedAt || asset.last_verified_at;
    if (verifiedAt) {
      const ageInDays = (Date.now() - new Date(verifiedAt).getTime()) / 86400000;
      if (ageInDays <= 90) branch.verificationCoverage += 1;
    }
  }

  return Array.from(branchMap.values())
    .map((branch) => ({
      ...branch,
      verificationCoverage: branch.assetCount ? Math.round(branch.verificationCoverage / branch.assetCount * 100) : 0
    }))
    .sort((left, right) => right.assetCount - left.assetCount);
}

function buildReportPack(report, assets, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const generatedBy = options.generatedBy || "System";
  const scopeLabel = options.scopeLabel || "All branches";
  const rateToCdf = Number(options.exchangeRateToCdf || 2850);
  const assetSummary = summarizeAssets(assets);

  let columns = [];
  let previewRows = [];
  let exportRows = [];
  let summary = [];

  if (report.id === "ias16") {
    const rows = buildCategoryRows(assets);
    columns = ["Category", "Assets", "Gross Cost USD", "Gross Cost CDF", "Accumulated Depreciation USD", "Accumulated Depreciation CDF", "Closing NBV USD", "Closing NBV CDF"];
    previewRows = rows.map((row) => [row.category, formatInteger(row.assetCount), formatCurrency(row.grossCostUSD, "USD"), formatCurrency(row.grossCostCDF, "CDF"), formatCurrency(row.accumulatedDepreciationUSD, "USD"), formatCurrency(row.accumulatedDepreciationCDF, "CDF"), formatCurrency(row.nbvUSD, "USD"), formatCurrency(row.nbvCDF, "CDF")]);
    exportRows = rows.map((row) => [row.category, row.assetCount, row.grossCostUSD, row.grossCostCDF, row.accumulatedDepreciationUSD, row.accumulatedDepreciationCDF, row.nbvUSD, row.nbvCDF]);
    summary = [
      { label: "Total Assets", value: formatInteger(assetSummary.totalAssets) },
      { label: "Gross Cost USD", value: formatCurrency(assetSummary.grossCostUSD, "USD") },
      { label: "Gross Cost CDF", value: formatCurrency(assetSummary.grossCostCDF, "CDF") },
      { label: "Closing NBV USD", value: formatCurrency(assetSummary.nbvUSD, "USD") }
    ];
  } else if (report.id === "ohada") {
    const rows = buildCategoryRows(assets).map((row) => ({
      ...row,
      grossCostCdfTranslated: round(row.grossCostCDF + (row.grossCostUSD * rateToCdf)),
      accumulatedDepreciationCdfTranslated: round(row.accumulatedDepreciationCDF + (row.accumulatedDepreciationUSD * rateToCdf)),
      nbvCdfTranslated: round(row.nbvCDF + (row.nbvUSD * rateToCdf))
    }));
    columns = ["Category", "Method", "Useful Life (Months)", "Assets", "Gross Cost CDF", "Accumulated Depreciation CDF", "Closing NBV CDF"];
    previewRows = rows.map((row) => [row.category, row.method, formatInteger(row.usefulLifeMonths), formatInteger(row.assetCount), formatCurrency(row.grossCostCdfTranslated, "CDF"), formatCurrency(row.accumulatedDepreciationCdfTranslated, "CDF"), formatCurrency(row.nbvCdfTranslated, "CDF")]);
    exportRows = rows.map((row) => [row.category, row.method, row.usefulLifeMonths, row.assetCount, row.grossCostCdfTranslated, row.accumulatedDepreciationCdfTranslated, row.nbvCdfTranslated]);
    summary = [
      { label: "Translation Rate", value: `${formatInteger(rateToCdf)} CDF/USD` },
      { label: "Total Assets", value: formatInteger(assetSummary.totalAssets) },
      { label: "Translated NBV CDF", value: formatCurrency(assetSummary.nbvCDF + (assetSummary.nbvUSD * rateToCdf), "CDF") },
      { label: "Scope", value: scopeLabel }
    ];
  } else if (report.id === "gl-reconciliation") {
    const reconciliation = options.reconciliation || { accounts: [], varianceUSD: 0, varianceCDF: 0, discrepancyCount: 0, status: "UNKNOWN", period: "N/A" };
    columns = ["GL Code", "Label", "Variance USD", "Variance CDF", "Status"];
    previewRows = reconciliation.accounts.map((row) => [row.glCode, row.label, formatCurrency(row.varianceUSD, "USD"), formatCurrency(row.varianceCDF, "CDF"), row.status]);
    exportRows = reconciliation.accounts.map((row) => [row.glCode, row.label, row.varianceUSD, row.varianceCDF, row.status]);
    summary = [
      { label: "Period", value: reconciliation.period || "N/A" },
      { label: "Status", value: reconciliation.status || "Unknown" },
      { label: "Variance USD", value: formatCurrency(reconciliation.varianceUSD || 0, "USD") },
      { label: "Variance CDF", value: formatCurrency(reconciliation.varianceCDF || 0, "CDF") }
    ];
  } else if (report.id === "branch") {
    const rows = buildBranchRows(assets).slice(0, 15);
    columns = ["Branch", "City", "Assets", "Active", "Pending Actions", "Verification Coverage"];
    previewRows = rows.map((row) => [row.branchName, row.city, formatInteger(row.assetCount), formatInteger(row.activeCount), formatInteger(row.pendingActions), `${row.verificationCoverage}%`]);
    exportRows = rows.map((row) => [row.branchName, row.city, row.assetCount, row.activeCount, row.pendingActions, row.verificationCoverage]);
    summary = [
      { label: "Branches Covered", value: formatInteger(rows.length) },
      { label: "Visible Assets", value: formatInteger(assetSummary.totalAssets) },
      { label: "Scope", value: scopeLabel },
      { label: "Generated By", value: generatedBy }
    ];
  } else if (report.id === "disposal") {
    const rows = assets
      .filter((asset) => ["HELD_FOR_SALE", "DISPOSED"].includes((asset.status || "").toUpperCase()))
      .slice(0, 100)
      .map((asset) => ({
        assetId: asset.assetId || asset.asset_id,
        tagCode: asset.tagCode || asset.tag_code,
        name: asset.name,
        branchName: asset.branchName || asset.branch_name,
        category: asset.category,
        status: statusPalette[(asset.status || "").toUpperCase()]?.label || asset.status,
        currency: asset.currency,
        nbv: round(asset.netBookValue || asset.net_book_value || 0)
      }));
    columns = ["Asset ID", "Tag", "Asset", "Branch", "Category", "Status", "NBV"];
    previewRows = rows.map((row) => [row.assetId, row.tagCode, row.name, row.branchName, row.category, row.status, formatCurrency(row.nbv, row.currency)]);
    exportRows = rows.map((row) => [row.assetId, row.tagCode, row.name, row.branchName, row.category, row.status, row.nbv]);
    summary = [
      { label: "Disposal Candidates", value: formatInteger(rows.length) },
      { label: "Scope", value: scopeLabel },
      { label: "Generated By", value: generatedBy },
      { label: "Generated At", value: new Date(generatedAt).toLocaleString("en-GB") }
    ];
  } else {
    const rows = assets
      .filter((asset) => {
        const netBookValue = Number(asset.netBookValue || asset.net_book_value || 0);
        const residualValue = Number(asset.residualValue || asset.residual_value || 0);
        const method = asset.depreciationMethod || asset.depreciation_method;
        return method !== "NONE" && netBookValue <= residualValue + 1;
      })
      .slice(0, 100)
      .map((asset) => ({
        assetId: asset.assetId || asset.asset_id,
        name: asset.name,
        branchName: asset.branchName || asset.branch_name,
        category: asset.category,
        lastVerifiedAt: asset.lastVerifiedAt || asset.last_verified_at || "",
        currency: asset.currency,
        nbv: round(asset.netBookValue || asset.net_book_value || 0)
      }));
    columns = ["Asset ID", "Asset", "Branch", "Category", "Last Verified", "Residual NBV"];
    previewRows = rows.map((row) => [row.assetId, row.name, row.branchName, row.category, row.lastVerifiedAt || "-", formatCurrency(row.nbv, row.currency)]);
    exportRows = rows.map((row) => [row.assetId, row.name, row.branchName, row.category, row.lastVerifiedAt || "", row.nbv]);
    summary = [
      { label: "Fully Depreciated Assets", value: formatInteger(rows.length) },
      { label: "Scope", value: scopeLabel },
      { label: "Generated By", value: generatedBy },
      { label: "Generated At", value: new Date(generatedAt).toLocaleString("en-GB") }
    ];
  }

  return {
    report,
    generatedAt,
    generatedBy,
    scopeLabel,
    summary,
    preview: { columns, rows: previewRows },
    exportData: { columns, rows: exportRows }
  };
}

function buildReportCsv(pack) {
  const lines = [
    [`Report`, pack.report.title],
    [`Generated At`, new Date(pack.generatedAt).toISOString()],
    [`Generated By`, pack.generatedBy],
    [`Scope`, pack.scopeLabel],
    []
  ];

  for (const item of pack.summary) {
    lines.push([item.label, item.value]);
  }

  lines.push([]);
  lines.push(pack.exportData.columns);
  lines.push(...pack.exportData.rows);

  return `\uFEFF${lines.map((row) => row.map((cell) => `"${String(cell ?? "").replaceAll('"', '""')}"`).join(",")).join("\n")}`;
}

function buildReportExcelXml(pack) {
  const summaryRows = [
    ["Report", pack.report.title],
    ["Generated At", new Date(pack.generatedAt).toISOString()],
    ["Generated By", pack.generatedBy],
    ["Scope", pack.scopeLabel],
    ...pack.summary.map((item) => [item.label, item.value])
  ];

  const xmlRow = (cells) => `<Row>${cells.map((cell) => {
    const isNumber = typeof cell === "number" && Number.isFinite(cell);
    return `<Cell><Data ss:Type="${isNumber ? "Number" : "String"}">${escapeXml(cell ?? "")}</Data></Cell>`;
  }).join("")}</Row>`;

  return `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:html="http://www.w3.org/TR/REC-html40">
  <Worksheet ss:Name="Summary">
    <Table>
      ${summaryRows.map(xmlRow).join("")}
    </Table>
  </Worksheet>
  <Worksheet ss:Name="Data">
    <Table>
      ${xmlRow(pack.exportData.columns)}
      ${pack.exportData.rows.map(xmlRow).join("")}
    </Table>
  </Worksheet>
</Workbook>`;
}

function buildPrintableHtml(pack) {
  const summaryCards = pack.summary.map((item) => `<div class="summary-card"><span>${escapeHtml(item.label)}</span><strong>${escapeHtml(item.value)}</strong></div>`).join("");
  const tableHead = `<tr>${pack.preview.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>`;
  const tableBody = pack.preview.rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>${escapeHtml(pack.report.title)}</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: "Segoe UI", Arial, sans-serif; margin: 32px; color: #0f172a; }
      body:before { content: "PESA FAMS"; position: fixed; inset: 0; display: grid; place-items: center; font-size: 72px; font-weight: 800; color: rgba(15, 37, 87, 0.05); transform: rotate(-28deg); pointer-events: none; }
      header { display: flex; justify-content: space-between; gap: 24px; border-bottom: 3px solid #10285d; padding-bottom: 16px; margin-bottom: 24px; }
      .eyebrow { color: #c9a84c; font-size: 11px; font-weight: 700; letter-spacing: .16em; text-transform: uppercase; }
      h1 { margin: 6px 0 0; font-size: 28px; }
      .meta { color: #475569; font-size: 13px; line-height: 1.7; }
      .summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 24px; }
      .summary-card { border: 1px solid #cbd5e1; padding: 12px; }
      .summary-card span { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: #64748b; margin-bottom: 8px; }
      .summary-card strong { font-size: 18px; }
      .signoff { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin: 24px 0; }
      .signoff-card { border: 1px solid #cbd5e1; padding: 14px; min-height: 110px; }
      .signoff-card span { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: #64748b; margin-bottom: 18px; }
      .line { border-top: 1px solid #94a3b8; margin-top: 44px; padding-top: 8px; color: #475569; font-size: 12px; }
      table { width: 100%; border-collapse: collapse; }
      th, td { border: 1px solid #cbd5e1; padding: 10px; font-size: 12px; text-align: left; vertical-align: top; }
      th { background: #e2e8f0; text-transform: uppercase; letter-spacing: .08em; font-size: 11px; }
      @media print { body { margin: 14mm; } }
    </style>
  </head>
  <body>
    <header>
      <div>
        <div class="eyebrow">PESA FAMS</div>
        <h1>${escapeHtml(pack.report.title)}</h1>
      </div>
      <div class="meta">
        <div><strong>Generated:</strong> ${escapeHtml(new Date(pack.generatedAt).toLocaleString("en-GB"))}</div>
        <div><strong>Generated By:</strong> ${escapeHtml(pack.generatedBy)}</div>
        <div><strong>Scope:</strong> ${escapeHtml(pack.scopeLabel)}</div>
      </div>
    </header>
    <section class="summary">${summaryCards}</section>
    <section class="signoff">
      <div class="signoff-card"><span>Prepared By</span><strong>${escapeHtml(pack.generatedBy)}</strong><div class="line">Signature / Date</div></div>
      <div class="signoff-card"><span>Reviewed By</span><strong>Finance Controller</strong><div class="line">Signature / Date</div></div>
      <div class="signoff-card"><span>Approval Stamp</span><strong>Bank DRC Fixed Asset Control</strong><div class="line">Checker / Audit Stamp</div></div>
    </section>
    <table>
      <thead>${tableHead}</thead>
      <tbody>${tableBody}</tbody>
    </table>
  </body>
</html>`;
}

module.exports = {
  buildReportPack,
  buildReportCsv,
  buildReportExcelXml,
  buildPrintableHtml
};
