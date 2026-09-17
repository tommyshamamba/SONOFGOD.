const { branches, categoryProfiles, demoUsers, reportCatalog, statusPalette } = require("./referenceData");

const AS_OF_DATE = new Date("2026-03-31T00:00:00.000Z");

function randomUnit(seed) {
  const raw = Math.sin(seed * 12.9898) * 43758.5453123;
  return raw - Math.floor(raw);
}

function roundCurrency(amount) {
  return Math.round(amount * 100) / 100;
}

function monthDifference(start, end) {
  const years = end.getUTCFullYear() - start.getUTCFullYear();
  const months = end.getUTCMonth() - start.getUTCMonth();
  let total = years * 12 + months;
  if (end.getUTCDate() < start.getUTCDate()) total -= 1;
  return Math.max(total, 0);
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function depreciationSnapshot(asset, asOf = AS_OF_DATE) {
  if (asset.depreciationMethod === "NONE") {
    return { monthlyCharge: 0, accumulatedDepreciation: 0, netBookValue: asset.acquisitionCost, isFullyDepreciated: false };
  }

  const depreciableBase = Math.max(asset.acquisitionCost - asset.residualValue, 0);
  const elapsedMonths = monthDifference(new Date(asset.capitalisationDate), asOf);
  const cappedMonths = Math.min(elapsedMonths, asset.usefulLifeMonths);

  if (asset.depreciationMethod === "SLM") {
    const monthlyCharge = roundCurrency(depreciableBase / asset.usefulLifeMonths);
    const accumulatedDepreciation = roundCurrency(Math.min(depreciableBase, monthlyCharge * cappedMonths));
    return {
      monthlyCharge,
      accumulatedDepreciation,
      netBookValue: roundCurrency(asset.acquisitionCost - accumulatedDepreciation),
      isFullyDepreciated: accumulatedDepreciation >= depreciableBase
    };
  }

  const annualRate = 1 - Math.pow(Math.max(asset.residualValue, 1) / asset.acquisitionCost, 1 / (asset.usefulLifeMonths / 12));
  const monthlyRate = 1 - Math.pow(1 - annualRate, 1 / 12);
  let carryingValue = asset.acquisitionCost;
  let accumulatedDepreciation = 0;
  let monthlyCharge = 0;

  for (let month = 0; month < cappedMonths; month += 1) {
    monthlyCharge = roundCurrency((carryingValue - asset.residualValue) * monthlyRate);
    monthlyCharge = Math.max(0, Math.min(monthlyCharge, carryingValue - asset.residualValue));
    accumulatedDepreciation = roundCurrency(accumulatedDepreciation + monthlyCharge);
    carryingValue = roundCurrency(asset.acquisitionCost - accumulatedDepreciation);
    if (carryingValue <= asset.residualValue) {
      carryingValue = asset.residualValue;
      break;
    }
  }

  return { monthlyCharge, accumulatedDepreciation, netBookValue: carryingValue, isFullyDepreciated: carryingValue <= asset.residualValue };
}

function buildSchedule(asset, months = 12) {
  if (asset.depreciationMethod === "NONE") {
    return [{ period: "N/A", openingNBV: asset.acquisitionCost, depreciationCharge: 0, closingNBV: asset.acquisitionCost, method: "No depreciation" }];
  }

  const results = [];
  const start = new Date(asset.capitalisationDate);
  const totalPeriods = Math.min(asset.usefulLifeMonths, months);
  let openingNBV = asset.acquisitionCost;
  const monthlyCharge = depreciationSnapshot(asset).monthlyCharge;

  for (let offset = 0; offset < totalPeriods; offset += 1) {
    const periodDate = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth() + offset, 1));
    const charge = asset.depreciationMethod === "SLM"
      ? monthlyCharge
      : depreciationSnapshot(asset, new Date(Date.UTC(periodDate.getUTCFullYear(), periodDate.getUTCMonth() + 1, 0))).monthlyCharge;
    const closingNBV = roundCurrency(Math.max(asset.residualValue, openingNBV - charge));
    results.push({ period: periodDate.toISOString().slice(0, 7), openingNBV: roundCurrency(openingNBV), depreciationCharge: roundCurrency(charge), closingNBV, method: asset.depreciationMethod });
    openingNBV = closingNBV;
    if (openingNBV <= asset.residualValue) break;
  }

  return results;
}

function buildAsset(index) {
  const category = categoryProfiles[index % categoryProfiles.length];
  const branch = branches[index % branches.length];
  const currency = randomUnit(index * 4.2) > 0.55 ? "USD" : "CDF";
  const [minCost, maxCost] = category.cost[currency];
  const acquisitionCost = roundCurrency(minCost + (maxCost - minCost) * randomUnit(index * 1.37));
  const residualValue = roundCurrency(acquisitionCost * category.residualRate);
  const startYear = 2016 + Math.floor(randomUnit(index * 0.91) * 10);
  const startMonth = Math.floor(randomUnit(index * 3.17) * 12);
  const startDay = 1 + Math.floor(randomUnit(index * 2.54) * 27);
  const usefulLifeMonths = category.usefulLifeMonths === 0 ? 0 : Math.max(24, Math.round(category.usefulLifeMonths * (0.88 + randomUnit(index * 5.1) * 0.26)));
  const template = category.templates[index % category.templates.length];
  const seededStatus = randomUnit(index * 6.77);
  let status = "ACTIVE";
  if (seededStatus > 0.96) status = "DISPOSED";
  else if (seededStatus > 0.93) status = "HELD_FOR_SALE";
  else if (seededStatus > 0.9) status = "REVALUED";
  else if (seededStatus > 0.87) status = "IMPAIRED";
  else if (seededStatus > 0.83) status = "TRANSFERRED";
  else if (seededStatus > 0.79) status = "PENDING";

  const asset = {
    id: `asset-${String(index).padStart(5, "0")}`,
    assetId: `FAMS-2026-${String(index).padStart(5, "0")}`,
    tagCode: `TAG-${String(index).padStart(5, "0")}`,
    name: template,
    description: `${template} deployed at ${branch.name} for regulated banking operations.`,
    category: category.label,
    categoryKey: category.key,
    branchCode: branch.code,
    branchName: branch.name,
    city: branch.city,
    province: branch.province,
    currency,
    acquisitionCost,
    residualValue,
    capitalisationDate: isoDate(new Date(Date.UTC(startYear, startMonth, startDay))),
    usefulLifeMonths,
    depreciationMethod: category.key === "EQUIPMENT" && randomUnit(index * 9.11) > 0.6 ? "WDV" : category.method,
    status,
    glAssetAccount: `15-${100 + (index % 40)}-${category.key.slice(0, 3)}`,
    glDepreciationExpenseAccount: `61-${200 + (index % 20)}-DEP`,
    glAccumulatedDepreciationAccount: `18-${300 + (index % 20)}-ACC`,
    purchaseOrderRef: `PO-202${index % 6}-${String(1000 + index).slice(-4)}`,
    warrantyExpiryDate: isoDate(new Date(Date.UTC(startYear + 3, startMonth, startDay))),
    lastVerifiedAt: isoDate(new Date(Date.UTC(2026, 2, 1 + (index % 27)))),
    createdBy: demoUsers[index % demoUsers.length].name
  };

  const snapshot = depreciationSnapshot(asset);
  return { ...asset, ...snapshot };
}

function buildAssets(count = 5600) {
  const assets = Array.from({ length: count }, (_, index) => buildAsset(index + 1));
  const overrides = [
    { name: "Toyota Land Cruiser Prado - Executive Transport", branchCode: "BR-GOM", branchName: "Gombe Branch", city: "Kinshasa", province: "Kinshasa", category: "Motor Vehicles", categoryKey: "MOTOR_VEHICLES", currency: "USD", acquisitionCost: 78250, residualValue: 15650, capitalisationDate: "2024-03-18", usefulLifeMonths: 60, depreciationMethod: "WDV", status: "ACTIVE" },
    { name: "Dell OptiPlex 7090 Desktop", branchCode: "BR-LUB", branchName: "Lubumbashi Branch", city: "Lubumbashi", province: "Haut-Katanga", category: "Computer Equipment & IT", categoryKey: "COMPUTER_EQUIPMENT", currency: "USD", acquisitionCost: 1240, residualValue: 62, capitalisationDate: "2025-06-10", usefulLifeMonths: 48, depreciationMethod: "SLM", status: "ACTIVE" },
    { name: "Finacle Core Banking License", branchCode: "HQ-KIN", branchName: "Head Office (Kinshasa HQ)", city: "Kinshasa", province: "Kinshasa", category: "Intangible Assets", categoryKey: "INTANGIBLE_ASSETS", currency: "USD", acquisitionCost: 420000, residualValue: 0, capitalisationDate: "2024-01-01", usefulLifeMonths: 60, depreciationMethod: "SLM", status: "ACTIVE" },
    { name: "Bukavu Branch Cash Counter Retrofit", branchCode: "BR-BKV", branchName: "Bukavu Branch", city: "Bukavu", province: "Sud-Kivu", category: "Leasehold Improvements", categoryKey: "LEASEHOLD_IMPROVEMENTS", currency: "CDF", acquisitionCost: 187500000, residualValue: 3750000, capitalisationDate: "2025-01-14", usefulLifeMonths: 84, depreciationMethod: "SLM", status: "PENDING" }
  ];

  overrides.forEach((override, index) => Object.assign(assets[index], override, depreciationSnapshot({ ...assets[index], ...override })));
  return assets;
}

function buildBranchSummary(assets) {
  return branches
    .map((branch) => {
      const branchAssets = assets.filter((asset) => asset.branchCode === branch.code);
      return {
        branchCode: branch.code,
        branchName: branch.name,
        city: branch.city,
        assetCount: branchAssets.length,
        activeCount: branchAssets.filter((asset) => asset.status === "ACTIVE").length,
        verificationCoverage: 82 + Math.round(randomUnit(branchAssets.length + 8) * 17),
        pendingActions: branchAssets.filter((asset) => ["PENDING", "HELD_FOR_SALE", "TRANSFERRED"].includes(asset.status)).length
      };
    })
    .sort((left, right) => right.assetCount - left.assetCount);
}

function buildDepreciationRuns(assets) {
  const runs = [];
  for (let offset = 11; offset >= 0; offset -= 1) {
    const periodDate = new Date(Date.UTC(2025, 3 + (11 - offset), 1));
    const period = periodDate.toISOString().slice(0, 7);
    const factor = 0.94 + offset * 0.004;
    const totalUSD = roundCurrency(assets.filter((asset) => asset.currency === "USD" && asset.status !== "DISPOSED").reduce((sum, asset) => sum + asset.monthlyCharge * factor, 0) * 0.14);
    const totalCDF = roundCurrency(assets.filter((asset) => asset.currency === "CDF" && asset.status !== "DISPOSED").reduce((sum, asset) => sum + asset.monthlyCharge * factor, 0) * 0.16);
    runs.push({
      id: `dep-${period}`,
      period,
      status: offset === 0 ? "PENDING_APPROVAL" : "POSTED",
      totalAssetsProcessed: 4823 + ((offset + 3) % 5) * 27,
      totalAssetsSkipped: 72 + ((offset + 1) % 4) * 6,
      totalDepreciationUSD: totalUSD,
      totalDepreciationCDF: totalCDF,
      exchangeRateUsed: 2847 + offset * 9,
      failureCount: offset === 0 ? 2 : offset === 2 ? 1 : 0,
      runBy: "Jean-Pierre Mbala",
      approvedBy: offset === 0 ? null : "Jean-Pierre Mbala",
      approvedAt: offset === 0 ? null : `2026-${String(Math.max(1, 3 - offset)).padStart(2, "0")}-28T09:45:00Z`,
      postedAt: offset === 0 ? null : `2026-${String(Math.max(1, 3 - offset)).padStart(2, "0")}-28T10:12:00Z`,
      summary: offset === 0 ? "Ready for checker approval before Finacle posting." : "Posted to Finacle with audit trail captured.",
      glBatchReference: offset === 0 ? null : `BATCH-${period.replace("-", "")}-${4800 + offset}`
    });
  }
  return runs;
}

function buildLifecycleCards(assets) {
  const take = (status, offset) => assets.filter((asset) => asset.status === status)[offset] || assets[offset];
  return [
    { id: "wf-transfer-1", column: "pendingTransfers", title: "Transfer Awaiting Source Approval", actionLabel: "Approve Source", nextColumn: "inTransit", asset: take("TRANSFERRED", 1), owner: "Marie Lukusa", note: "Move to Kisangani cash operations team" },
    { id: "wf-transfer-2", column: "pendingTransfers", title: "Inter-branch Cash Van Movement", actionLabel: "Approve Source", nextColumn: "inTransit", asset: take("TRANSFERRED", 2), owner: "Patrick Kayembe", note: "Destination branch acknowledged readiness" },
    { id: "wf-transfer-3", column: "inTransit", title: "Awaiting Destination Confirmation", actionLabel: "Confirm Receipt", nextColumn: "completed", asset: take("TRANSFERRED", 3), owner: "Jean-Pierre Mbala", note: "Physical handover completed; update branch ledger" },
    { id: "wf-disposal-1", column: "pendingDisposals", title: "Disposal Request Pending Finance", actionLabel: "Approve Disposal", nextColumn: "completed", asset: take("HELD_FOR_SALE", 1), owner: "Jean-Pierre Mbala", note: "NBV below materiality threshold; gain/loss auto-calculated" },
    { id: "wf-disposal-2", column: "pendingDisposals", title: "Write-off Review", actionLabel: "Approve Disposal", nextColumn: "completed", asset: take("DISPOSED", 1), owner: "Immaculee Nzinga", note: "Supporting audit evidence attached" },
    { id: "wf-impairment-1", column: "pendingImpairments", title: "Impairment Indicator Raised", actionLabel: "Record Impairment", nextColumn: "completed", asset: take("IMPAIRED", 1), owner: "Patrick Kayembe", note: "Damage reported during branch verification" }
  ];
}

function buildAuditLogs(assets) {
  const actions = ["LOGIN", "CREATE_ASSET", "TRANSFER_REQUEST", "TRANSFER_APPROVE", "DEPRECIATION_RUN", "DEPRECIATION_APPROVE", "GL_RECONCILE", "REPORT_EXPORT", "PHYSICAL_VERIFY", "DISPOSAL_REQUEST"];
  return Array.from({ length: 60 }, (_, index) => {
    const asset = assets[(index * 17) % assets.length];
    const user = demoUsers[index % demoUsers.length];
    const happenedAt = new Date(Date.UTC(2026, 2, 31 - Math.floor(index / 2), 8 + (index % 9), (index * 7) % 60));
    const action = actions[index % actions.length];
    return { id: `audit-${index + 1}`, timestamp: happenedAt.toISOString(), user: user.name, role: user.role, action, entity: asset.assetId, branchName: asset.branchName, detail: `${action.replaceAll("_", " ")} on ${asset.name}` };
  });
}

function formatAmount(amount, currency) {
  const digits = currency === "CDF" ? 0 : 0;
  return new Intl.NumberFormat(currency === "CDF" ? "fr-CD" : "en-US", { maximumFractionDigits: digits }).format(amount);
}

function buildReconciliation(assets) {
  const famsUsd = assets.filter((asset) => asset.currency === "USD").reduce((sum, asset) => sum + asset.netBookValue, 0);
  const famsCdf = assets.filter((asset) => asset.currency === "CDF").reduce((sum, asset) => sum + asset.netBookValue, 0);
  return {
    period: "2026-03",
    status: "MATCHED",
    famsBalanceUSD: roundCurrency(famsUsd),
    famsBalanceCDF: roundCurrency(famsCdf),
    glBalanceUSD: roundCurrency(famsUsd - 1240),
    glBalanceCDF: roundCurrency(famsCdf - 4200000),
    varianceUSD: 1240,
    varianceCDF: 4200000,
    discrepancyCount: 2,
    lastRunAt: "2026-03-31T04:00:00Z",
    accounts: [
      { glCode: "15-101-BUI", label: "Buildings and Premises", varianceUSD: 0, varianceCDF: 0, status: "MATCHED" },
      { glCode: "15-205-ITE", label: "Computer Equipment", varianceUSD: 1240, varianceCDF: 0, status: "EXCEPTION" },
      { glCode: "15-305-VEH", label: "Motor Vehicles", varianceUSD: 0, varianceCDF: 4200000, status: "EXCEPTION" },
      { glCode: "18-101-ACC", label: "Accumulated Depreciation", varianceUSD: 0, varianceCDF: 0, status: "MATCHED" }
    ]
  };
}

function createReportPreview(id, assets) {
  if (id === "ias16") {
    return {
      columns: ["Category", "Opening NBV", "Additions", "Depreciation", "Closing NBV"],
      rows: categoryProfiles.map((category) => {
        const value = assets.filter((asset) => asset.categoryKey === category.key).reduce((sum, asset) => sum + asset.netBookValue, 0);
        return [category.label, `$${formatAmount(value * 1.08, "USD")}`, `$${formatAmount(value * 0.09, "USD")}`, `$${formatAmount(value * 0.11, "USD")}`, `$${formatAmount(value, "USD")}`];
      })
    };
  }
  if (id === "branch" || id === "ohada") {
    return {
      columns: ["Branch", "Assets", "Pending Actions", "Verification"],
      rows: buildBranchSummary(assets).slice(0, 10).map((branch) => [branch.branchName, String(branch.assetCount), String(branch.pendingActions), `${branch.verificationCoverage}%`])
    };
  }
  return {
    columns: ["Asset ID", "Asset", "Branch", "Status", "NBV"],
    rows: assets.filter((asset) => ["HELD_FOR_SALE", "DISPOSED", "ACTIVE"].includes(asset.status)).slice(0, 8).map((asset) => [asset.assetId, asset.name, asset.branchName, statusPalette[asset.status].label, `${asset.currency} ${formatAmount(asset.netBookValue, asset.currency)}`])
  };
}

function buildBaseData() {
  const assets = buildAssets();
  const depreciationRuns = buildDepreciationRuns(assets);
  const branchSummary = buildBranchSummary(assets);
  const lifecycleCards = buildLifecycleCards(assets);
  const auditLogs = buildAuditLogs(assets);
  const recentActivity = auditLogs.slice(0, 12).map((log) => ({ id: log.id, title: log.action.replaceAll("_", " "), detail: log.detail, user: log.user, timestamp: log.timestamp }));
  const statusBreakdown = Object.keys(statusPalette).map((status) => ({ status, label: statusPalette[status].label, count: assets.filter((asset) => asset.status === status).length }));
  return {
    generatedAt: new Date().toISOString(),
    branches,
    categoryProfiles,
    demoUsers,
    reportCatalog,
    assets,
    depreciationRuns,
    branchSummary,
    lifecycleCards,
    auditLogs,
    recentActivity,
    statusBreakdown,
    reconciliation: buildReconciliation(assets)
  };
}

module.exports = {
  AS_OF_DATE,
  randomUnit,
  roundCurrency,
  monthDifference,
  depreciationSnapshot,
  buildSchedule,
  buildAssets,
  buildBranchSummary,
  buildDepreciationRuns,
  buildLifecycleCards,
  buildAuditLogs,
  buildReconciliation,
  createReportPreview,
  buildBaseData
};
