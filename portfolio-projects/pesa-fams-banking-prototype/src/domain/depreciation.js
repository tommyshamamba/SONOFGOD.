function roundCurrency(amount) {
  return Math.round(amount * 100) / 100;
}

function monthDifference(start, end) {
  const years = end.getUTCFullYear() - start.getUTCFullYear();
  const months = end.getUTCMonth() - start.getUTCMonth();
  let total = years * 12 + months;
  if (end.getUTCDate() < start.getUTCDate()) {
    total -= 1;
  }
  return Math.max(total, 0);
}

function calculateDepreciationLine(asset, asOfDate) {
  if (asset.depreciation_method === "NONE") {
    return {
      openingNBV: Number(asset.net_book_value),
      depreciationCharge: 0,
      closingNBV: Number(asset.net_book_value),
      skipped: true,
      skipReason: "Non-depreciable asset"
    };
  }

  if (["PENDING", "HELD_FOR_SALE", "DISPOSED"].includes(asset.status)) {
    return {
      openingNBV: Number(asset.net_book_value),
      depreciationCharge: 0,
      closingNBV: Number(asset.net_book_value),
      skipped: true,
      skipReason: `Status ${asset.status} is excluded from depreciation`
    };
  }

  const acquisitionCost = Number(asset.acquisition_cost);
  const residualValue = Number(asset.residual_value);
  const openingNBV = Number(asset.net_book_value);
  const depreciableBase = Math.max(acquisitionCost - residualValue, 0);

  if (openingNBV <= residualValue || depreciableBase === 0) {
    return {
      openingNBV,
      depreciationCharge: 0,
      closingNBV: openingNBV,
      skipped: true,
      skipReason: "Asset already at residual value"
    };
  }

  const start = new Date(asset.capitalisation_date);
  const asOf = new Date(asOfDate);
  const monthsElapsed = monthDifference(start, asOf);
  const usefulLifeMonths = Number(asset.useful_life_months);

  let depreciationCharge = 0;

  if (asset.depreciation_method === "SLM") {
    depreciationCharge = depreciableBase / usefulLifeMonths;
  } else if (asset.depreciation_method === "WDV") {
    const annualRate = 1 - Math.pow(Math.max(residualValue, 1) / acquisitionCost, 1 / (usefulLifeMonths / 12));
    const monthlyRate = 1 - Math.pow(1 - annualRate, 1 / 12);
    depreciationCharge = (openingNBV - residualValue) * monthlyRate;
  }

  const isFirstMonth = monthsElapsed === 0;
  if (isFirstMonth) {
    const daysInMonth = new Date(Date.UTC(asOf.getUTCFullYear(), asOf.getUTCMonth() + 1, 0)).getUTCDate();
    const prorataDays = daysInMonth - start.getUTCDate() + 1;
    depreciationCharge *= prorataDays / daysInMonth;
  }

  depreciationCharge = roundCurrency(Math.max(0, Math.min(depreciationCharge, openingNBV - residualValue)));
  const closingNBV = roundCurrency(Math.max(residualValue, openingNBV - depreciationCharge));

  return {
    openingNBV: roundCurrency(openingNBV),
    depreciationCharge,
    closingNBV,
    skipped: depreciationCharge === 0,
    skipReason: depreciationCharge === 0 ? "No depreciation charge generated" : null
  };
}

module.exports = {
  calculateDepreciationLine,
  monthDifference,
  roundCurrency
};
