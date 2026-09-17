const test = require("node:test");
const assert = require("node:assert/strict");
const { calculateDepreciationLine } = require("../src/domain/depreciation");

test("straight line depreciation respects residual value", () => {
  const line = calculateDepreciationLine(
    {
      acquisition_cost: 1200,
      residual_value: 0,
      net_book_value: 1200,
      useful_life_months: 12,
      capitalisation_date: "2026-04-01",
      depreciation_method: "SLM",
      status: "ACTIVE"
    },
    new Date("2026-04-30T00:00:00.000Z")
  );

  assert.equal(line.depreciationCharge, 100);
  assert.equal(line.closingNBV, 1100);
});

test("held for sale assets are skipped", () => {
  const line = calculateDepreciationLine(
    {
      acquisition_cost: 5000,
      residual_value: 500,
      net_book_value: 3000,
      useful_life_months: 60,
      capitalisation_date: "2025-01-15",
      depreciation_method: "WDV",
      status: "HELD_FOR_SALE"
    },
    new Date("2026-04-30T00:00:00.000Z")
  );

  assert.equal(line.depreciationCharge, 0);
  assert.equal(line.skipped, true);
});
