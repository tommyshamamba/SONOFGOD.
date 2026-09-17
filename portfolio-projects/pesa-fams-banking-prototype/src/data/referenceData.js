const branches = [
  { code: "HQ-KIN", name: "Head Office (Kinshasa HQ)", city: "Kinshasa", province: "Kinshasa", isHeadOffice: true },
  { code: "BR-GOM", name: "Gombe Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-LIM", name: "Limete Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-NGA", name: "Ngaliema Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-MAT", name: "Matete Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-KES", name: "Kinshasa Est Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-KNO", name: "Kinshasa Nord Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-BAR", name: "Barumbu Branch", city: "Kinshasa", province: "Kinshasa" },
  { code: "BR-LUB", name: "Lubumbashi Branch", city: "Lubumbashi", province: "Haut-Katanga" },
  { code: "BR-MBJ", name: "Mbuji-Mayi Branch", city: "Mbuji-Mayi", province: "Kasai-Oriental" },
  { code: "BR-KIS", name: "Kisangani Branch", city: "Kisangani", province: "Tshopo" },
  { code: "BR-BKV", name: "Bukavu Branch", city: "Bukavu", province: "Sud-Kivu" },
  { code: "BR-GOA", name: "Goma Branch", city: "Goma", province: "Nord-Kivu" },
  { code: "BR-MTD", name: "Matadi Branch", city: "Matadi", province: "Kongo Central" },
  { code: "BR-KNG", name: "Kananga Branch", city: "Kananga", province: "Kasai Central" },
  { code: "BR-KOL", name: "Kolwezi Branch", city: "Kolwezi", province: "Lualaba" },
  { code: "BR-LIK", name: "Likasi Branch", city: "Likasi", province: "Haut-Katanga" },
  { code: "BR-BTB", name: "Butembo Branch", city: "Butembo", province: "Nord-Kivu" },
  { code: "BR-UVI", name: "Uvira Branch", city: "Uvira", province: "Sud-Kivu" },
  { code: "BR-KAL", name: "Kalemie Branch", city: "Kalemie", province: "Tanganyika" },
  { code: "BR-KAM", name: "Kamina Branch", city: "Kamina", province: "Haut-Lomami" },
  { code: "BR-TSH", name: "Tshikapa Branch", city: "Tshikapa", province: "Kasai" },
  { code: "BR-BAN", name: "Bandundu Branch", city: "Bandundu", province: "Kwilu" },
  { code: "BR-MBK", name: "Mbandaka Branch", city: "Mbandaka", province: "Equateur" },
  { code: "BR-KKW", name: "Kikwit Branch", city: "Kikwit", province: "Kwilu" },
  { code: "BR-BOM", name: "Boma Branch", city: "Boma", province: "Kongo Central" },
  { code: "BR-KND", name: "Kindu Branch", city: "Kindu", province: "Maniema" },
  { code: "BR-BUN", name: "Bunia Branch", city: "Bunia", province: "Ituri" },
  { code: "BR-ISI", name: "Isiro Branch", city: "Isiro", province: "Haut-Uele" },
  { code: "BR-GEM", name: "Gemena Branch", city: "Gemena", province: "Sud-Ubangi" },
  { code: "BR-GBD", name: "Gbadolite Branch", city: "Gbadolite", province: "Nord-Ubangi" },
  { code: "BR-BOE", name: "Boende Branch", city: "Boende", province: "Tshuapa" },
  { code: "BR-LOD", name: "Lodja Branch", city: "Lodja", province: "Sankuru" },
  { code: "BR-MWD", name: "Mwene-Ditu Branch", city: "Mwene-Ditu", province: "Lomami" },
  { code: "BR-KEN", name: "Kenge Branch", city: "Kenge", province: "Kwango" },
  { code: "BR-INO", name: "Inongo Branch", city: "Inongo", province: "Mai-Ndombe" },
  { code: "BR-LIS", name: "Lisala Branch", city: "Lisala", province: "Mongala" }
];

const categoryProfiles = [
  { key: "LAND", label: "Land", method: "NONE", usefulLifeMonths: 0, residualRate: 1, cost: { USD: [80000, 450000], CDF: [280000000, 1450000000] }, templates: ["Branch Land Parcel", "Future Office Plot", "Perimeter Land Bank"] },
  { key: "BUILDINGS_FREEHOLD", label: "Buildings (Freehold)", method: "SLM", usefulLifeMonths: 360, residualRate: 0.1, cost: { USD: [250000, 2200000], CDF: [850000000, 6200000000] }, templates: ["Regional Office Building", "Cash Center Premises", "Training Centre Building"] },
  { key: "LEASEHOLD_IMPROVEMENTS", label: "Leasehold Improvements", method: "SLM", usefulLifeMonths: 96, residualRate: 0.02, cost: { USD: [12000, 220000], CDF: [42000000, 780000000] }, templates: ["Branch Interior Fit-Out", "Security Glass Upgrade", "ATM Lobby Renovation"] },
  { key: "MOTOR_VEHICLES", label: "Motor Vehicles", method: "WDV", usefulLifeMonths: 60, residualRate: 0.2, cost: { USD: [18000, 95000], CDF: [68000000, 345000000] }, templates: ["Toyota Land Cruiser Prado", "Toyota Hilux Cash Van", "Mitsubishi Pajero Branch SUV"] },
  { key: "COMPUTER_EQUIPMENT", label: "Computer Equipment & IT", method: "SLM", usefulLifeMonths: 48, residualRate: 0.05, cost: { USD: [750, 18500], CDF: [2500000, 62000000] }, templates: ["Dell OptiPlex Workstation", "Cisco Branch Router", "NCR Lobby ATM", "HP LaserJet Finance Printer", "Lenovo Treasury Laptop"] },
  { key: "FURNITURE_FITTINGS", label: "Furniture & Fittings", method: "SLM", usefulLifeMonths: 84, residualRate: 0.03, cost: { USD: [240, 9600], CDF: [850000, 33500000] }, templates: ["Customer Service Desk", "Boardroom Conference Table", "Branch Teller Counter", "Executive Office Cabinet"] },
  { key: "EQUIPMENT", label: "Equipment", method: "SLM", usefulLifeMonths: 72, residualRate: 0.08, cost: { USD: [3200, 88000], CDF: [11200000, 312000000] }, templates: ["Diesel Backup Generator", "Cash Counting Machine", "Vault Security Control Panel", "Network UPS Array"] },
  { key: "INTANGIBLE_ASSETS", label: "Intangible Assets", method: "SLM", usefulLifeMonths: 60, residualRate: 0, cost: { USD: [12000, 420000], CDF: [42000000, 1450000000] }, templates: ["Finacle Core Banking License", "Microsoft Enterprise Agreement", "Fortinet Security Subscription"] }
];

const statusPalette = {
  ACTIVE: { label: "Active", tone: "success" },
  PENDING: { label: "Pending", tone: "warning" },
  TRANSFERRED: { label: "Transferred", tone: "info" },
  IMPAIRED: { label: "Impaired", tone: "danger" },
  REVALUED: { label: "Revalued", tone: "violet" },
  HELD_FOR_SALE: { label: "Held for Sale", tone: "amber" },
  DISPOSED: { label: "Disposed", tone: "muted" }
};

const demoUsers = [
  { id: "usr-finance", email: "finance@bankdrc.cd", password: "Finance123!", name: "Jean-Pierre Mbala", role: "finance_admin", branchCode: "HQ-KIN", title: "Finance Administrator" },
  { id: "usr-ops", email: "operations@bankdrc.cd", password: "Ops123!", name: "Marie Lukusa", role: "operations", branchCode: "BR-GOM", title: "Operations User" },
  { id: "usr-admin", email: "admin@bankdrc.cd", password: "Admin123!", name: "Patrick Kayembe", role: "admin_user", branchCode: "BR-LUB", title: "Administration User" },
  { id: "usr-audit", email: "auditor@bankdrc.cd", password: "Audit123!", name: "Immaculee Nzinga", role: "auditor", branchCode: "HQ-KIN", title: "Internal Auditor" },
  { id: "usr-it", email: "it@bankdrc.cd", password: "ITAdmin123!", name: "Eric Tshimanga", role: "it_admin", branchCode: "HQ-KIN", title: "IT Administrator" }
];

const reportCatalog = [
  { id: "ias16", title: "IAS 16 Fixed Asset Schedule", description: "Primary external audit schedule with opening balances, additions, depreciation, and closing NBV.", owner: "Finance Control" },
  { id: "ohada", title: "OHADA Depreciation Schedule", description: "Local statutory depreciation schedule aligned to SYSCOHADA requirements.", owner: "Finance Control" },
  { id: "disposal", title: "Disposal Register", description: "Full register of disposals, write-offs, and derecognition activity.", owner: "Finance Control" },
  { id: "gl-reconciliation", title: "GL Reconciliation Report", description: "Reconciles FAMS balances against Finacle GL balances with exceptions.", owner: "Finance & Audit" },
  { id: "branch", title: "Assets by Branch", description: "Operational visibility into branch asset concentrations and verification coverage.", owner: "Operations" },
  { id: "fully-depreciated", title: "Fully Depreciated Review Pack", description: "Flags fully depreciated assets ready for disposal review or continued use assessment.", owner: "Finance & Administration" }
];

module.exports = {
  branches,
  categoryProfiles,
  statusPalette,
  demoUsers,
  reportCatalog
};
