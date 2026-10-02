// Start the local services documented in docs/LOCAL_DEMOS.md before running.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');

(async () => {
  const browser = process.env.BROWSER_CDP_URL ? await chromium.connectOverCDP(process.env.BROWSER_CDP_URL) : await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  context.setDefaultTimeout(15000);
  const page = await context.newPage();
  const errors = [];
  function monitor(p) { p.on('pageerror', error => errors.push(error.message)); }
  monitor(page);
  context.on('page', monitor);
  const screenshots = process.env.DEMO_SCREENSHOT_DIR || path.resolve('playwright-report');
  fs.mkdirSync(screenshots, { recursive: true });
  try {
    await page.goto(process.env.TRACE_URL || 'http://localhost:3000');
    const fixture = fs.readFileSync(path.join(__dirname, '../trace-stores/apps/storefront/tests/creator.test.mjs'), 'utf8').match(/const png = Buffer.from\("([^"]+)"/)[1];
    await page.getByLabel('Upload artwork', { exact: true }).setInputFiles({ name: 'demo.png', mimeType: 'image/png', buffer: Buffer.from(fixture, 'base64') });
    await page.getByRole('link', { name: 'Download PNG' }).waitFor({ timeout: 30000 });
    assert.match(await page.locator('.processor').innerText(), /Processed with/);
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('link', { name: 'Download PNG' }).click()
    ]);
    assert.match(download.suggestedFilename(), /\.png$/);
    await page.getByRole('button', { name: 'Add to cart' }).first().click();
    await page.getByRole('button', { name: /Cart 1/ }).click();
    await page.getByRole('button', { name: 'Increase Die Cut Stickers quantity' }).click();
    assert.match(await page.locator('.cart-total').innerText(), /38/);
    await page.getByRole('button', { name: 'Close cart' }).click();
    await page.reload();
    await page.getByRole('button', { name: /Cart 2/ }).waitFor();
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: path.join(screenshots, 'trace.png') });
    console.log('PASS Trace: real upload, PNG download, cart updates and persistence');

    await page.goto((process.env.INTERVIEW_URL || 'http://localhost:5000') + '/register');
    await page.getByPlaceholder('Fiston Matandi').fill('Synthetic browser tester');
    await page.getByPlaceholder('you@email.com').fill(`browser-${randomUUID()}@example.test`);
    await page.getByPlaceholder('Min. 12 characters').fill('synthetic browser password 123');
    await page.getByPlaceholder('••••••••', { exact: true }).fill('synthetic browser password 123');
    await page.getByRole('button', { name: 'Create Account' }).click();
    await page.waitForURL(url => url.pathname === '/');
    await page.goto((process.env.INTERVIEW_URL || 'http://localhost:5000') + '/mock');
    await page.locator('input').first().fill('Software engineer');
    const [sessionResponse] = await Promise.all([
      page.waitForResponse(r => r.url().endsWith('/api/sessions/start') && r.request().method() === 'POST'),
      page.getByRole('button', { name: /Start Interview/i }).click()
    ]);
    assert.ok(sessionResponse.ok(), 'Interview session starts successfully');
    const sessionData = await sessionResponse.json();
    assert.ok(sessionData.questions?.length > 0);
    await page.getByRole('button', { name: /Start Question 1/i }).click();
    for (let i = 0; i < sessionData.questions.length; i++) {
      await page.locator('textarea').fill('In a synthetic project I investigated a failed deployment, reproduced the defect with a test, fixed the configuration and documented the outcome with my team.');
      await page.getByRole('button', { name: /Submit Answer/ }).click();
      await page.getByRole('button', { name: i + 1 === sessionData.questions.length ? /Finish.*Coaching Plan/ : /Next Question/ }).click();
    }
    await page.getByRole('heading', { name: 'Coaching Report', exact: true }).waitFor();
    await page.reload();
    await page.getByRole('heading', { name: 'Coaching Report', exact: true }).waitFor();
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: path.join(screenshots, 'interview.png') });
    console.log('PASS Interview: registration, all answers, scoring, completion and persisted coaching through browser');

    const maker = await context.newPage();
    await maker.goto(process.env.BANKING_URL || 'http://localhost:3100');
    await maker.locator('#emailInput').fill('it@bankdrc.cd');
    await maker.locator('#passwordInput').fill('ITAdmin123!');
    await maker.getByRole('button', { name: 'Enter Prototype' }).click();
    await maker.locator('[data-section="depreciation"]').click();
    const [prepared] = await Promise.all([
      maker.waitForResponse(r => r.url().endsWith('/api/depreciation/run') && r.request().method() === 'POST'),
      maker.locator('#runDepreciationButton').click()
    ]);
    assert.ok(prepared.ok(), 'Banking maker prepares run');
    assert.equal(await maker.locator('#approveDepreciationButton').isDisabled(), true, 'IT maker cannot approve');
    const checker = await context.newPage();
    await checker.goto(process.env.BANKING_URL || 'http://localhost:3100');
    await checker.getByRole('button', { name: 'Enter Prototype' }).click();
    await checker.locator('[data-section="depreciation"]').click();
    const [posted] = await Promise.all([
      checker.waitForResponse(r => r.url().endsWith('/api/depreciation/approve') && r.request().method() === 'POST'),
      checker.locator('#approveDepreciationButton').click()
    ]);
    assert.ok(posted.ok(), 'Different banking checker approves');
    await checker.locator('#depreciationPanel').getByText(/Posted/i).first().waitFor();
    await checker.evaluate(() => window.scrollTo(0, 0));
    await checker.screenshot({ path: path.join(screenshots, 'banking.png') });
    await checker.locator('[data-section="reports"]').click();
    const [report] = await Promise.all([
      context.waitForEvent('page'),
      checker.locator('#reportGrid').getByRole('button', { name: 'Print Pack', exact: true }).first().click()
    ]);
    await report.getByRole('heading', { name: 'IAS 16 Fixed Asset Schedule', exact: true }).waitFor();
    assert.equal(await report.evaluate(() => window.opener), null, 'Report cannot control the parent page');
    await report.close();
    await checker.getByRole('button', { name: 'Switch account', exact: true }).click();
    await checker.locator('#emailInput').fill('auditor@bankdrc.cd');
    await checker.locator('#passwordInput').fill('Audit123!');
    await checker.getByRole('button', { name: 'Enter Prototype' }).click();
    await checker.locator('[data-section="depreciation"]').click();
    assert.equal(await checker.locator('#runDepreciationButton').isDisabled(), true, 'Auditor cannot prepare runs');
    assert.equal(await checker.locator('#approveDepreciationButton').isDisabled(), true, 'Auditor cannot approve runs');
    await maker.close(); await checker.close();
    console.log('PASS Banking: separate maker/checker approval, authenticated print report and auditor restrictions');

    await page.goto(process.env.BLOCKCHAIN_URL || 'http://localhost:3300');
    await page.locator('input[type="email"]').fill(`browser-${randomUUID()}@example.test`);
    await page.getByPlaceholder('At least 12 characters').fill('synthetic browser password 123');
    await page.getByRole('button', { name: 'Register', exact: true }).click();
    await page.getByPlaceholder('Key name (e.g., Production)').fill('Browser demo');
    await page.getByRole('button', { name: 'Create Key', exact: true }).click();
    await page.getByRole('button', { name: 'Hide key', exact: true }).click();
    await page.getByPlaceholder('0x...', { exact: true }).fill('0x1111111111111111111111111111111111111111');
    await page.getByRole('button', { name: 'Get Balance', exact: true }).click();
    await page.getByRole('heading', { name: 'Balance Result', exact: true }).waitFor();
    await page.getByRole('button', { name: 'Revoke', exact: true }).click();
    await page.getByText('REVOKED', { exact: true }).waitFor();
    const [rejected] = await Promise.all([
      page.waitForResponse(r => r.url().includes('/api/v1/ethereum/balance/')),
      page.getByRole('button', { name: 'Get Balance', exact: true }).click()
    ]);
    assert.equal(rejected.status(), 401, 'Revoked API key must fail');
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: path.join(screenshots, 'blockchain.png') });
    console.log('PASS Blockchain: registration, full key, query and revocation through browser');

    await page.goto(process.env.KUBERNETES_URL || 'http://127.0.0.1:3200');
    await page.getByRole('heading', { name: 'Kubernetes Demo Application' }).waitFor();
    await page.getByText('"podName": "local-process"', { exact: false }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'kubernetes.png') });
    console.log('PASS Kubernetes: browser loads backend data');

    await page.goto(process.env.VOICE_URL || 'http://127.0.0.1:8090');
    await page.getByRole('button', { name: 'Simulate call event' }).click();
    await page.getByText('Event saved locally.', { exact: true }).waitFor();
    await page.getByRole('button', { name: 'Replay same event' }).click();
    await page.getByText('Duplicate recognized. No additional draft created.', { exact: true }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'voice.png') });
    console.log('PASS Voice simulation: draft creation and duplicate handling');
    assert.deepEqual(errors, [], 'No uncaught browser exceptions');
  } catch (error) {
    fs.writeFileSync(path.join(screenshots, 'failure.json'), JSON.stringify({
      message: error.message, browserErrors: errors, pages: context.pages().map(p => p.url())
    }, null, 2));
    await Promise.allSettled(context.pages().map((p, index) =>
      p.screenshot({ path: path.join(screenshots, `failure-${index}.png`), fullPage: true })
    ));
    throw error;
  } finally { await context.close(); await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
