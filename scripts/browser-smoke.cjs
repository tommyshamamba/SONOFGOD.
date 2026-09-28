// Start the local services documented in docs/LOCAL_DEMOS.md before running.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');

(async () => {
  const browser = process.env.BROWSER_CDP_URL ? await chromium.connectOverCDP(process.env.BROWSER_CDP_URL) : await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const screenshots = process.env.DEMO_SCREENSHOT_DIR || path.resolve('playwright-report');
  fs.mkdirSync(screenshots, { recursive: true });
  try {
    await page.goto(process.env.TRACE_URL || 'http://localhost:3000');
    const fixture = fs.readFileSync(path.join(__dirname, '../trace-stores/apps/storefront/tests/creator.test.mjs'), 'utf8').match(/const png = Buffer.from\("([^"]+)"/)[1];
    await page.getByLabel('Upload artwork', { exact: true }).setInputFiles({ name: 'demo.png', mimeType: 'image/png', buffer: Buffer.from(fixture, 'base64') });
    await page.getByRole('link', { name: 'Download PNG' }).waitFor({ timeout: 30000 });
    assert.match(await page.locator('.processor').innerText(), /Processed with/);
    const downloading = page.waitForEvent('download');
    await page.getByRole('link', { name: 'Download PNG' }).click();
    const download = await downloading;
    assert.match(download.suggestedFilename(), /\.png$/);
    await page.getByRole('button', { name: 'Add to cart' }).first().click();
    await page.getByRole('button', { name: /Cart 1/ }).click();
    await page.getByRole('button', { name: 'Increase Die Cut Stickers quantity' }).click();
    assert.match(await page.locator('.cart-total').innerText(), /38/);
    await page.getByRole('button', { name: 'Close cart' }).click();
    await page.reload();
    await page.getByRole('button', { name: /Cart 2/ }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'trace.png'), fullPage: true });
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
    await page.getByRole('button', { name: /Start Interview/i }).click();
    await page.getByRole('button', { name: /Start Question 1/i }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'interview.png'), fullPage: true });
    console.log('PASS Interview: registration and mock session creation through browser');

    await page.goto(process.env.KUBERNETES_URL || 'http://127.0.0.1:3200');
    await page.getByRole('heading', { name: 'Kubernetes Demo Application' }).waitFor();
    await page.getByText('"podName": "local-process"', { exact: false }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'kubernetes.png'), fullPage: true });
    console.log('PASS Kubernetes: browser loads backend data');

    await page.goto(process.env.VOICE_URL || 'http://127.0.0.1:8090');
    await page.getByRole('button', { name: 'Simulate call event' }).click();
    await page.getByText('Event saved locally.', { exact: true }).waitFor();
    await page.getByRole('button', { name: 'Replay same event' }).click();
    await page.getByText('Duplicate recognized. No additional draft created.', { exact: true }).waitFor();
    await page.screenshot({ path: path.join(screenshots, 'voice.png'), fullPage: true });
    console.log('PASS Voice simulation: draft creation and duplicate handling');
    assert.deepEqual(errors, [], 'No uncaught browser exceptions');
  } finally { await context.close(); await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
