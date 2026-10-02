const { test, before } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Interface, AbiCoder, ContractFactory, ZeroAddress } = require('ethers');
const { createVM, runTx } = require('@ethereumjs/vm');
const { Common, Hardfork, Mainnet } = require('@ethereumjs/common');
const { createLegacyTx } = require('@ethereumjs/tx');
const { createAccount, createAddressFromPrivateKey, createAddressFromString, hexToBytes, bytesToHex } = require('@ethereumjs/util');
const { compile } = require('./compile.cjs');

// Synthetic keys exist only inside a fresh in-memory EVM, never an RPC provider.
const ownerKey = hexToBytes(`0x${'21'.repeat(32)}`);
const strangerKey = hexToBytes(`0x${'42'.repeat(32)}`);
const owner = createAddressFromPrivateKey(ownerKey).toString();
const stranger = createAddressFromPrivateKey(strangerKey).toString();
const AAVE = '0x794a61358D6845594F94dc1DB02A252b5b4814aD';
const BALANCER = '0xBA12222222228d8Ba445958a75a0704d566BF2C8';
const SUSHI = '0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506';
const coder = AbiCoder.defaultAbiCoder();
let artifacts;
before(() => { artifacts = compile(); });

async function fixture() {
  const common = new Common({ chain: Mainnet, hardfork: Hardfork.Shanghai });
  const vm = await createVM({ common });
  for (const key of [ownerKey, strangerKey]) {
    await vm.stateManager.putAccount(createAddressFromPrivateKey(key), createAccount({ balance: 10n ** 30n }));
  }
  const powerArtifact = artifacts['Power2.sol'].Power;
  const powerInterface = new Interface(powerArtifact.abi);
  async function transact(data, to, key = ownerKey) {
    const from = createAddressFromPrivateKey(key);
    const nonce = (await vm.stateManager.getAccount(from)).nonce;
    const tx = createLegacyTx({ nonce, gasLimit: 20_000_000n, gasPrice: 10_000_000_000n, data,
      ...(to ? { to } : {}) }, { common }).sign(key);
    const result = await runTx(vm, { tx });
    if (result.execResult.exceptionError) {
      const encoded = bytesToHex(result.execResult.returnValue);
      let name = result.execResult.exceptionError.error;
      try { name = powerInterface.parseError(encoded)?.name || name; } catch {}
      if (encoded.startsWith('0x08c379a0')) name = coder.decode(['string'], `0x${encoded.slice(10)}`)[0];
      throw new Error(`EVM reverted: ${name}`);
    }
    return result;
  }
  function contract(address, artifact) { return { address, interface: new Interface(artifact.abi) }; }
  async function deploy(artifact, args = []) {
    const data = (await new ContractFactory(artifact.abi, artifact.evm.bytecode.object).getDeployTransaction(...args)).data;
    const result = await transact(data);
    return contract(result.createdAddress.toString(), artifact);
  }
  async function call(c, method, args = [], key) {
    const result = await transact(c.interface.encodeFunctionData(method, args), c.address, key);
    const decoded = c.interface.decodeFunctionResult(method, bytesToHex(result.execResult.returnValue));
    return decoded.length === 1 ? decoded[0] : decoded;
  }
  async function fixed(address, artifact) {
    await vm.stateManager.putAccount(createAddressFromString(address), createAccount({}));
    await vm.stateManager.putCode(createAddressFromString(address), hexToBytes(`0x${artifact.evm.deployedBytecode.object}`));
    return contract(address, artifact);
  }
  const tokenArtifact = artifacts['LenderMocks.sol'].MockToken;
  const lenderArtifact = artifacts['LenderMocks.sol'].MockLender;
  const first = await deploy(tokenArtifact);
  const second = await deploy(tokenArtifact);
  const aave = await fixed(AAVE, lenderArtifact);
  const balancer = await fixed(BALANCER, lenderArtifact);
  const router = await fixed(SUSHI, artifacts['LenderMocks.sol'].MockRouter);
  const uni = await deploy(lenderArtifact);
  const dodo = await deploy(lenderArtifact);
  const implementation = await deploy(powerArtifact);
  const proxy = await deploy(artifacts['ERC1967Proxy.sol'].ERC1967Proxy,
    [implementation.address, powerInterface.encodeFunctionData('initialize', [owner, owner])]);
  const power = contract(proxy.address, powerArtifact);
  for (const lender of [aave, balancer, uni, dodo]) {
    await call(lender, 'configure', [first.address, second.address, 5, 0, 0]);
    for (const token of [first, second]) await call(token, 'mint', [lender.address, 100_000]);
  }
  for (const lender of [uni, dodo]) await call(power, 'setPoolAllowed', [lender.address, true]);
  await call(power, 'setDodoFeeBps', [dodo.address, 50]);
  await call(router, 'configure', [100, ZeroAddress]);
  async function payload(token = first) {
    return call(power, 'buildArbPayload', [[14, [token.address, token.address], 1001, 1, 3000, 3000,
      '0x', [[], [], []], [ZeroAddress, 0, 0]]]);
  }
  async function request(provider, token = first) {
    let data = await payload(token);
    if (provider === 2) data = coder.encode(['address', 'bytes'], [uni.address, data]);
    if (provider === 3) data = coder.encode(['address', 'address', 'bytes'], [dodo.address, first.address, data]);
    return call(power, 'requestFlashLoan', [provider, token.address, 1000, data, 3000]);
  }
  return { vm, call, deploy, first, second, aave, balancer, uni, dodo, router, power, implementation, payload, request };
}

for (const [name, provider, lenderName] of [['Aave', 0, 'aave'], ['Balancer', 1, 'balancer'], ['Uniswap', 2, 'uni']]) {
  test(`${name}: synchronous callback, repayment, profit and subsequent request`, async () => {
    const f = await fixture();
    await f.request(provider);
    assert.equal(await f.call(f.first, 'balanceOf', [f[lenderName].address]), 100005n);
    assert.equal(await f.call(f.first, 'balanceOf', [owner]), 95n);
    assert.equal(await f.call(f.first, 'balanceOf', [f.power.address]), 0n);
    assert.equal(await f.call(f.first, 'allowance', [f.power.address, f[lenderName].address]), 0n);
    await f.request(provider);
    assert.equal(await f.call(f.first, 'balanceOf', [owner]), 190n);
  });
}

test('Uniswap token1 is repaid in the borrowed asset', async () => {
  const f = await fixture();
  await f.request(2, f.second);
  assert.equal(await f.call(f.second, 'balanceOf', [f.uni.address]), 100005n);
  assert.equal(await f.call(f.first, 'balanceOf', [f.uni.address]), 100000n);
});

for (const [kind, name] of ['DVM', 'DPP', 'DSP', 'legacy'].entries()) {
  test(`DODO ${name}: base and quote loans repay the correct token`, async () => {
    const f = await fixture();
    await f.call(f.dodo, 'configure', [f.first.address, f.second.address, 5, 0, kind]);
    await f.request(3, f.first);
    await f.request(3, f.second);
    for (const token of [f.first, f.second]) {
      assert.equal(await f.call(token, 'balanceOf', [f.dodo.address]), 100005n);
      assert.equal(await f.call(token, 'balanceOf', [owner]), 95n);
    }
  });
}

for (const [mode, name] of [[1, 'foreign initiator'], [2, 'changed amount'], [3, 'changed payload'], [4, 'missing callback'], [5, 'replayed callback']]) {
  test(`Aave rejects ${name} and rolls back the entire loan`, async () => {
    const f = await fixture();
    await f.call(f.aave, 'configure', [f.first.address, f.second.address, 5, mode, 0]);
    await assert.rejects(() => f.request(0), /EVM reverted/);
    assert.equal(await f.call(f.first, 'balanceOf', [f.aave.address]), 100000n);
    assert.equal(await f.call(f.first, 'balanceOf', [owner]), 0n);
    await f.call(f.aave, 'configure', [f.first.address, f.second.address, 5, 0, 0]);
    await f.request(0);
  });
}

test('callbacks without an active request fail even from an allowed lender', async () => {
  const f = await fixture();
  const payload = await f.payload();
  const data = f.power.interface.encodeFunctionData('executeOperation', [f.first.address, 1000, 5, f.power.address, payload]);
  await assert.rejects(() => f.call(f.aave, 'invoke', [f.power.address, data]), /UnexpectedCallback/);
  await assert.rejects(() => f.call(f.power, 'executeOperation', [f.first.address, 1000, 5, f.power.address, payload]), /NotAave/);
});

test('nested lender callback during a router call is rejected', async () => {
  const f = await fixture();
  await f.call(f.router, 'configure', [100, f.aave.address]);
  await f.request(0);
  assert.equal(await f.call(f.aave, 'reentriesRejected'), 1n);
});

test('malformed Balancer vectors are rejected before indexing', async () => {
  const f = await fixture();
  await f.call(f.balancer, 'configure', [f.first.address, f.second.address, 5, 6, 0]);
  await assert.rejects(() => f.request(1), /single-only/);
});

test('preexisting funds cannot disguise a losing trade and are preserved on success', async () => {
  const f = await fixture();
  await f.call(f.first, 'mint', [f.power.address, 777]);
  await f.call(f.router, 'configure', [2, ZeroAddress]);
  await assert.rejects(() => f.request(0), /profit/);
  assert.equal(await f.call(f.first, 'balanceOf', [f.power.address]), 777n);
  await f.call(f.router, 'configure', [100, ZeroAddress]);
  await f.request(0);
  assert.equal(await f.call(f.first, 'balanceOf', [f.power.address]), 777n);
  assert.equal(await f.call(f.first, 'balanceOf', [owner]), 95n);
});

test('owner-only withdrawal matches the bot ABI and rejects other users', async () => {
  const f = await fixture();
  await f.call(f.first, 'mint', [f.power.address, 777]);
  await assert.rejects(() => f.call(f.power, 'withdrawProfit', [f.first.address], strangerKey), /OwnableUnauthorizedAccount/);
  await f.call(f.power, 'withdrawProfit', [f.first.address]);
  assert.equal(await f.call(f.first, 'balanceOf', [owner]), 777n);
  assert.equal(await f.call(f.first, 'balanceOf', [f.power.address]), 0n);
});

test('liquidation preserves collateral reserves and counts only the current trade profit', async () => {
  const f = await fixture();
  await f.call(f.second, 'mint', [f.power.address, 777]);
  const data = await f.call(f.power, 'buildLiqPayload', [[f.second.address, f.first.address, stranger,
    1000, [f.second.address, f.first.address], 1, 95, 3000, '0x']]);
  await f.call(f.power, 'requestFlashLoan', [0, f.first.address, 1000, data, 3000]);
  assert.equal(await f.call(f.second, 'balanceOf', [f.power.address]), 777n);
  assert.equal(await f.call(f.second, 'balanceOf', [f.router.address]), 1000n);
  assert.equal(await f.call(f.first, 'balanceOf', [owner]), 95n);
});

test('collateral reserves cannot subsidize a losing liquidation', async () => {
  const f = await fixture();
  await f.call(f.second, 'mint', [f.power.address, 777]);
  await f.call(f.router, 'configure', [2, ZeroAddress]);
  const data = await f.call(f.power, 'buildLiqPayload', [[f.second.address, f.first.address, stranger,
    1000, [f.second.address, f.first.address], 1, 1, 3000, '0x']]);
  await assert.rejects(() => f.call(f.power, 'requestFlashLoan', [0, f.first.address, 1000, data, 3000]), /slippage/);
  assert.equal(await f.call(f.second, 'balanceOf', [f.power.address]), 777n);
  assert.equal(await f.call(f.first, 'balanceOf', [f.aave.address]), 100000n);
  assert.equal(await f.call(f.first, 'balanceOf', [owner]), 0n);
});

test('owner and pause controls remain enforced; implementation cannot initialize', async () => {
  const f = await fixture();
  const data = await f.payload();
  await assert.rejects(() => f.call(f.power, 'requestFlashLoan', [0, f.first.address, 1000, data, 3000], strangerKey), /OwnableUnauthorizedAccount/);
  await f.call(f.power, 'pause');
  await assert.rejects(() => f.request(0), /EnforcedPause/);
  await assert.rejects(() => f.call(f.implementation, 'initialize', [owner, owner]), /InvalidInitialization/);
});

test('compiled ABI matches both Python request and management ABIs', () => {
  const actual = new Interface(artifacts['Power2.sol'].Power.abi);
  for (const filename of ['SONOFGOD.py', 'JEHOVAH2.0.py']) {
    const text = fs.readFileSync(path.join(__dirname, '..', filename), 'utf8');
    for (const name of ['EXECUTOR_ABI', 'EXECUTOR_MGMT_ABI']) {
      const match = text.match(new RegExp(`${name} = json\\.loads\\('([^']+)'\\)`));
      assert.ok(match, `${filename} ${name}`);
      for (const fragment of new Interface(JSON.parse(match[1])).fragments) {
        const generated = actual.getFunction(fragment.format('sighash'));
        assert.equal(generated.format('full'), fragment.format('full'));
      }
    }
  }
});

test('active-loan fields consume exactly five slots from the existing storage gap', () => {
  const layout = artifacts['Power2.sol'].Power.storageLayout;
  const active = layout.storage.find(s => s.label === '_activeLoan');
  const gap = layout.storage.find(s => s.label === '__gap');
  assert.equal(layout.types[active.type].numberOfBytes, '160');
  assert.equal(BigInt(gap.slot), BigInt(active.slot) + 5n);
  assert.equal(layout.types[gap.type].numberOfBytes, String(40 * 32));
});
