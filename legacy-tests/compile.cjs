const fs = require('node:fs');
const path = require('node:path');
const solc = require('solc');

function compile() {
  const sources = {
    'Power2.sol': { content: fs.readFileSync(path.join(__dirname, '..', 'Power2.sol'), 'utf8') },
    'LenderMocks.sol': { content: fs.readFileSync(path.join(__dirname, 'LenderMocks.sol'), 'utf8') },
    'ERC1967Proxy.sol': { content: fs.readFileSync(path.join(__dirname, '..', 'ERC1967Proxy.sol'), 'utf8') },
  };
  const result = JSON.parse(solc.compile(JSON.stringify({
    language: 'Solidity', sources,
    settings: { viaIR: true, optimizer: { enabled: true, runs: 200 }, evmVersion: 'shanghai',
      outputSelection: { '*': { '*': ['abi', 'evm.bytecode.object', 'evm.deployedBytecode.object', 'storageLayout'] } } },
  }), { import(name) {
    try { return { contents: fs.readFileSync(require.resolve(name), 'utf8') }; }
    catch { return { error: `Import unavailable in pinned dependencies: ${name}` }; }
  } }));
  const errors = (result.errors || []).filter(e => e.severity === 'error');
  if (errors.length) throw new Error(errors.map(e => e.formattedMessage).join('\n'));
  const power = result.contracts['Power2.sol'].Power;
  const size = power.evm.deployedBytecode.object.length / 2;
  if (size > 24576) throw new Error(`Power runtime bytecode ${size} exceeds EIP-170`);
  return result.contracts;
}

module.exports = { compile };
if (require.main === module) {
  const contracts = compile();
  console.log(`Solidity ${solc.version()}: Power compiled (${contracts['Power2.sol'].Power.evm.deployedBytecode.object.length / 2} runtime bytes)`);
}
