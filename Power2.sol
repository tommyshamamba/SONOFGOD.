// file: contracts/Power.sol
// SPDX-License-Identifier: MIT
// Contract finalized by tommyshamamba on 2025-11-06 13:17:09
pragma solidity ^0.8.22;

// --- OpenZeppelin Imports ---
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts-upgradeable/v5.0.2/contracts/proxy/utils/UUPSUpgradeable.sol";
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts-upgradeable/v5.0.2/contracts/access/OwnableUpgradeable.sol";
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts-upgradeable/v5.0.2/contracts/utils/PausableUpgradeable.sol";
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts-upgradeable/v5.0.2/contracts/utils/ReentrancyGuardUpgradeable.sol";
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts/v5.0.2/contracts/token/ERC20/IERC20.sol";
import "https://raw.githubusercontent.com/OpenZeppelin/openzeppelin-contracts-upgradeable/v5.0.2/contracts/utils/introspection/ERC165Upgradeable.sol";

// --- Libraries ---
library SafeERC20Compat {
    function safeTransfer(IERC20 token, address to, uint256 value) internal { _callOptionalReturn(token, abi.encodeWithSelector(token.transfer.selector, to, value)); }
    function forceApprove(IERC20 token, address spender, uint256 value) internal {
        bytes memory approveData = abi.encodeWithSelector(token.approve.selector, spender, value);
        (bool success, bytes memory returndata) = address(token).call(approveData);
        if (success && (returndata.length == 0 || abi.decode(returndata, (bool)))) return;
        _callOptionalReturn(token, abi.encodeWithSelector(token.approve.selector, spender, 0));
        _callOptionalReturn(token, approveData);
    }
    function safeIncreaseAllowance(IERC20 token, address spender, uint256 value) internal {
        uint256 newAllowance = token.allowance(address(this), spender) + value;
        forceApprove(token, spender, newAllowance);
    }
    function safeDecreaseAllowance(IERC20 token, address spender, uint256 value) internal {
        uint256 oldAllowance = token.allowance(address(this), spender);
        if (oldAllowance < value) revert("SafeERC20: insufficient allowance");
        forceApprove(token, spender, oldAllowance - value);
    }
    function _callOptionalReturn(IERC20 token, bytes memory data) private {
        (bool success, bytes memory returndata) = address(token).call(data);
        require(success, "SafeERC20: low-level call failed");
        if (returndata.length > 0) require(abi.decode(returndata, (bool)), "SafeERC20: ERC20 op failed");
    }
}

// --- Interfaces ---
interface IAavePool { function flashLoanSimple(address receiver, address asset, uint256 amount, bytes calldata params, uint16 refCode) external; function liquidationCall(address collateral, address debt, address user, uint256 debtToCover, bool receiveAToken) external; }
interface IBalancerVault { enum SwapKind { GIVEN_IN, GIVEN_OUT } struct BatchSwapStep { bytes32 poolId; uint256 assetInIndex; uint256 assetOutIndex; uint256 amount; bytes userData; } struct FundManagement { address sender; bool fromInternalBalance; address payable recipient; bool toInternalBalance; } function flashLoan(address recipient, address[] calldata tokens, uint256[] calldata amounts, bytes calldata userData) external; function batchSwap(SwapKind kind, BatchSwapStep[] calldata swaps, address[] calldata assets, FundManagement calldata funds, int256[] calldata limits, uint256 deadline) external payable returns (int256[] memory); }
interface IBalancerFlashLoanRecipient { function receiveFlashLoan(address[] calldata tokens, uint256[] calldata amounts, uint256[] calldata feeAmounts, bytes calldata userData) external; }
interface IUniswapV3Pool { function flash(address recipient, uint256 amount0, uint256 amount1, bytes calldata data) external; function token0() external view returns (address); function token1() external view returns (address); }
interface IUniswapV3FlashCallback { function uniswapV3FlashCallback(uint256 fee0, uint256 fee1, bytes calldata data) external; }
interface ISwapRouterV3 { struct ExactInputSingleParams { address tokenIn; address tokenOut; uint24 fee; address recipient; uint256 deadline; uint256 amountIn; uint256 amountOutMinimum; uint160 sqrtPriceLimitX96; } struct ExactInputParams { bytes path; address recipient; uint256 deadline; uint256 amountIn; uint256 amountOutMinimum; } function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256); function exactInput(ExactInputParams calldata params) external payable returns (uint256); }
interface IUniswapV2Router { function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory); }
interface IDODO { function flashLoan(uint256 baseAmount, uint256 quoteAmount, address assetTo, bytes calldata data) external; }
interface IDODOCallee { function dodoFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) external; }
interface ICurvePool { function exchange(int128 i, int128 j, uint256 dx, uint256 min_dy) external returns (uint256); }
interface IWETH { function withdraw(uint256) external; function deposit() external payable; function balanceOf(address) external view returns (uint256); }

/* ================= Contract ================= */
contract Power is UUPSUpgradeable, OwnableUpgradeable, PausableUpgradeable, ReentrancyGuardUpgradeable, ERC165Upgradeable, IBalancerFlashLoanRecipient, IUniswapV3FlashCallback, IDODOCallee {
    using SafeERC20Compat for IERC20;

    address public guardian;
    uint256 public constant CONTRACT_VERSION = 14;

    address public immutable AAVE_POOL;
    address public immutable BALANCER_VAULT;
    address public immutable UNIV3_ROUTER;
    address public immutable SUSHI_ROUTER;
    address public immutable CAMELOT_ROUTER;
    address public immutable WETH_ADDRESS;

    uint256 public deadlineExtension;
    uint256 public upgradeDelay;
    address public pendingImplementation;
    uint256 public pendingImplementationTimestamp;
    mapping(address => bool) public allowedRouters;
    mapping(address => bool) public allowedPools;
    mapping(address => uint256) public dodoFeeBps;
    bool public unwrapWETHOnPayout;

    uint256 private constant FEE_BPS_DENOMINATOR = 10_000;
    bytes4 private constant ARB_SELECTOR = 0x260751a8;
    bytes4 private constant LIQUIDATE_SELECTOR = 0x64917a78;

    enum FlashloanProvider { AAVE, BALANCER, UNISWAP, DODO }
    enum DexTag { UNISWAP_V3, SUSHI, CAMELOT, BALANCER, CURVE }
    struct BalancerSwapParams { IBalancerVault.BatchSwapStep[] steps; address[] assets; int256[] limits; }
    struct CurveSwapParams { address pool; int128 i; int128 j; }
    struct ArbitrageOpportunity { uint256 version; address[] path; uint256 minOutput; DexTag dex; uint256 deadline; uint24 feeTier; bytes uniV3Path; BalancerSwapParams balancerParams; CurveSwapParams curveParams; }
    struct LiquidationOpportunity { address collateralAsset; address debtAsset; address user; uint256 debtToCover; address[] swapPath; DexTag dex; uint256 minProfit; uint256 deadline; bytes uniV3Path; }

    event GuardianTransferred(address indexed previousGuardian, address indexed newGuardian);
    event FlashloanRequested(FlashloanProvider provider, address indexed token, uint256 amount);
    event ArbitrageExecuted(address indexed asset, uint256 amount, uint256 profit);
    event LiquidationExecuted(address indexed collateral, address indexed debt, uint256 profit);
    event Withdraw(address indexed token, uint256 amount);
    event UpgradeProposed(address indexed implementation, uint256 at);
    event UpgradeCancelled(address indexed implementation);
    event RouterAllowed(address indexed router, bool allowed);
    event PoolAllowed(address indexed pool, bool allowed);
    event DeadlineExtensionSet(uint256 newSeconds);
    event UpgradeDelaySet(uint256 newDelay);
    event DodoFeeBpsSet(address indexed pool, uint256 bps);
    event Rescued(address indexed token, address indexed to, uint256 amount);
    event UnwrapWETHOnPayoutSet(bool enabled);

    error NotAave(); error NotBalancer(); error InvalidUniV3Caller(); error InvalidDODOCaller();
    error RouterNotAllowed(); error PoolNotAllowed(); error BadPayload(); error InvalidVersion();
    error ZeroProfitNotAllowed(); error InvalidDeadline(); error InvalidUpgrade(); error NotAuthorized();

    uint256[45] private __gap;

    constructor() {
        _disableInitializers();
        AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
        BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
        UNIV3_ROUTER   = 0xE592427A0AEce92De3Edee1F18E0157C05861564;
        SUSHI_ROUTER   = 0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506;
        CAMELOT_ROUTER = 0xc873fEcbd354f5A56E00E710B90EF4201db2448d;
        WETH_ADDRESS   = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    }

    function initialize(address owner_, address guardian_) public initializer {
        __UUPSUpgradeable_init(); __Ownable_init(owner_); __Pausable_init(); __ReentrancyGuard_init(); __ERC165_init();
        if (guardian_ == address(0)) revert("guardian=0");
        guardian = guardian_; emit GuardianTransferred(address(0), guardian_);
        allowedRouters[UNIV3_ROUTER] = true; allowedRouters[SUSHI_ROUTER] = true; allowedRouters[CAMELOT_ROUTER] = true;
        deadlineExtension = 180; upgradeDelay = 2 days; unwrapWETHOnPayout = true; emit UnwrapWETHOnPayoutSet(true);
    }

    function pause() external { if (msg.sender != owner() && msg.sender != guardian) revert NotAuthorized(); _pause(); }
    function unpause() external onlyOwner { _unpause(); }
    function setGuardian(address guardian_) external onlyOwner { if (guardian_ == address(0)) revert("guardian=0"); emit GuardianTransferred(guardian, guardian_); guardian = guardian_; }
    function setRouterAllowed(address r, bool a) external onlyOwner { allowedRouters[r] = a; emit RouterAllowed(r, a); }
    function setPoolAllowed(address p, bool a) external onlyOwner { allowedPools[p] = a; emit PoolAllowed(p, a); }
    function setDodoFeeBps(address pool, uint256 bps) external onlyOwner { if (pool == address(0)) revert("pool=0"); if (bps > FEE_BPS_DENOMINATOR) revert("bps>10000"); dodoFeeBps[pool] = bps; emit DodoFeeBpsSet(pool, bps); }
    function setDeadlineExtension(uint256 newSeconds) external onlyOwner { if (newSeconds == 0 || newSeconds > 300) revert("Deadline must be 1-300s"); deadlineExtension = newSeconds; emit DeadlineExtensionSet(newSeconds); }
    function setUpgradeDelay(uint256 newDelay) external onlyOwner { if (newDelay < 1 days || newDelay > 14 days) revert("Delay must be 1-14 days"); upgradeDelay = newDelay; emit UpgradeDelaySet(newDelay); }
    function setUnwrapWETHOnPayout(bool enabled) external onlyOwner { unwrapWETHOnPayout = enabled; emit UnwrapWETHOnPayoutSet(enabled); }

    function proposeUpgrade(address newImplementation) external onlyOwner {
        if (newImplementation == address(0)) revert InvalidUpgrade();
        _validateImplementation(newImplementation);
        pendingImplementation = newImplementation; pendingImplementationTimestamp = block.timestamp;
        emit UpgradeProposed(newImplementation, pendingImplementationTimestamp);
    }

    function cancelUpgrade() external {
        if (msg.sender != owner() && msg.sender != guardian) revert NotAuthorized();
        emit UpgradeCancelled(pendingImplementation);
        pendingImplementation = address(0); pendingImplementationTimestamp = 0;
    }

    function _authorizeUpgrade(address newImplementation) internal override onlyOwner {
        if (pendingImplementation != newImplementation) revert("Not proposed or cancelled");
        if (block.timestamp < pendingImplementationTimestamp + upgradeDelay) revert("Upgrade delay not met");
        pendingImplementation = address(0); pendingImplementationTimestamp = 0;
    }

    function _validateImplementation(address newImplementation) private view {
        uint256 size; assembly { size := extcodesize(newImplementation) }
        if (size == 0) revert InvalidUpgrade();
        (bool ok, bytes memory ret) = newImplementation.staticcall(abi.encodeWithSignature("proxiableUUID()"));
        if (!ok || ret.length != 32 || abi.decode(ret, (bytes32)) != 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc) revert InvalidUpgrade();
    }

    function supportsInterface(bytes4 interfaceId) public view virtual override(ERC165Upgradeable) returns (bool) {
        return interfaceId == type(IBalancerFlashLoanRecipient).interfaceId || interfaceId == type(IUniswapV3FlashCallback).interfaceId || interfaceId == type(IDODOCallee).interfaceId || super.supportsInterface(interfaceId);
    }

    function requestFlashLoan(FlashloanProvider p, address token, uint256 amount, bytes calldata params, uint256 deadline) public onlyOwner whenNotPaused nonReentrant {
        if (deadline > block.timestamp + 3600 || block.timestamp > deadline) revert InvalidDeadline();
        if (token == address(0)) revert("token=0");
        if (amount == 0) revert("amount=0");
        emit FlashloanRequested(p, token, amount);
        if (p == FlashloanProvider.AAVE) { IAavePool(AAVE_POOL).flashLoanSimple(address(this), token, amount, params, 0); } 
        else if (p == FlashloanProvider.BALANCER) { address[] memory tokens = new address[](1); tokens[0] = token; uint256[] memory amounts = new uint256[](1); amounts[0] = amount; IBalancerVault(BALANCER_VAULT).flashLoan(address(this), tokens, amounts, params); } 
        else if (p == FlashloanProvider.UNISWAP) { (address pool, bytes memory inner) = abi.decode(params, (address, bytes)); if (!allowedPools[pool]) revert PoolNotAllowed(); address t0 = IUniswapV3Pool(pool).token0(); address t1 = IUniswapV3Pool(pool).token1(); if (token != t0 && token != t1) revert("uni:token!pool"); uint256 amount0 = token == t0 ? amount : 0; uint256 amount1 = token == t1 ? amount : 0; bytes memory cbData = abi.encode(amount0, amount1, inner); IUniswapV3Pool(pool).flash(address(this), amount0, amount1, cbData); } 
        else if (p == FlashloanProvider.DODO) { (address dodoPool, address baseToken, bytes memory inner) = abi.decode(params, (address, address, bytes)); if (!allowedPools[dodoPool]) revert PoolNotAllowed(); bytes memory cbData = abi.encode(dodoPool, baseToken, inner); IDODO(dodoPool).flashLoan(baseToken == token ? amount : 0, baseToken != token ? amount : 0, address(this), cbData); }
    }

    function executeOperation(address asset, uint256 amount, uint256 premium, address, bytes calldata params) external nonReentrant returns (bool) { if (msg.sender != AAVE_POOL) revert NotAave(); _dispatchAndProcessCalldata(asset, amount, premium, params); IERC20(asset).safeTransfer(AAVE_POOL, amount + premium); return true; }
    function receiveFlashLoan(address[] calldata tokens, uint256[] calldata amounts, uint256[] calldata fees, bytes calldata userData) external override nonReentrant { if (msg.sender != BALANCER_VAULT) revert NotBalancer(); if (tokens.length != 1) revert("single-only"); _dispatchAndProcessCalldata(tokens[0], amounts[0], fees[0], userData); IERC20(tokens[0]).safeTransfer(BALANCER_VAULT, amounts[0] + fees[0]); }
    function uniswapV3FlashCallback(uint256 fee0, uint256 fee1, bytes calldata data) external override nonReentrant { (uint256 amount0, uint256 amount1, bytes memory inner) = abi.decode(data, (uint256, uint256, bytes)); address pool = msg.sender; if (!allowedPools[pool]) revert InvalidUniV3Caller(); address asset; uint256 amount; uint256 fee; if (amount0 > 0) { asset = IUniswapV3Pool(pool).token0(); amount = amount0; fee = fee0; } else { asset = IUniswapV3Pool(pool).token1(); amount = amount1; fee = fee1; } _dispatchAndProcessMemory(asset, amount, fee, inner); IERC20(asset).safeTransfer(pool, amount + fee); }
    function dodoFlashLoanCall(address sender, uint256 baseAmount, uint256 quoteAmount, bytes calldata data) external override nonReentrant { (address dodoPool, address asset, bytes memory inner) = abi.decode(data, (address, address, bytes)); if (sender != address(this) || msg.sender != dodoPool || !allowedPools[dodoPool]) revert InvalidDODOCaller(); uint256 borrowed = baseAmount > 0 ? baseAmount : quoteAmount; uint256 bps = dodoFeeBps[dodoPool]; uint256 fee = (bps > 0) ? (borrowed * bps + FEE_BPS_DENOMINATOR - 1) / FEE_BPS_DENOMINATOR : 0; _dispatchAndProcessMemory(asset, borrowed, fee, inner); IERC20(asset).safeTransfer(msg.sender, borrowed + fee); }

    function _dispatchAndProcessCalldata(address asset, uint256 amount, uint256 fee, bytes calldata payload) internal {
        if (payload.length < 4) revert BadPayload();
        bytes4 sel; assembly { sel := shr(224, calldataload(payload.offset)) }
        if (sel == ARB_SELECTOR) { ArbitrageOpportunity memory opp = abi.decode(payload[4:], (ArbitrageOpportunity)); if (opp.version != CONTRACT_VERSION) revert InvalidVersion(); if (opp.minOutput == 0) revert ZeroProfitNotAllowed(); _arb(asset, amount, fee, opp); } 
        else if (sel == LIQUIDATE_SELECTOR) { LiquidationOpportunity memory oppL = abi.decode(payload[4:], (LiquidationOpportunity)); if (oppL.minProfit == 0) revert ZeroProfitNotAllowed(); _liquidate(asset, amount, fee, oppL); } 
        else { revert BadPayload(); }
    }

    function _dispatchAndProcessMemory(address asset, uint256 amount, uint256 fee, bytes memory payload) internal {
        if (payload.length < 4) revert BadPayload();
        bytes4 sel; assembly { sel := shr(224, mload(add(payload, 0x20))) }
        if (sel == ARB_SELECTOR) { bytes memory tail = _decodeAfterSelectorMem(payload); ArbitrageOpportunity memory opp = abi.decode(tail, (ArbitrageOpportunity)); if (opp.version != CONTRACT_VERSION) revert InvalidVersion(); if (opp.minOutput == 0) revert ZeroProfitNotAllowed(); _arb(asset, amount, fee, opp); } 
        else if (sel == LIQUIDATE_SELECTOR) { bytes memory tail2 = _decodeAfterSelectorMem(payload); LiquidationOpportunity memory oppL = abi.decode(tail2, (LiquidationOpportunity)); if (oppL.minProfit == 0) revert ZeroProfitNotAllowed(); _liquidate(asset, amount, fee, oppL); } 
        else { revert BadPayload(); }
    }

    function _decodeAfterSelectorMem(bytes memory payload) internal pure returns (bytes memory out_) { uint256 len = payload.length - 4; out_ = new bytes(len); for (uint256 i = 0; i < len; i++) { out_[i] = payload[i + 4]; } }
    
    function _arb(address asset, uint256 amount, uint256 premium, ArbitrageOpportunity memory opp) internal {
        if (block.timestamp > opp.deadline) revert InvalidDeadline();
        if (opp.dex == DexTag.UNISWAP_V3) _execUniV3Arb(asset, amount, opp);
        else if (opp.dex == DexTag.SUSHI) _execUniV2LikeArb(asset, amount, opp, SUSHI_ROUTER);
        else if (opp.dex == DexTag.CAMELOT) _execUniV2LikeArb(asset, amount, opp, CAMELOT_ROUTER);
        else if (opp.dex == DexTag.BALANCER) _execBalancerArb(amount, opp);
        else if (opp.dex == DexTag.CURVE) _execCurveArb(amount, opp);
        else revert("dex");
        uint256 bal = IERC20(asset).balanceOf(address(this));
        if (bal < amount + premium) revert("profit");
        uint256 profit = bal - (amount + premium);
        if (profit > 0) { emit ArbitrageExecuted(asset, amount, profit); _autoPayout(asset, profit); }
    }

    function _liquidate(address flashAsset, uint256 amount, uint256 premium, LiquidationOpportunity memory opp) internal {
        if (block.timestamp > opp.deadline) revert InvalidDeadline();
        if (flashAsset != opp.debtAsset) revert("liq:asset!=debt");
        IERC20(opp.debtAsset).safeIncreaseAllowance(AAVE_POOL, amount);
        IAavePool(AAVE_POOL).liquidationCall(opp.collateralAsset, opp.debtAsset, opp.user, opp.debtToCover, false);
        uint256 curDebtAllow = IERC20(opp.debtAsset).allowance(address(this), AAVE_POOL);
        if (curDebtAllow > 0) IERC20(opp.debtAsset).safeDecreaseAllowance(AAVE_POOL, curDebtAllow);
        uint256 collBal = IERC20(opp.collateralAsset).balanceOf(address(this));
        if (collBal > 0) {
            if (opp.swapPath.length < 2 || opp.swapPath[0] != opp.collateralAsset || opp.swapPath[opp.swapPath.length-1] != opp.debtAsset) revert("liq:path-mismatch");
            uint256 debtBalance = IERC20(opp.debtAsset).balanceOf(address(this));
            uint256 requiredDebt = amount + premium + opp.minProfit;
            uint256 minDebtOut = (debtBalance >= requiredDebt) ? 0 : requiredDebt - debtBalance;
            if (minDebtOut > 0) {
                if (opp.dex == DexTag.UNISWAP_V3) { if (opp.uniV3Path.length == 0) revert("liq:v3-path-bytes"); _swapUniV3(opp.collateralAsset, collBal, minDebtOut, opp.uniV3Path); } 
                else if (opp.dex == DexTag.SUSHI || opp.dex == DexTag.CAMELOT) { address router = (opp.dex == DexTag.SUSHI) ? SUSHI_ROUTER : CAMELOT_ROUTER; _swapUniV2Like(router, collBal, minDebtOut, opp.swapPath); } 
                else { revert("liq:dex"); }
            }
        }
        uint256 debtAfter = IERC20(opp.debtAsset).balanceOf(address(this));
        if (debtAfter < amount + premium) revert("liq:insolvent");
        if (debtAfter < amount + premium + opp.minProfit) revert("liq:profit");
        uint256 liqProfit = debtAfter - (amount + premium);
        if (liqProfit > 0) { emit LiquidationExecuted(opp.collateralAsset, opp.debtAsset, liqProfit); _autoPayout(opp.debtAsset, liqProfit); }
    }

    function _execUniV3Arb(address tokenIn, uint256 amountIn, ArbitrageOpportunity memory opp) internal { if (opp.uniV3Path.length > 0) { _swapUniV3(tokenIn, amountIn, opp.minOutput, opp.uniV3Path); } else { if (opp.path.length < 2) revert("v3-path"); uint24 fee = opp.feeTier == 0 ? 3000 : opp.feeTier; _swapUniV3Single(opp.path[0], opp.path[1], fee, amountIn, opp.minOutput); } }
    function _execUniV2LikeArb(address tokenIn, uint256 amountIn, ArbitrageOpportunity memory opp, address router) internal { if (opp.path.length < 2) revert("v2-path"); if (tokenIn != opp.path[0]) revert("arb:v2-path-mismatch"); _swapUniV2Like(router, amountIn, opp.minOutput, opp.path); }
    function _execBalancerArb(uint256 amountIn, ArbitrageOpportunity memory opp) internal { BalancerSwapParams memory p = opp.balancerParams; if (p.steps.length == 0 || p.assets.length == 0) revert("Balancer: invalid params"); if (p.limits.length != p.assets.length) revert("Balancer: limits!=assets"); address tokenIn = p.assets[p.steps[0].assetInIndex]; if (p.steps[0].amount != amountIn) revert("Balancer: amount mismatch"); IERC20(tokenIn).safeIncreaseAllowance(BALANCER_VAULT, amountIn); IBalancerVault(BALANCER_VAULT).batchSwap(IBalancerVault.SwapKind.GIVEN_IN, p.steps, p.assets, IBalancerVault.FundManagement({ sender: address(this), fromInternalBalance: false, recipient: payable(address(this)), toInternalBalance: false }), p.limits, opp.deadline); uint256 cur = IERC20(tokenIn).allowance(address(this), BALANCER_VAULT); if (cur > 0) IERC20(tokenIn).safeDecreaseAllowance(BALANCER_VAULT, cur); }
    function _execCurveArb(uint256 amountIn, ArbitrageOpportunity memory opp) internal { CurveSwapParams memory p = opp.curveParams; if (!allowedPools[p.pool]) revert PoolNotAllowed(); if (opp.path.length < 2) revert("Curve: invalid path"); address tokenIn = opp.path[0]; IERC20(tokenIn).safeIncreaseAllowance(p.pool, amountIn); ICurvePool(p.pool).exchange(p.i, p.j, amountIn, opp.minOutput); uint256 cur = IERC20(tokenIn).allowance(address(this), p.pool); if (cur > 0) IERC20(tokenIn).safeDecreaseAllowance(p.pool, cur); }
    function _swapUniV3(address tokenIn, uint256 amountIn, uint256 amountOutMin, bytes memory path) internal { if (!allowedRouters[UNIV3_ROUTER]) revert RouterNotAllowed(); IERC20(tokenIn).safeIncreaseAllowance(UNIV3_ROUTER, amountIn); ISwapRouterV3(UNIV3_ROUTER).exactInput(ISwapRouterV3.ExactInputParams({ path: path, recipient: address(this), deadline: block.timestamp + deadlineExtension, amountIn: amountIn, amountOutMinimum: amountOutMin })); uint256 cur = IERC20(tokenIn).allowance(address(this), UNIV3_ROUTER); if (cur > 0) IERC20(tokenIn).safeDecreaseAllowance(UNIV3_ROUTER, cur); }
    function _swapUniV3Single(address tokenIn, address tokenOut, uint24 fee, uint256 amountIn, uint256 amountOutMin) internal { if (!allowedRouters[UNIV3_ROUTER]) revert RouterNotAllowed(); IERC20(tokenIn).safeIncreaseAllowance(UNIV3_ROUTER, amountIn); ISwapRouterV3(UNIV3_ROUTER).exactInputSingle(ISwapRouterV3.ExactInputSingleParams({ tokenIn: tokenIn, tokenOut: tokenOut, fee: fee, recipient: address(this), deadline: block.timestamp + deadlineExtension, amountIn: amountIn, amountOutMinimum: amountOutMin, sqrtPriceLimitX96: 0 })); uint256 cur = IERC20(tokenIn).allowance(address(this), UNIV3_ROUTER); if (cur > 0) IERC20(tokenIn).safeDecreaseAllowance(UNIV3_ROUTER, cur); }
    function _swapUniV2Like(address router, uint256 amountIn, uint256 amountOutMin, address[] memory path) internal { if (!allowedRouters[router]) revert RouterNotAllowed(); IERC20(path[0]).safeIncreaseAllowance(router, amountIn); IUniswapV2Router(router).swapExactTokensForTokens( amountIn, amountOutMin, path, address(this), block.timestamp + deadlineExtension); uint256 cur = IERC20(path[0]).allowance(address(this), router); if (cur > 0) IERC20(path[0]).safeDecreaseAllowance(router, cur); }

    function _autoPayout(address asset, uint256 amount) internal {
        address payable owner_ = payable(owner());
        if (asset == WETH_ADDRESS && unwrapWETHOnPayout) { IWETH(WETH_ADDRESS).withdraw(amount); (bool ok, ) = owner_.call{value: amount}(""); require(ok, "ETH sweep failed"); emit Withdraw(address(0), amount); } 
        else { IERC20(asset).safeTransfer(owner_, amount); emit Withdraw(asset, amount); }
    }

    function rescueToken(address token, address to, uint256 amount) external onlyOwner whenPaused nonReentrant { if (to == address(0)) revert("to=0"); IERC20(token).safeTransfer(to, amount); emit Rescued(token, to, amount); }
    function rescueETH(address payable to, uint256 amount) external onlyOwner whenPaused nonReentrant { if (to == address(0)) revert("to=0"); (bool ok, ) = to.call{value: amount}(""); require(ok, "ETH rescue failed"); emit Rescued(address(0), to, amount); }
    
    function buildArbPayload(ArbitrageOpportunity memory opp) public pure returns (bytes memory) { return abi.encodePacked(ARB_SELECTOR, abi.encode(opp)); }
    function buildLiqPayload(LiquidationOpportunity memory opp) public pure returns (bytes memory) { return abi.encodePacked(LIQUIDATE_SELECTOR, abi.encode(opp)); }

    receive() external payable {}
}