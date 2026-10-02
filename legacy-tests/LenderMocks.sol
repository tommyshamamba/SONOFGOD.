// SPDX-License-Identifier: MIT
pragma solidity ^0.8.22;
import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract MockToken is ERC20 {
    constructor() ERC20("Local test token", "TEST") {}
    function mint(address to, uint256 amount) external { _mint(to, amount); }
}

contract MockRouter {
    uint256 public extra;
    address public reenterLender;
    function configure(uint256 extra_, address reenterLender_) external { extra = extra_; reenterLender = reenterLender_; }
    function swapExactTokensForTokens(uint256 amount, uint256 minimum, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory result) {
        require(block.timestamp <= deadline, "expired");
        require(IERC20(path[0]).transferFrom(msg.sender, address(this), amount), "input");
        if (reenterLender != address(0)) MockLender(reenterLender).tryReentry();
        uint256 output = amount + extra;
        require(output >= minimum, "slippage");
        MockToken(path[path.length - 1]).mint(to, output);
        result = new uint256[](path.length);
        result[0] = amount; result[path.length - 1] = output;
    }
}

contract MockLender {
    address public token0;
    address public token1;
    uint256 public fee;
    uint256 public mode; // 1 initiator; 2 amount; 3 payload; 4 missing; 5 replay; 6 malformed array
    uint256 public dodoKind;
    uint256 public reentriesRejected;
    address public receiver;
    bytes public lastCall;
    function configure(address first, address second, uint256 fee_, uint256 mode_, uint256 kind) external {
        token0 = first; token1 = second; fee = fee_; mode = mode_; dodoKind = kind;
    }
    function _BASE_TOKEN_() external view returns (address) { return token0; }
    function _QUOTE_TOKEN_() external view returns (address) { return token1; }
    function liquidationCall(address collateral, address debt, address, uint256 amount, bool) external {
        require(IERC20(debt).transferFrom(msg.sender, address(this), amount), "liquidation debt");
        MockToken(collateral).mint(msg.sender, amount);
    }
    function invoke(address target, bytes calldata data) external returns (bytes memory result) {
        (bool ok, bytes memory ret) = target.call(data);
        if (!ok) assembly { revert(add(ret, 32), mload(ret)) }
        return ret;
    }
    function tryReentry() external {
        (bool ok,) = receiver.call(lastCall);
        require(!ok, "nested callback unexpectedly accepted");
        reentriesRejected++;
    }
    function _callback(address target, bytes memory callData) private {
        receiver = target; lastCall = callData;
        if (mode == 4) return;
        (bool ok, bytes memory ret) = target.call(callData);
        if (!ok) assembly { revert(add(ret, 32), mload(ret)) }
        if (mode == 5) {
            (ok, ret) = target.call(callData);
            if (!ok) assembly { revert(add(ret, 32), mload(ret)) }
        }
    }
    function flashLoanSimple(address target, address asset, uint256 amount, bytes calldata params, uint16) external {
        uint256 beforeBalance = IERC20(asset).balanceOf(address(this));
        IERC20(asset).transfer(target, amount);
        _callback(target, abi.encodeWithSignature("executeOperation(address,uint256,uint256,address,bytes)", asset,
            mode == 2 ? amount + 1 : amount, fee, mode == 1 ? address(1) : msg.sender,
            mode == 3 ? bytes("altered") : params));
        if (mode == 4) return;
        require(IERC20(asset).allowance(target, address(this)) == amount + fee, "exact repayment allowance");
        IERC20(asset).transferFrom(target, address(this), amount + fee);
        require(IERC20(asset).balanceOf(address(this)) >= beforeBalance + fee, "Aave repayment");
    }
    function flashLoan(address target, address[] calldata assets, uint256[] calldata amounts, bytes calldata data) external {
        uint256 beforeBalance = IERC20(assets[0]).balanceOf(address(this));
        IERC20(assets[0]).transfer(target, amounts[0]);
        uint256[] memory fees = new uint256[](mode == 6 ? 0 : 1);
        if (fees.length != 0) fees[0] = fee;
        _callback(target, abi.encodeWithSignature("receiveFlashLoan(address[],uint256[],uint256[],bytes)", assets, amounts, fees, data));
        require(IERC20(assets[0]).balanceOf(address(this)) == beforeBalance + fee, "Balancer repayment");
    }
    function flash(address target, uint256 a0, uint256 a1, bytes calldata data) external {
        address asset = a0 > 0 ? token0 : token1;
        uint256 amount = a0 + a1;
        uint256 beforeBalance = IERC20(asset).balanceOf(address(this));
        IERC20(asset).transfer(target, amount);
        _callback(target, abi.encodeWithSignature("uniswapV3FlashCallback(uint256,uint256,bytes)", a0 > 0 ? fee : 0, a1 > 0 ? fee : 0, data));
        require(IERC20(asset).balanceOf(address(this)) == beforeBalance + fee, "Uniswap repayment");
    }
    function flashLoan(uint256 a0, uint256 a1, address target, bytes calldata data) external {
        address asset = a0 > 0 ? token0 : token1;
        uint256 amount = a0 + a1;
        uint256 beforeBalance = IERC20(asset).balanceOf(address(this));
        IERC20(asset).transfer(target, amount);
        bytes4 selector = dodoKind == 0 ? bytes4(keccak256("DVMFlashLoanCall(address,uint256,uint256,bytes)")) :
            dodoKind == 1 ? bytes4(keccak256("DPPFlashLoanCall(address,uint256,uint256,bytes)")) :
            dodoKind == 2 ? bytes4(keccak256("DSPFlashLoanCall(address,uint256,uint256,bytes)")) :
            bytes4(keccak256("dodoFlashLoanCall(address,uint256,uint256,bytes)"));
        _callback(target, abi.encodeWithSelector(selector, msg.sender, a0, a1, data));
        require(IERC20(asset).balanceOf(address(this)) == beforeBalance + fee, "DODO repayment");
    }
}
