// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Proxy} from "@openzeppelin/contracts/proxy/Proxy.sol";
import {ERC1967Utils} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Utils.sol";

/**
 * @dev This is the proxy contract recommended to use alongside UUPS implementations.
 *
 * It implements a constructor that stores an initial implementation and
 * optionally performs an initialization call.
 */
contract ERC1967Proxy is Proxy {
    constructor(address _logic, bytes memory _data) payable {
        ERC1967Utils.upgradeToAndCall(_logic, _data);
    }

    function _implementation() internal view override returns (address) {
        return ERC1967Utils.getImplementation();
    }
}