# Third-party components

The root MIT license applies to original code owned by Tommy Shamamba. It does not replace licenses, copyright notices, trademarks or usage conditions for third-party code, packages, model weights or datasets.

- JavaScript dependencies are recorded in each project's `package.json` and `package-lock.json`. Installed packages retain their own license files.
- Python dependencies are listed in Trace's requirements files. Their licenses remain applicable.
- Trace downloads U2NetP model weights from [rembg's upstream release](https://github.com/danielgatis/rembg/releases/tag/v0.0.0), with provenance in `trace-stores/scripts/download_model.py`. The weights are not committed or relicensed by this repository. Review upstream model and training-data terms before redistribution or commercial deployment.
- Solidity dependencies, including OpenZeppelin, retain their upstream licenses. Compiler imports and the local contract test toolchain identify their versions.
- Terraform modules and providers retain their respective upstream licenses. Module source versions and provider lockfiles identify what is used.

Project and company names in demonstrations do not imply affiliation, endorsement or commercial deployment. Preserve upstream notices when redistributing dependencies or derived artifacts.
