# rag-tools
Tools for testing and benchmarking AGTI RAG system

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![python](https://img.shields.io/badge/python-3.13-blue)](https://www.python.org)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)



## Installation Modes

| Command                           | Behavior                                                       |
| --------------------------------- | -------------------------------------------------------------- |
| `poetry sync`                     | Does not install PyTorch (import fails).                       |
| `poetry sync -E cpu`              | Installs PyTorch with CPU only.                                |
| `poetry sync -E cuda --with cuda` | Installs the CUDA variant of PyTorch. Expects NVIDIA hardware. |

>[!WARNING]
> The example below is likely not what you want:
>
> | Command               | Behavior                                                                 |
> | --------------------- | ------------------------------------------------------------------------ |
> | `poetry sync -E cuda` | Actually installs the CPU variant of PyTorch without errors or warnings. |


