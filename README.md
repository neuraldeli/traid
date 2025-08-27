# Traid

Traid is a research and backtesting framework for algorithmic trading.

## Getting Started

### Prerequisites

- Python 3.11

### Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
```

Or use the Makefile:

```bash
make setup
```

### Running Tests

No tests are implemented yet, but the test infrastructure is ready:

```bash
make test
```

### Repository Structure

- `backtest/` – backtesting utilities
- `data/` – sample datasets and loaders
- `reports/` – generated reports
- `scripts/` – helper scripts
- `tests/` – test modules

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
