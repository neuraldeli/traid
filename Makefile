.PHONY: setup test backtest

setup:
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e .

test:
	.venv/bin/pytest

backtest:
	@echo "Backtest placeholder"
