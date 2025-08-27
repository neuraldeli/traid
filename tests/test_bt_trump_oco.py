import subprocess
from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from backtest.bt_trump_oco import calc_x_bp, simulate_trades


def test_cli_runs_end_to_end(tmp_path):
    out_path = tmp_path / "trades.csv"
    cmd = [
        "python",
        "backtest/bt_trump_oco.py",
        "--posts",
        "tests/fixtures/posts.csv",
        "--book",
        "tests/fixtures/book.csv",
        "--latency_ms",
        "0",
        "--config",
        "tests/fixtures/config.json",
        "--out",
        str(out_path),
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    assert "pnl" in proc.stdout
    df = pd.read_csv(out_path)
    assert "pnl_bp" in df.columns


def test_calc_x_bp():
    assert calc_x_bp(0) == 2
    assert calc_x_bp(4) == 2
    assert calc_x_bp(10) == 5
    assert calc_x_bp(40) == 8


def test_time_stop():
    posts = pd.DataFrame({"timestamp": ["2020-01-01T00:00:00Z"], "text": ["post"]})
    times = pd.date_range("2020-01-01T00:00:00Z", periods=200, freq="1s")
    prices = [100] + [100.03] * 199
    book = pd.DataFrame({"timestamp": times, "price": prices})
    trades = simulate_trades(posts, book, {}, 0)
    assert trades, "no trade generated"
    trade = trades[0]
    assert trade.exit_reason == "time"
    assert (trade.exit_time - trade.entry_time) >= pd.Timedelta(seconds=180)
