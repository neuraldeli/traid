import subprocess
from pathlib import Path

import pandas as pd


def test_alignment_and_output():
    out_path = Path("reports/test_output.parquet")
    if out_path.exists():
        out_path.unlink()

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
    subprocess.run(cmd, check=True)
    assert out_path.exists(), "Output parquet was not created"

    df = pd.read_parquet(out_path)
    # ensure timestamps are aligned to the second
    timestamps = pd.to_datetime(df["timestamp"])
    assert (timestamps == timestamps.dt.floor("1s")).all()

    # check edge at window boundary
    pre = df.loc[df["timestamp"] == pd.Timestamp("2020-01-01T00:00:59Z")]
    boundary = df.loc[df["timestamp"] == pd.Timestamp("2020-01-01T00:01:00Z")]
    assert int(pre["post_count"].iloc[0]) == 1
    assert int(boundary["post_count"].iloc[0]) == 1

    out_path.unlink()
