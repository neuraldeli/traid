import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


def align_posts(posts_df: pd.DataFrame) -> pd.DataFrame:
    """Align posts to the nearest second without lookahead."""
    df = posts_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.floor("1s")
    return df.groupby("timestamp").size().rename("post_count")


def compute_book_metrics(book_df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling baseline and volatility statistics for book data."""
    df = book_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").set_index("timestamp")

    df["baseline_high"] = df["price"].rolling("60s").max()
    df["baseline_low"] = df["price"].rolling("60s").min()
    df["rolling_1s_stdev_bp"] = (
        df["price"].rolling("1s").std().fillna(0) / df["price"] * 10000
    )
    return df


def calc_x_bp(stdev_bp: float) -> float:
    """Width of the breakout bands in bp."""
    return min(8.0, max(2.0, 0.5 * stdev_bp))


@dataclass
class Trade:
    post_time: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: str
    entry_price: float
    exit_price: float
    pnl_bp: float
    exit_reason: str


def simulate_trades(
    posts_df: pd.DataFrame,
    book_df: pd.DataFrame,
    config: Dict,
    latency_ms: int,
) -> List[Trade]:
    posts = align_posts(posts_df)
    book = compute_book_metrics(book_df)

    slip_bp_param = config.get("slip_bp_param", 1.0)
    trades: List[Trade] = []

    for post_time in posts.index:
        arm_time = post_time + pd.Timedelta(milliseconds=latency_ms)

        if arm_time not in book.index:
            idx = book.index.searchsorted(arm_time)
            if idx >= len(book):
                continue
            current = book.iloc[idx]
            start_idx = idx
        else:
            current = book.loc[arm_time]
            start_idx = book.index.get_loc(arm_time)

        baseline_high = current["baseline_high"]
        baseline_low = current["baseline_low"]
        stdev_bp = current["rolling_1s_stdev_bp"]
        x_bp = calc_x_bp(stdev_bp)

        long_stop = baseline_high * (1 + x_bp / 10000)
        short_stop = baseline_low * (1 - x_bp / 10000)

        position = None
        entry_price = entry_time = None
        tp = sl = time_exit = None

        for t, row in book.iloc[start_idx:].iterrows():
            price = row["price"]
            spread_bp = row.get("spread_bp", 0.0)
            slippage_bp = max(0.5 * spread_bp, slip_bp_param)

            if position is None:
                if price >= long_stop:
                    entry_price = long_stop * (1 + slippage_bp / 10000)
                    entry_time = t
                    position = "long"
                    tp = entry_price * (1 + 12 / 10000)
                    sl = entry_price * (1 - 6 / 10000)
                    time_exit = entry_time + pd.Timedelta(seconds=180)
                elif price <= short_stop:
                    entry_price = short_stop * (1 - slippage_bp / 10000)
                    entry_time = t
                    position = "short"
                    tp = entry_price * (1 - 12 / 10000)
                    sl = entry_price * (1 + 6 / 10000)
                    time_exit = entry_time + pd.Timedelta(seconds=180)
            else:
                exit_price = exit_reason = None
                if position == "long":
                    if price >= tp:
                        exit_price = tp
                        exit_reason = "tp"
                    elif price <= sl:
                        exit_price = sl
                        exit_reason = "sl"
                    elif t >= time_exit:
                        exit_price = price
                        exit_reason = "time"
                    if exit_price is not None:
                        pnl_bp = (exit_price - entry_price) / entry_price * 10000
                        trades.append(
                            Trade(
                                post_time,
                                entry_time,
                                t,
                                "long",
                                entry_price,
                                exit_price,
                                pnl_bp,
                                exit_reason,
                            )
                        )
                        break
                else:  # short
                    if price <= tp:
                        exit_price = tp
                        exit_reason = "tp"
                    elif price >= sl:
                        exit_price = sl
                        exit_reason = "sl"
                    elif t >= time_exit:
                        exit_price = price
                        exit_reason = "time"
                    if exit_price is not None:
                        pnl_bp = (entry_price - exit_price) / entry_price * 10000
                        trades.append(
                            Trade(
                                post_time,
                                entry_time,
                                t,
                                "short",
                                entry_price,
                                exit_price,
                                pnl_bp,
                                exit_reason,
                            )
                        )
                        break

    return trades


def run(posts_path: Path, book_path: Path, config_path: Path, out_path: Path, latency_ms: int) -> None:
    posts = pd.read_csv(posts_path)
    book = pd.read_csv(book_path)
    with open(config_path) as f:
        config = json.load(f)

    trades = simulate_trades(posts, book, config, latency_ms)
    df = pd.DataFrame([t.__dict__ for t in trades])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    for t in trades:
        print(f"{t.post_time} {t.side} pnl={t.pnl_bp:.2f}bp via {t.exit_reason}")
    if trades:
        avg = sum(tr.pnl_bp for tr in trades) / len(trades)
        print(f"trades={len(trades)} avg_pnl_bp={avg:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Trump OCO")
    parser.add_argument("--posts", type=Path, required=True, help="Path to posts CSV")
    parser.add_argument("--book", type=Path, required=True, help="Path to book CSV")
    parser.add_argument("--latency_ms", type=int, default=0, help="Latency in ms")
    parser.add_argument("--config", type=Path, required=True, help="Path to config file")
    parser.add_argument("--out", type=Path, required=True, help="Output parquet path")
    args = parser.parse_args()

    run(args.posts, args.book, args.config, args.out, args.latency_ms)


if __name__ == "__main__":
    main()
