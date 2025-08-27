import argparse
from pathlib import Path
import pandas as pd


def align_posts(posts_df: pd.DataFrame) -> pd.DataFrame:
    """Align posts to the nearest second without lookahead.

    Parameters
    ----------
    posts_df : pd.DataFrame
        DataFrame with a `timestamp` column.

    Returns
    -------
    pd.DataFrame
        Posts with timestamps floored to the second and aggregated by second.
    """
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


def run(posts_path: Path, book_path: Path, config_path: Path, out_path: Path, latency_ms: int) -> None:
    posts = pd.read_csv(posts_path)
    book = pd.read_csv(book_path)

    aligned_posts = align_posts(posts)
    book_metrics = compute_book_metrics(book)

    result = book_metrics.join(aligned_posts, how="left").fillna({"post_count": 0})
    result.reset_index().to_parquet(out_path, index=False)


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
