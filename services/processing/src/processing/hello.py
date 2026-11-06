from pathlib import Path

from processing.spark import build_spark

CSV_PATH = Path(__file__).resolve().parents[2] / "examples" / "sample.csv"


def main() -> None:
    spark = build_spark("hello")

    df = spark.read.csv(str(CSV_PATH), header=True, inferSchema=True)

    df_filtered = df.filter(df["valor"] > 10)

    df_filtered.show()

    spark.stop()


if __name__ == "__main__":
    main()
