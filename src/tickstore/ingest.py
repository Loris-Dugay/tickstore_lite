import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = {
    "cm" : {
        "id": "int64",
        "price": "float64",
        "qty": "float64",
        "base_qty": "float64",
        "time": "int64",
        "is_buyer_maker": "bool"
    },
    "um" : {
        "id": "int64",
        "price": "float64",
        "qty": "float64",
        "quote_qty": "float64",
        "time": "int64",
        "is_buyer_maker": "bool"
    }
}

def check_csv(df: pd.DataFrame, market: str, schema_map: dict) -> bool:
    schema = pd.Series(schema_map[market])
    expected_cols = set(schema.keys())
    found_cols = set(df.columns)
    if found_cols != expected_cols:
        missing = sorted(expected_cols - found_cols)
        extra   = sorted(found_cols - expected_cols)
        print({"status": "wrong_columns", "missing": missing, "extra": extra})
        return False

    order = list(schema.keys())  # aligne l'ordre
    expected = pd.Series(schema, index=order)
    found = df.dtypes.astype(str).reindex(order)

    bad_mask = found != expected
    if bad_mask.any():
        bad = pd.DataFrame({"found": found[bad_mask], "expected": expected[bad_mask]})
        # print(bad)
        raise TypeError(f"type_mismatch for market='{market}'")

    return True


def read_csv(csv_file: str, dedup: bool) -> pd.DataFrame:
    df = pd.read_csv(csv_file)
    if dedup:
        df["nb_dedup"] = df.duplicated()
        nb_dedup = len(df.loc[df["nb_dedup"]])
        print("number of row erased from dedupe : ", nb_dedup)
        df = df.drop(["nb_dedup"], axis=1)
        return df.drop_duplicates()
    return df


def write_parquet(df: pd.DataFrame, parquet_file: str, dry_run, market: str, symbol: str) -> None:
    if dry_run:
        pass
    else:
        if market == "cm":
            df["quote_qty"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        else:
            df["base_qty"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df["symbol"] = pd.Series(symbol, index=df.index)
        table = pa.Table.from_pandas(df)
        parquet_file_path = parquet_file
        pq.write_table(table, parquet_file_path)


def ingest(csv_file: str, parquet_file: str, market: str, symbol: str, dedup: bool, dry_run: bool) -> int:
    list_file = os.listdir(csv_file)
    for file in list_file:
        if (file.find("trades") != -1) and symbol in file:
            df = read_csv(csv_file + file, dedup)
            try:
                if check_csv(df, market, SCHEMA): write_parquet(df, parquet_file + file.split(".")[0] + ".parquet", dry_run, market, symbol)
            except TypeError as e:
                print(e)
    return 0


if __name__ == "__main__":
    ingest("data/sample/", "data/lake/", "um", "BTCUSD", True, False)
