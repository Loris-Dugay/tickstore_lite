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


def read_csv(csv_file):
    csv_file_path = csv_file
    df = pd.read_csv(csv_file_path)
    return df


def write_parquet(df, parquet_file):
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    table = pa.Table.from_pandas(df)
    parquet_file_path = parquet_file
    pq.write_table(table, parquet_file_path)


def ingest(csv_file, parquet_file, market, symbol):
    list_file = os.listdir(csv_file)
    for file in list_file:
        if (file.find("trades") != -1) and symbol in file:
            df = read_csv(csv_file + file)
            try:
                if check_csv(df, market, SCHEMA): write_parquet(df, parquet_file + file.split(".")[0] + ".parquet")
            except TypeError:
                print("Bad error type on file : ", file)

if __name__ == "__main__":
    ingest("data/sample/", "data/lake/", "cm", "BTCUSD")
