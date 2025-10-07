import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = {
    "id": "int64",
    "price": "float64",
    "qty": "float64",
    "base_qty": "float64",
    "time": "int64",
    "is_buyer_maker": "bool"
}

def check_csv(df, schema_map):
    schema = pd.Series(schema_map)
    ok = (
        set(df.columns) == set(schema.index) and
        df.dtypes.astype(str).reindex(schema.index).eq(schema).all()
    )
    if not ok:
        bad = pd.DataFrame({"found": df.dtypes.astype(str).reindex(schema.index), "expected": schema})
        bad = bad[bad["found"].isna() | (bad["found"] != bad["expected"])]
        print(bad)
    return ok


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
        if (file.find("trades") != -1):
            df = read_csv(csv_file + file)
            if check_csv(df, SCHEMA) == True:
                write_parquet(df, parquet_file + file.split(".")[0] + ".parquet")
        else:
            print("Non accetable file : ", file)

if __name__ == "__main__":
    ingest("data/sample/", "data/lake/", "cm", "BTCUSD")
