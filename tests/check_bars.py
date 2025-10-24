import polars as pl
import os
from pathlib import Path


def check_bars(input: str, check: str) -> int:
    input_dir = str(Path.cwd() / input)
    check_dir = str(Path.cwd() / check)
    df = pl.read_delta(input_dir)

    df_btc = df.filter((pl.col("symbol") == "BTCUSDT"))
    df_bch = df.filter((pl.col("symbol") == "BCHUSDT"))
    df_mana = df.filter((pl.col("symbol") == "MANAUSDT"))

    df_btc = df_btc.select(pl.col("open").cast(pl.Float64).round(5),
        pl.col("high").cast(pl.Float64).round(5),
        pl.col("low").cast(pl.Float64).round(5),
        pl.col("close").cast(pl.Float64).round(5),
        pl.col("volume").cast(pl.Float64).round(5),
        pl.col("quote_volume").cast(pl.Float64).round(5),
        pl.col("count").cast(pl.Int64),
        pl.col("taker_buy_volume").cast(pl.Float64).round(5),
        pl.col("taker_buy_quote_volume").cast(pl.Float64).round(5)).with_row_index("index")

    df_bch = df_bch.select(pl.col("open").cast(pl.Float64).round(5),
        pl.col("high").cast(pl.Float64).round(5),
        pl.col("low").cast(pl.Float64).round(5),
        pl.col("close").cast(pl.Float64).round(5),
        pl.col("volume").cast(pl.Float64).round(5),
        pl.col("quote_volume").cast(pl.Float64).round(5),
        pl.col("count").cast(pl.Int64),
        pl.col("taker_buy_volume").cast(pl.Float64).round(5),
        pl.col("taker_buy_quote_volume").cast(pl.Float64).round(5)).with_row_index("index")

    df_mana = df_mana.select(pl.col("open").cast(pl.Float64).round(5),
        pl.col("high").cast(pl.Float64).round(5),
        pl.col("low").cast(pl.Float64).round(5),
        pl.col("close").cast(pl.Float64).round(5),
        pl.col("volume").cast(pl.Float64).round(5),
        pl.col("quote_volume").cast(pl.Float64).round(5),
        pl.col("count").cast(pl.Int64),
        pl.col("taker_buy_volume").cast(pl.Float64).round(5),
        pl.col("taker_buy_quote_volume").cast(pl.Float64).round(5)).with_row_index("index")


    generated_bars = {
        "BTC" : df_btc,
        "BCH": df_bch,
        "MANA": df_mana
    }

    compare_check = {}
    check_files = os.listdir(check_dir)

    for file in check_files:
        key = file.split("USD")[0]
        tmp = pl.read_csv(check_dir + "/" + file)
        tmp = tmp.select(pl.col("open").cast(pl.Float64).round(5),
            pl.col("high").cast(pl.Float64).round(5),
            pl.col("low").cast(pl.Float64).round(5),
            pl.col("close").cast(pl.Float64).round(5),
            pl.col("volume").cast(pl.Float64).round(5),
            pl.col("quote_volume").cast(pl.Float64).round(5),
            pl.col("count").cast(pl.Int64),
            pl.col("taker_buy_volume").cast(pl.Float64).round(5),
            pl.col("taker_buy_quote_volume").cast(pl.Float64).round(5)).with_row_index("index")
        compare_check[key] = tmp

    is_equal = 0
    for key_generate, key_check in zip(generated_bars, compare_check):
        key_check = key_generate  # Assurer que les symboles correspondent
        df_gen = generated_bars[key_generate]
        df_check = compare_check[key_check]

        state = df_gen.equals(df_check)

        print(f"\n Is {key_generate} equal to {key_check}_check : {state}")


    # 1. Vérifier les schémas
        if df_gen.schema != df_check.schema:
            print("Schema mismatch:")
            print(f"Generated schema: {df_gen.schema}")
            print(f"Check schema: {df_check.schema}")
            is_equal = 1
            continue

        # 2. Vérifier les tailles
        if len(df_gen) != len(df_check):
            print(f"Row count mismatch: Generated={len(df_gen)}, Check={len(df_check)}")
            is_equal = 1


        # 3. Joindre sur bar_time pour aligner les lignes
        df_diff = df_gen.join(df_check, on="index", how="full", suffix="_check")

        # 4. Vérifier les lignes manquantes ou supplémentaires
        missing_in_gen = df_diff.filter(pl.col("open").is_null() & pl.col("open_check").is_not_null())
        missing_in_check = df_diff.filter(pl.col("open").is_not_null() & pl.col("open_check").is_null())

        if not missing_in_gen.is_empty():
            print(f"Rows present in check but missing in generated for {key_generate}:")
            print(missing_in_gen.select(
                pl.col("index"),
                pl.col("open_check"),
                pl.col("close_check"),
                pl.col("volume_check")
            ))
            is_equal = 1

        if not missing_in_check.is_empty():
            print(f"Rows present in generated but missing in check for {key_generate}:")
            print(missing_in_check.select(
                pl.col("index"),
                pl.col("open"),
                pl.col("close"),
                pl.col("volume")
            ))
            is_equal = 1

        # 5. Comparer les valeurs pour les lignes alignées
        for col in df_gen.columns:
            if col == "index":
                continue
            # Comparaison avec tolérance pour les colonnes flottantes
            mismatches = df_diff.filter(
                (pl.col(col).is_not_null() & pl.col(f"{col}_check").is_not_null()) &
                (abs(pl.col(col) - pl.col(f"{col}_check")) > 1e-5)
            )
            if not mismatches.is_empty():
                print(f"Differences in column '{col}' for {key_generate}:")
                print(mismatches.select(
                    pl.col("index"),
                    pl.col(col).alias(f"{col}_generated"),
                    pl.col(f"{col}_check")
                ).limit(10))
                is_equal = 1

    return is_equal

if __name__ == "__main__":
    input = "tests/data/bars/1_minute"
    check = "tests/data/checks/1_minute"
    check_bars(input, check)
