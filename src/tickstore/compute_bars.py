import duckdb
import os
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def compute(parquet_file, tick, date_start, date_end):
    con = duckdb.connect()
    bar_data = con.execute("""
        SET TimeZone = 'Etc/GMT';

        CREATE MACRO time_bucket(ts, step_s, origin) AS
        CAST(origin AS TIMESTAMP) +
        INTERVAL (CAST(FLOOR(date_diff('second', CAST(origin AS TIMESTAMP), CAST(ts AS TIMESTAMP)) / step_s) AS INTEGER) * step_s) SECOND;

        WITH params AS (
            SELECT
                CAST($time AS bigint) as step_s,
                CAST($start AS timestamptz)   AS origin
        ),
        src AS (
            SELECT
                id as id,
                symbol as symbol,
                time AS ts,
                time_bucket(time, p.step_s, p.origin)  AS bucket,
                price AS price,
                qty AS qty,
                CASE
                    WHEN base_qty IS NOT NULL AND quote_qty IS NULL THEN (qty * 100 / price)
                    WHEN quote_qty IS NOT NULL AND base_qty IS NULL THEN quote_qty
                END AS base_qty,
                is_buyer_maker AS is_buyer_maker
            FROM read_parquet($file) s
            CROSS JOIN params p
            WHERE time >= $start
            AND time <  $end
        ),

        open_data AS (
            SELECT
                bucket,
                ts AS open_time,
                price AS open
            FROM (
                SELECT bucket, ts, price, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY ts) AS rn
                FROM src
            )
            WHERE rn = 1
        ),

        close_data AS (
            SELECT
                bucket,
                ts AS close_time,
                price AS close
            FROM (
                SELECT bucket, ts, price, ROW_NUMBER() OVER (PARTITION BY bucket ORDER by ts DESC, id DESC) AS rn
                FROM src
            )
            WHERE rn = 1
        ),

        agg_data AS (
            SELECT
                symbol,
                bucket,
                MAX(price) AS high,
                MIN(price) AS low,
                SUM(qty) as volume,
                SUM(base_qty) AS quote_volume,
                COUNT(1) AS count,
                SUM(CASE WHEN NOT is_buyer_maker THEN qty ELSE 0 END) AS taker_buy_volume,
                SUM(CASE WHEN NOT is_buyer_maker THEN base_qty ELSE 0 END) taker_buy_quote_volume,
                SUM(price * base_qty) / SUM(base_qty) AS vwap
            FROM src
            GROUP BY symbol, bucket
        ),

        add_blank AS (
            SELECT gs AS bucket
            FROM generate_series(
                CAST($start as timestamptz),
                CAST($end as timestamptz) - INTERVAL ($time) SECOND,
                INTERVAL ($time) SECOND
            ) AS t(gs)
          ),

        joined AS (
          SELECT b.bucket, a.*
          FROM add_blank b
          LEFT JOIN agg_data a USING (bucket)
        ),

        merged AS (
        SELECT
            symbol,
            bucket,
            od.open_time,
            od.open,
            high,
            low,
            cd.close,
            COALESCE(volume, 0) AS volume,
            cd.close_time,
            COALESCE(quote_volume, 0) AS quote_volume,
            COALESCE(count, 0) AS count,
            COALESCE(taker_buy_volume, 0) AS taker_buy_volume,
            COALESCE(taker_buy_quote_volume, 0) AS taker_buy_quote_volume,
            vwap
        FROM joined
        LEFT JOIN open_data  od USING (bucket)
        LEFT JOIN close_data cd USING (bucket)
        order by bucket
        ),

        get_last AS (
            SELECT
                bucket,
                last_value(close IGNORE NULLS) OVER (
                    ORDER BY bucket
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS last_close,
                last_value(symbol IGNORE NULLS) OVER (
                    ORDER BY bucket
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS last_symbol
            FROM merged
        )

        SELECT
            COALESCE(m.symbol, last_symbol) AS symbol,
            COALESCE(m.open_time, gl.bucket) AS open_time,
            COALESCE(m.open, gl.last_close) AS open,
            COALESCE(m.high, gl.last_close) AS high,
            COALESCE(m.low, gl.last_close) AS low,
            COALESCE(m.close, gl.last_close) AS close,
            m.volume,
            COALESCE(m.close_time, gl.bucket + INTERVAL ($time) SECOND) AS close_time,
            m.quote_volume,
            m.count,
            m.taker_buy_volume,
            m.taker_buy_quote_volume,
            COALESCE(m.vwap, 0) AS vwap
        FROM merged m
        JOIN get_last gl USING(bucket)
        ORDER BY m.bucket
    """,
    {"time":tick,
     "file":parquet_file,
     "start":date_start,
     "end":date_end}).df()
    return bar_data


def write_parquet(df, output):
    table = pa.Table.from_pandas(df)
    parquet_file_path = output
    pq.write_table(table, parquet_file_path)


def compute_bar(parquet_file, output, tick):
    list_file = os.listdir(parquet_file)
    for file in list_file:
        m = re.search(r'(\d{4}-\d{2}-\d{2})(?=\.parquet$)', file)
        date = m.group(1)
        date_start = pd.to_datetime(date)
        date_end = date_start + pd.Timedelta(days=1)
        df = compute(parquet_file + file, tick, date_start.strftime("%Y-%m-%d %H:%M:%S"), date_end.strftime("%Y-%m-%d %H:%M:%S"))
        output_file_name = "bar_1s_" + file.split("_")[0] + "_" + date
        write_parquet(df, output + output_file_name + ".parquet")


if __name__ == "__main__":
    tick = 3600 * 6
    compute_bar("data/lake/", "data/derived/bars_1s/", tick)
