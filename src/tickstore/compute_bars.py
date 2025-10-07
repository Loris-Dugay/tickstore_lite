import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def compute(parquet_file, date_start, date_end):
    con = duckdb.connect()
    bar_data = con.execute("""
        SET TimeZone = 'Etc/GMT';

        WITH src AS (
            SELECT
                id as id,
                time AS ts,
                DATE_TRUNC('minutes', time) AS bucket,
                price AS price,
                qty AS qty,
                (qty * 100 / price) AS base_qty,
                is_buyer_maker AS is_buyer_maker
            FROM read_parquet($file)
            WHERE time >= $start
            AND time < $end
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
            GROUP BY bucket
        ),

        add_blank AS (
            SELECT gs AS bucket
            FROM generate_series(
                CAST($start as timestamptz),
                CAST($end as timestamptz) - INTERVAL 1 MINUTE,
                INTERVAL 1 MINUTE
            ) AS t(gs)
          ),

        joined AS (
          SELECT b.bucket, a.*
          FROM add_blank b
          LEFT JOIN agg_data a USING (bucket)
        ),

        merged AS (
        SELECT
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
                ) AS last_close
            FROM merged
        )

        SELECT
            'BTCUSD' AS symbol,
            COALESCE(m.open_time, gl.bucket) AS open_time,
            COALESCE(m.open, gl.last_close) AS open,
            COALESCE(m.high, gl.last_close) AS high,
            COALESCE(m.low, gl.last_close) AS low,
            COALESCE(m.close, gl.last_close) AS close,
            m.volume,
            COALESCE(m.close_time, gl.bucket + INTERVAL 59 SECOND) AS close_time,
            m.quote_volume,
            m.count,
            m.taker_buy_volume,
            m.taker_buy_quote_volume,
            COALESCE(m.vwap, 0) AS vwap
        FROM merged m
        JOIN get_last gl USING(bucket)
        ORDER BY m.bucket
    """,
    {"file":parquet_file,
     "start":date_start,
     "end":date_end}).df()
    return bar_data


def write_parquet(df, output):
    table = pa.Table.from_pandas(df)
    parquet_file_path = output
    pq.write_table(table, parquet_file_path)


def compute_bar(parquet_file, date_start, date_end, output):
    df = compute(parquet_file, date_start, date_end)
    write_parquet(df, output + "bar_1s_BTCUSD_" + date_start.split(" ")[0] + ".parquet")


if __name__ == "__main__":
    compute_bar("data/lake/BTCUSD_251226-trades-2025-09-23.parquet", '2025-09-23 00:00:00', '2025-09-24 00:00:00', "data/derived/bars_1s/")
