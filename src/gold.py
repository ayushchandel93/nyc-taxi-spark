# src/gold.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path
import time

# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("NYC Taxi — Gold Layer") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

# ── Gold Tables ───────────────────────────────────────────────────────────────

def build_hourly_metrics(df, gold_path: str):
    """Revenue + trip volume by hour of day — for time series dashboard."""
    hourly = df.groupBy("pickup_month", "pickup_hour", "time_of_day") \
               .agg(
                   F.count("*")                          .alias("total_trips"),
                   F.round(F.sum("fare_amount"), 2)      .alias("total_revenue"),
                   F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
                   F.round(F.avg("trip_distance"), 2)    .alias("avg_distance"),
                   F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
                   F.round(F.avg("tip_pct"), 2)          .alias("avg_tip_pct"),
                   F.round(F.avg("passenger_count"), 2)  .alias("avg_passengers"),
               ) \
               .orderBy("pickup_month", "pickup_hour")

    hourly.write.mode("overwrite").parquet(f"{gold_path}/hourly_metrics")
    print(f"[gold] hourly_metrics — {hourly.count()} rows")
    hourly.show(5)


def build_daily_metrics(df, gold_path: str):
    """Revenue + trip volume by day — for trend analysis."""
    daily = df.groupBy(
                F.to_date("tpep_pickup_datetime").alias("pickup_date"),
                "pickup_month",
                "is_weekend"
              ) \
              .agg(
                  F.count("*")                          .alias("total_trips"),
                  F.round(F.sum("fare_amount"), 2)      .alias("total_revenue"),
                  F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
                  F.round(F.avg("trip_distance"), 2)    .alias("avg_distance"),
                  F.round(F.sum("tip_amount"), 2)       .alias("total_tips"),
                  F.round(F.avg("tip_pct"), 2)          .alias("avg_tip_pct"),
              ) \
              .orderBy("pickup_date")

    daily.write.mode("overwrite").parquet(f"{gold_path}/daily_metrics")
    print(f"[gold] daily_metrics — {daily.count()} rows")
    daily.show(5)


def build_zone_metrics(df, gold_path: str):
    """Trip volume + revenue by pickup location — for heatmap."""
    zone = df.groupBy("PULocationID", "pickup_month") \
             .agg(
                 F.count("*")                          .alias("total_trips"),
                 F.round(F.sum("fare_amount"), 2)      .alias("total_revenue"),
                 F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
                 F.round(F.avg("trip_distance"), 2)    .alias("avg_distance"),
                 F.round(F.avg("tip_pct"), 2)          .alias("avg_tip_pct"),
             ) \
             .orderBy(F.desc("total_trips"))

    zone.write.mode("overwrite").parquet(f"{gold_path}/zone_metrics")
    print(f"[gold] zone_metrics — {zone.count()} rows")
    zone.show(5)


def build_payment_metrics(df, gold_path: str):
    """Revenue breakdown by payment type."""
    payment = df.groupBy("payment_type", "pickup_month") \
                .agg(
                    F.count("*")                     .alias("total_trips"),
                    F.round(F.sum("fare_amount"), 2) .alias("total_revenue"),
                    F.round(F.avg("tip_pct"), 2)     .alias("avg_tip_pct"),
                ) \
                .orderBy("pickup_month", F.desc("total_trips"))

    payment.write.mode("overwrite").parquet(f"{gold_path}/payment_metrics")
    print(f"[gold] payment_metrics — {payment.count()} rows")
    payment.show()


def build_weekday_metrics(df, gold_path: str):
    """Weekday vs weekend performance comparison."""
    weekday = df.groupBy("pickup_dayofweek", "is_weekend", "pickup_month") \
                .agg(
                    F.count("*")                          .alias("total_trips"),
                    F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
                    F.round(F.avg("trip_distance"), 2)    .alias("avg_distance"),
                    F.round(F.avg("tip_pct"), 2)          .alias("avg_tip_pct"),
                ) \
                .withColumn(
                    "day_name",
                    F.when(F.col("pickup_dayofweek") == 1, "Sunday")
                     .when(F.col("pickup_dayofweek") == 2, "Monday")
                     .when(F.col("pickup_dayofweek") == 3, "Tuesday")
                     .when(F.col("pickup_dayofweek") == 4, "Wednesday")
                     .when(F.col("pickup_dayofweek") == 5, "Thursday")
                     .when(F.col("pickup_dayofweek") == 6, "Friday")
                     .otherwise("Saturday")
                ) \
                .orderBy("pickup_dayofweek")

    weekday.write.mode("overwrite").parquet(f"{gold_path}/weekday_metrics")
    print(f"[gold] weekday_metrics — {weekday.count()} rows")
    weekday.show()


# ── Main ──────────────────────────────────────────────────────────────────────

def run_gold(
    silver_path: str = "/home/jovyan/data/silver",
    gold_path:   str = "/home/jovyan/data/gold"
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n══ Gold Layer ══\n")
    start = time.time()

    # Read silver — cache since we run 5 aggregations on same data
    df = spark.read.parquet(silver_path).cache()
    total = df.count()
    print(f"[gold] Silver rows loaded: {total:,}")

    # Build all gold tables
    build_hourly_metrics(df,  gold_path)
    build_daily_metrics(df,   gold_path)
    build_zone_metrics(df,    gold_path)
    build_payment_metrics(df, gold_path)
    build_weekday_metrics(df, gold_path)

    elapsed = round(time.time() - start, 1)
    print(f"\n[gold] All tables done in {elapsed}s → {gold_path}")
    print(f"[gold] Tables written:")
    for p in sorted(Path(gold_path).glob("*")):
        if p.is_dir():
            print(f"  {p.name}/")

    spark.stop()


if __name__ == "__main__":
    run_gold()