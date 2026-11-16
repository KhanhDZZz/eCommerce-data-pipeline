import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

DAG_ID = "ecommerce_customer_churn_pipeline"

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SOURCE_FILE = os.getenv("SOURCE_FILE", "eCommerce-dataset.csv")

PG_HOST = os.getenv("PG_HOST", "host.docker.internal")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
POSTGRES_JDBC_JAR = os.getenv("POSTGRES_JDBC_JAR", "/opt/spark-jars/postgresql.jar")
SPARK_MAX_PARALLEL_TASKS = int(os.getenv("SPARK_MAX_PARALLEL_TASKS", "2"))

SCHEMA = "public"
BRONZE_TABLE = "bronze_ecommerce_dataset"
SILVER_TABLE = "silver_customer_behavior"
FACT_TABLE = "fact_customer_behavior"
UNKNOWN = "Unknown"

NULL_TOKENS = ["", "null", "none", "nan", "n/a", "na", "-"]

TEXT_COLS = ["gender", "country", "city", "signup_quarter"]
MONEY_COLS = ["average_order_value", "lifetime_value", "credit_balance"]
DECIMAL_COLS = [
    "age", "membership_years", "session_duration_avg", "pages_per_session",
    "cart_abandonment_rate", "total_purchases", "discount_usage_rate",
    "returns_rate", "email_open_rate", "social_media_engagement_score",
    "mobile_app_usage",
]
INT_COLS = [
    "login_frequency", "wishlist_items", "days_since_last_purchase",
    "customer_service_calls", "product_reviews_written", "payment_method_diversity",
]
BOOL_SOURCE = "churned"
BOOL_TARGET = "is_churned"

NON_NEGATIVE_COLS = [
    "membership_years", "login_frequency", "session_duration_avg",
    "pages_per_session", "wishlist_items", "total_purchases",
    "average_order_value", "days_since_last_purchase", "customer_service_calls",
    "product_reviews_written", "mobile_app_usage", "lifetime_value", "credit_balance",
]
RANGE_RULES = {
    "age": (13, 100),
    "cart_abandonment_rate": (0, 100),
    "discount_usage_rate": (0, 100),
    "returns_rate": (0, 100),
    "email_open_rate": (0, 100),
    "social_media_engagement_score": (0, 100),
    "average_order_value": (0, 1000),
}
VALID_QUARTERS = ["Q1", "Q2", "Q3", "Q4"]

AGE_BUCKETS = [
    ("Under 18", 0, 18), ("18-24", 18, 25), ("25-34", 25, 35), ("35-44", 35, 45),
    ("45-54", 45, 55), ("55-64", 55, 65), ("65+", 65, None),
]
TENURE_BUCKETS = [
    ("Under 1 year", 0, 1), ("1-2 years", 1, 2), ("2-4 years", 2, 4), ("4+ years", 4, None),
]
RECENCY_BUCKETS = [
    ("0-14 days", 0, 15), ("15-30 days", 15, 31), ("31-60 days", 31, 61),
    ("61-90 days", 61, 91), ("90+ days", 91, None),
]

DIMENSIONS = {
    "dim_location": {"key": "location_key", "cols": ["country", "city"]},
    "dim_gender": {"key": "gender_key", "cols": ["gender"]},
    "dim_signup_quarter": {"key": "signup_quarter_key", "cols": ["signup_quarter"]},
    "dim_age_group": {"key": "age_group_key", "cols": ["age_group"], "buckets": AGE_BUCKETS, "source": "age"},
    "dim_membership_tenure": {"key": "membership_tenure_key", "cols": ["membership_tenure"], "buckets": TENURE_BUCKETS, "source": "membership_years"},
    "dim_purchase_recency": {"key": "purchase_recency_key", "cols": ["purchase_recency"], "buckets": RECENCY_BUCKETS, "source": "days_since_last_purchase"},
}
FACT_MEASURES = (
    ["age", "membership_years", "login_frequency", "session_duration_avg",
     "pages_per_session", "cart_abandonment_rate", "wishlist_items",
     "total_purchases", "average_order_value", "days_since_last_purchase",
     "discount_usage_rate", "returns_rate", "email_open_rate",
     "customer_service_calls", "product_reviews_written",
     "social_media_engagement_score", "mobile_app_usage",
     "payment_method_diversity", "lifetime_value", "credit_balance"]
)


def T(name: str) -> str:
    return f"{SCHEMA}.{name}"


def _require_password() -> str:
    if PG_PASSWORD is None:
        raise ValueError(
            "Chưa đặt biến môi trường PG_PASSWORD (khai báo trong docker-compose.yaml / file .env)."
        )
    return PG_PASSWORD


def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=PG_USER,
        password=_require_password(),
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
    )
    return create_engine(url)


def run_statements(statements: list) -> None:
    engine = get_engine()
    try:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
    finally:
        engine.dispose()


def scalar(sql: str):
    engine = get_engine()
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar()
    finally:
        engine.dispose()


@contextmanager
def spark_session(app_name: str):
    from pyspark.sql import SparkSession

    if not Path(POSTGRES_JDBC_JAR).is_file():
        raise FileNotFoundError(f"Không thấy JDBC driver tại {POSTGRES_JDBC_JAR}")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.jars", POSTGRES_JDBC_JAR)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        yield spark
    finally:
        spark.stop()


def _jdbc_url() -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"


def read_table(spark, table: str):
    return (
        spark.read.format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", T(table))
        .option("user", PG_USER)
        .option("password", _require_password())
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_table(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", _jdbc_url())
        .option("dbtable", T(table))
        .option("user", PG_USER)
        .option("password", _require_password())
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )


def clean_text(col):
    from pyspark.sql import functions as F

    trimmed = F.trim(col)
    return F.when(F.lower(trimmed).isin(NULL_TOKENS), None).otherwise(trimmed)


def to_number(col):
    from pyspark.sql import functions as F

    x = F.trim(col).cast("double")
    inf = F.lit(float("inf"))
    return F.when(x.isNull() | F.isnan(x) | (x == inf) | (x == -inf), None).otherwise(x)


def to_boolean(col):
    from pyspark.sql import functions as F

    v = F.lower(F.trim(col))
    return (
        F.when(v.isin("1", "1.0", "true", "t", "yes", "y"), True)
        .when(v.isin("0", "0.0", "false", "f", "no", "n"), False)
    )


def bucket_label(col, buckets):
    from pyspark.sql import functions as F

    expr = None
    for label, lo, hi in buckets:
        cond = col >= lo if hi is None else (col >= lo) & (col < hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise(UNKNOWN)


def silver_dataframe(bronze):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    raw_cols = [c for c in bronze.columns if not c.startswith("_")]
    hash_input = F.concat_ws(
        "||", *[F.coalesce(F.trim(F.col(c)), F.lit("<NULL>")) for c in raw_cols]
    )
    has_data = None
    for c in raw_cols:
        cond = F.col(c).isNotNull()
        has_data = cond if has_data is None else (has_data | cond)

    df = bronze.filter(has_data).withColumn("customer_id", F.sha2(hash_input, 256))

    for c in TEXT_COLS:
        df = df.withColumn(c, clean_text(F.col(c)))
    df = df.withColumn("gender", F.initcap(F.col("gender")))
    df = df.withColumn("signup_quarter", F.upper(F.col("signup_quarter")))
    df = df.withColumn(
        "signup_quarter",
        F.when(F.col("signup_quarter").isin(VALID_QUARTERS), F.col("signup_quarter")),
    )

    numeric_cols = MONEY_COLS + DECIMAL_COLS + INT_COLS
    for c in numeric_cols:
        x = to_number(F.col(c))
        if c in NON_NEGATIVE_COLS:
            x = F.when(x < 0, None).otherwise(x)
        if c in RANGE_RULES:
            lo, hi = RANGE_RULES[c]
            x = F.when((x < lo) | (x > hi), None).otherwise(x)
        if c in MONEY_COLS:
            x = F.round(x, 2).cast(DecimalType(18, 2))
        elif c in DECIMAL_COLS:
            x = F.round(x, 2).cast(DecimalType(10, 2))
        else:
            x = F.round(x).cast("int")
        df = df.withColumn(c, x)

    df = df.withColumn(BOOL_TARGET, to_boolean(F.col(BOOL_SOURCE)))
    df = df.dropDuplicates(["customer_id"])
    ordered = (
        ["customer_id", "age", "gender", "country", "city", "membership_years",
         "login_frequency", "session_duration_avg", "pages_per_session",
         "cart_abandonment_rate", "wishlist_items", "total_purchases",
         "average_order_value", "days_since_last_purchase", "discount_usage_rate",
         "returns_rate", "email_open_rate", "customer_service_calls",
         "product_reviews_written", "social_media_engagement_score",
         "mobile_app_usage", "payment_method_diversity", "lifetime_value",
         "credit_balance", BOOL_TARGET, "signup_quarter"]
    )
    return df.select(*ordered).withColumn("silver_loaded_at", F.current_timestamp())


def dimension_dataframe(spark, dim: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spec = DIMENSIONS[dim]
    key, cols = spec["key"], spec["cols"]
    if "buckets" in spec:
        rows = [(i + 1, label, i + 1) for i, (label, _lo, _hi) in enumerate(spec["buckets"])]
        rows.append((len(rows) + 1, UNKNOWN, len(rows) + 1))
        return spark.createDataFrame(rows, [key, cols[0], "sort_order"])
    silver = read_table(spark, SILVER_TABLE)
    distinct = silver.select(*[F.coalesce(F.col(c), F.lit(UNKNOWN)).alias(c) for c in cols]).distinct()
    window = Window.orderBy(*cols)
    return distinct.withColumn(key, F.row_number().over(window)).select(key, *cols)


def fact_dataframe(spark):
    from pyspark.sql import functions as F

    fact = read_table(spark, SILVER_TABLE)
    for dim, spec in DIMENSIONS.items():
        dim_df = F.broadcast(read_table(spark, dim))
        key, cols = spec["key"], spec["cols"]
        if "buckets" in spec:
            label = bucket_label(F.col(spec["source"]), spec["buckets"])
            fact = fact.withColumn(cols[0], label)
            fact = fact.join(dim_df.select(key, cols[0]), on=cols[0], how="left")
        else:
            for c in cols:
                fact = fact.withColumn(c, F.coalesce(F.col(c), F.lit(UNKNOWN)))
            fact = fact.join(dim_df.select(key, *cols), on=cols, how="left")
    keys = [spec["key"] for spec in DIMENSIONS.values()]
    return fact.select("customer_id", *keys, BOOL_TARGET, *FACT_MEASURES).withColumn(
        "gold_loaded_at", F.current_timestamp()
    )


def churn_metrics():
    from pyspark.sql import functions as F

    churned = F.col(BOOL_TARGET).cast("int")
    return [
        F.count("*").alias("customers"),
        F.sum(churned).alias("churned_customers"),
        F.round(F.sum(churned) / F.count("*"), 4).alias("churn_rate"),
        F.round(F.avg("lifetime_value"), 2).alias("avg_lifetime_value"),
        F.round(F.avg("average_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("total_purchases"), 2).alias("avg_total_purchases"),
        F.round(F.avg("cart_abandonment_rate"), 2).alias("avg_cart_abandonment_rate"),
        F.round(F.avg("days_since_last_purchase"), 2).alias("avg_days_since_last_purchase"),
        F.round(F.avg("login_frequency"), 2).alias("avg_login_frequency"),
    ]


def mart_dataframe(spark, mart: str):
    from pyspark.sql import functions as F

    fact = read_table(spark, FACT_TABLE)

    def with_dim(df, dim):
        return df.join(F.broadcast(read_table(spark, dim)), on=DIMENSIONS[dim]["key"], how="left")

    if mart == "mart_churn_by_country":
        return with_dim(fact, "dim_location").groupBy("country").agg(*churn_metrics())
    if mart == "mart_churn_by_city":
        return with_dim(fact, "dim_location").groupBy("country", "city").agg(*churn_metrics())
    if mart == "mart_churn_by_demographics":
        df = with_dim(with_dim(fact, "dim_age_group"), "dim_gender")
        return (
            df.groupBy("age_group", "gender")
            .agg(F.min("sort_order").alias("age_group_order"), *churn_metrics())
            .orderBy("age_group_order", "gender")
        )
    if mart == "mart_churn_by_tenure_recency":
        df = with_dim(fact, "dim_membership_tenure").withColumnRenamed("sort_order", "tenure_order")
        df = with_dim(df, "dim_purchase_recency").withColumnRenamed("sort_order", "recency_order")
        return df.groupBy("membership_tenure", "purchase_recency").agg(
            F.min("tenure_order").alias("tenure_order"),
            F.min("recency_order").alias("recency_order"),
            *churn_metrics(),
        )
    if mart == "mart_churn_by_signup_quarter":
        return with_dim(fact, "dim_signup_quarter").groupBy("signup_quarter").agg(*churn_metrics())
    if mart == "mart_engagement_vs_churn":
        return fact.groupBy(BOOL_TARGET).agg(
            F.count("*").alias("customers"),
            F.round(F.avg("login_frequency"), 2).alias("avg_login_frequency"),
            F.round(F.avg("session_duration_avg"), 2).alias("avg_session_duration_min"),
            F.round(F.avg("pages_per_session"), 2).alias("avg_pages_per_session"),
            F.round(F.avg("mobile_app_usage"), 2).alias("avg_mobile_app_usage"),
            F.round(F.avg("email_open_rate"), 2).alias("avg_email_open_rate"),
            F.round(F.avg("customer_service_calls"), 2).alias("avg_customer_service_calls"),
            F.round(F.avg("product_reviews_written"), 2).alias("avg_product_reviews_written"),
            F.round(F.avg("social_media_engagement_score"), 2).alias("avg_social_engagement_score"),
            F.round(F.avg("wishlist_items"), 2).alias("avg_wishlist_items"),
        )
    raise ValueError(f"Mart không được hỗ trợ: {mart}")


MART_NAMES = [
    "mart_churn_by_country",
    "mart_churn_by_city",
    "mart_churn_by_demographics",
    "mart_churn_by_tenure_recency",
    "mart_churn_by_signup_quarter",
    "mart_engagement_vs_churn",
]
RECONCILED_MARTS = MART_NAMES[:5]


@task(task_id="check_source_file")
def check_source_file() -> str:
    path = Path(DATA_DIR) / SOURCE_FILE
    if not path.is_file():
        raise FileNotFoundError(f"Không thấy file nguồn: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"File nguồn rỗng: {path}")
    print(f"File nguồn: {path} ({path.stat().st_size:,} bytes)")
    return str(path)


@task(task_id="ingest_csv_to_bronze", retries=1)
def ingest_csv_to_bronze(source_path: str) -> dict:
    from pyspark.sql import functions as F

    run_statements([f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'])
    with spark_session("bronze_ingest") as spark:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .option("encoding", "UTF-8")
            .csv(f"file://{source_path}")
        )
        df = df.toDF(*[c.strip().lower() for c in df.columns])
        df = df.withColumn("_source_file", F.lit(Path(source_path).name))
        df = df.withColumn("_ingested_at", F.current_timestamp())
        df.cache()
        rows = df.count()
        write_table(df, BRONZE_TABLE)
        columns = df.columns
        df.unpersist()
    result = {"table": BRONZE_TABLE, "rows": rows, "columns": columns}
    print(f"Đã nhập {rows:,} dòng vào {BRONZE_TABLE}")
    return result


@task(task_id="validate_bronze", retries=0)
def validate_bronze(ingest_result: dict) -> dict:
    expected = ingest_result["rows"]
    actual = scalar(f"SELECT COUNT(*) FROM {T(BRONZE_TABLE)}")
    if actual == 0 or actual != expected:
        raise ValueError(f"{BRONZE_TABLE}: đã đọc {expected} dòng nhưng bảng có {actual}")
    required = TEXT_COLS + MONEY_COLS + DECIMAL_COLS + INT_COLS + [BOOL_SOURCE]
    missing = [c for c in required if c not in ingest_result["columns"]]
    if missing:
        raise ValueError(f"Bronze thiếu các cột: {missing}")
    print(f"Bronze OK: {actual:,} dòng")
    return {"rows": actual}


@task(task_id="transform_silver_customer_behavior", retries=1)
def transform_silver() -> dict:
    with spark_session("silver_transform") as spark:
        bronze = read_table(spark, BRONZE_TABLE)
        silver = silver_dataframe(bronze)
        write_table(silver, SILVER_TABLE)
    run_statements([f"ALTER TABLE {T(SILVER_TABLE)} ADD PRIMARY KEY (customer_id)"])
    bronze_rows = scalar(f"SELECT COUNT(*) FROM {T(BRONZE_TABLE)}")
    silver_rows = scalar(f"SELECT COUNT(*) FROM {T(SILVER_TABLE)}")
    stats = {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "removed_rows": bronze_rows - silver_rows,
    }
    print(f"Silver xong: {stats}")
    return stats


@task(task_id="validate_silver", retries=0)
def validate_silver() -> dict:
    measure_cols = MONEY_COLS + DECIMAL_COLS + INT_COLS
    select = [
        "COUNT(*) AS total_rows",
        "COUNT(DISTINCT customer_id) AS distinct_ids",
        "COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_ids",
    ]
    select += [f"COUNT(*) FILTER (WHERE {c} < 0) AS neg_{c}" for c in NON_NEGATIVE_COLS]
    select += [
        f"COUNT(*) FILTER (WHERE {c} < {lo} OR {c} > {hi}) AS out_{c}"
        for c, (lo, hi) in RANGE_RULES.items()
    ]
    select += [
        f"ROUND(100.0 * COUNT(*) FILTER (WHERE {c} IS NULL) / NULLIF(COUNT(*), 0), 2) AS nullpct_{c}"
        for c in measure_cols
    ]
    engine = get_engine()
    try:
        with engine.connect() as conn:
            row = dict(conn.execute(text(f"SELECT {', '.join(select)} FROM {T(SILVER_TABLE)}")).mappings().one())
            bronze_rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(BRONZE_TABLE)}")).scalar()
    finally:
        engine.dispose()

    errors = []
    if row["total_rows"] == 0:
        errors.append("Silver không có dòng nào")
    if row["total_rows"] > bronze_rows:
        errors.append(f"Silver ({row['total_rows']}) nhiều hơn Bronze ({bronze_rows})")
    if row["distinct_ids"] != row["total_rows"] or row["null_ids"] > 0:
        errors.append("customer_id bị trùng hoặc NULL")
    errors += [f"{c} còn {row['neg_' + c]} giá trị âm" for c in NON_NEGATIVE_COLS if row["neg_" + c] > 0]
    errors += [f"{c} còn {row['out_' + c]} giá trị ngoài khoảng cho phép" for c in RANGE_RULES if row["out_" + c] > 0]
    if errors:
        raise ValueError("Silver không đạt kiểm tra: " + "; ".join(errors))

    null_pct = {c: float(row["nullpct_" + c]) for c in measure_cols if row["nullpct_" + c]}
    print(f"Silver OK: {row['total_rows']:,} dòng. Tỉ lệ NULL (%): {null_pct}")
    return {"rows": row["total_rows"], "null_pct": null_pct}


@task(task_id="reset_gold_layer")
def reset_gold_layer() -> None:
    tables = [FACT_TABLE] + MART_NAMES + list(DIMENSIONS)
    run_statements([f"DROP TABLE IF EXISTS {T(t)} CASCADE" for t in tables])


@task(task_id="build_dimension", retries=1)
def build_dimension(dim: str) -> dict:
    key = DIMENSIONS[dim]["key"]
    with spark_session(f"build_{dim}") as spark:
        df = dimension_dataframe(spark, dim)
        rows = df.count()
        write_table(df, dim)
    run_statements([f"ALTER TABLE {T(dim)} ADD PRIMARY KEY ({key})"])
    print(f"{dim}: {rows} dòng")
    return {"table": dim, "rows": rows}


@task(task_id="build_fact_customer_behavior", retries=1)
def build_fact() -> dict:
    with spark_session("build_fact") as spark:
        df = fact_dataframe(spark)
        write_table(df, FACT_TABLE)
    statements = [f"ALTER TABLE {T(FACT_TABLE)} ADD PRIMARY KEY (customer_id)"]
    for dim, spec in DIMENSIONS.items():
        key = spec["key"]
        statements.append(
            f"ALTER TABLE {T(FACT_TABLE)} ADD FOREIGN KEY ({key}) REFERENCES {T(dim)} ({key})"
        )
        statements.append(f"CREATE INDEX ON {T(FACT_TABLE)} ({key})")
    run_statements(statements)
    rows = scalar(f"SELECT COUNT(*) FROM {T(FACT_TABLE)}")
    print(f"{FACT_TABLE}: {rows:,} dòng")
    return {"table": FACT_TABLE, "rows": rows}


@task(task_id="build_mart", retries=1)
def build_mart(mart: str) -> dict:
    with spark_session(f"build_{mart}") as spark:
        df = mart_dataframe(spark, mart)
        rows = df.count()
        write_table(df, mart)
    print(f"{mart}: {rows} dòng")
    return {"table": mart, "rows": rows}


@task(task_id="validate_gold", retries=0)
def validate_gold() -> dict:
    fact, silver = T(FACT_TABLE), T(SILVER_TABLE)
    null_keys = " OR ".join(f"{spec['key']} IS NULL" for spec in DIMENSIONS.values())
    checks = [
        ("số dòng fact = số dòng silver",
         f"SELECT (SELECT COUNT(*) FROM {fact}), (SELECT COUNT(*) FROM {silver})"),
        ("số khách hàng rời bỏ: fact = silver",
         f"SELECT (SELECT COUNT(*) FROM {fact} WHERE is_churned), "
         f"(SELECT COUNT(*) FROM {silver} WHERE is_churned)"),
        ("tổng lifetime_value: fact = silver",
         f"SELECT (SELECT COALESCE(SUM(lifetime_value), 0) FROM {fact}), "
         f"(SELECT COALESCE(SUM(lifetime_value), 0) FROM {silver})"),
        ("fact không có khóa dim bị NULL",
         f"SELECT COUNT(*), 0 FROM {fact} WHERE {null_keys}"),
    ]
    for mart in RECONCILED_MARTS:
        checks.append((
            f"{mart}: SUM(customers) = số dòng fact",
            f"SELECT (SELECT COALESCE(SUM(customers), 0) FROM {T(mart)}), (SELECT COUNT(*) FROM {fact})",
        ))
        checks.append((
            f"{mart}: churn_rate nằm trong [0, 1]",
            f"SELECT COUNT(*) FILTER (WHERE churn_rate < 0 OR churn_rate > 1), 0 FROM {T(mart)}",
        ))

    failed, passed = [], []
    engine = get_engine()
    try:
        with engine.connect() as conn:
            for name, sql in checks:
                actual, expected = conn.execute(text(sql)).one()
                if abs(Decimal(actual) - Decimal(expected)) > Decimal("0.005"):
                    failed.append(f"{name} (thực tế {actual}, kỳ vọng {expected})")
                else:
                    passed.append(name)
    finally:
        engine.dispose()
    if failed:
        raise ValueError("Gold không đạt đối soát: " + "; ".join(failed))
    print(f"Gold OK, {len(passed)} kiểm tra đạt")
    return {"passed": passed}


@task(task_id="pipeline_summary", retries=0)
def pipeline_summary() -> dict:
    tables = [BRONZE_TABLE, SILVER_TABLE, FACT_TABLE] + list(DIMENSIONS) + MART_NAMES
    counts = {t: scalar(f"SELECT COUNT(*) FROM {T(t)}") for t in tables}
    width = max(len(t) for t in counts)
    print("=" * (width + 16))
    for t, n in counts.items():
        print(f"{t.ljust(width)} | {n:>10,}")
    print("=" * (width + 16))
    return counts


default_args = {
    "owner": "data_engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id=DAG_ID,
    description="PySpark Medallion: Bronze (csv) -> Silver (làm sạch) -> Gold (star schema + mart)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=SPARK_MAX_PARALLEL_TASKS,
    dagrun_timeout=timedelta(hours=2),
    tags=["ecommerce", "churn", "pyspark", "medallion", "bronze", "silver", "gold", "postgres"],
)
def ecommerce_customer_churn_pipeline():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("bronze", tooltip="Nhập CSV thô bằng Spark", ui_color="#ecd5b8") as bronze:
        source = check_source_file()
        ingested = ingest_csv_to_bronze(source)
        validate_bronze(ingested)

    with TaskGroup("silver", tooltip="Làm sạch, ép kiểu, khử trùng lặp bằng Spark", ui_color="#dde1e6") as silver:
        transform_silver() >> validate_silver()

    with TaskGroup("gold", tooltip="Star schema và bảng tổng hợp bằng Spark", ui_color="#faeab0") as gold:
        reset = reset_gold_layer()
        with TaskGroup("dimensions", tooltip="Các bảng dim_*") as dims:
            for dim in DIMENSIONS:
                build_dimension.override(task_id=f"build_{dim}")(dim)
        fact = build_fact()
        with TaskGroup("marts", tooltip="Các bảng mart_*") as marts:
            for mart in MART_NAMES:
                build_mart.override(task_id=f"build_{mart}")(mart)
        chain(reset, dims, fact, marts, validate_gold())

    chain(start, bronze, silver, gold, pipeline_summary(), end)


dag_obj = ecommerce_customer_churn_pipeline()