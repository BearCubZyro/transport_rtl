import os
import json

import pandas as pd
from sqlalchemy import create_engine
import requests
import logging
import smtplib
import ssl
from email.message import EmailMessage
import subprocess


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Use SQLite for easy local testing. To switch to MySQL/PostgreSQL, change this URL.
DB_URL = os.getenv("TRANSPORT_DB_URL", f"sqlite:///{os.path.join(BASE_DIR, 'data_pipeline.db')}")


logging.basicConfig(
    level=os.getenv("ETL_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "etl.log")),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


def load_public_transport_csv(path: str) -> pd.DataFrame:
    csv_url = os.getenv("TRANSPORT_CSV_URL")
    if csv_url:
        logger.info("Fetching public transport CSV from URL")
        resp = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
    else:
        df = pd.read_csv(path)
    required_columns = {"route_id", "city", "bus_stop", "ridership", "timing"}
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"Public transport CSV missing required columns: {missing_cols}")

    # Normalize column names for downstream processing
    df = df.rename(columns={"timing": "timestamp"})
    return df


def load_traffic_data(path: str) -> pd.DataFrame:
    api_url = os.getenv("TRAFFIC_API_URL")
    if api_url:
        resp = requests.get(api_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    df = pd.DataFrame(data)
    required_columns = {"route_id", "avg_speed", "congestion_index", "timestamp"}
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(f"Traffic JSON/API data missing required fields: {missing_cols}")
    return df


def validate_and_clean(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    # Drop rows with missing critical fields
    df = df.dropna(subset=[time_col])

    # Parse timestamps and drop invalid
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])

    # Basic type coercions
    if "ridership" in df.columns:
        df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce")
    if "avg_speed" in df.columns:
        df["avg_speed"] = pd.to_numeric(df["avg_speed"], errors="coerce")
    if "congestion_index" in df.columns:
        df["congestion_index"] = pd.to_numeric(df["congestion_index"], errors="coerce")

    # Drop rows where numeric columns could not be parsed
    numeric_cols = [c for c in ["ridership", "avg_speed", "congestion_index"] if c in df.columns]
    for col in numeric_cols:
        df = df.dropna(subset=[col])

    return df


def build_unified_table(pub_df: pd.DataFrame, traf_df: pd.DataFrame) -> pd.DataFrame:
    # Round timestamps to the nearest hour to align readings (simple assumption)
    pub_df["timestamp_hour"] = pub_df["timestamp"].dt.floor("H")
    traf_df["timestamp_hour"] = traf_df["timestamp"].dt.floor("H")

    merged = pd.merge(
        pub_df,
        traf_df,
        on=["route_id", "timestamp_hour"],
        how="inner",
        suffixes=("_public", "_traffic"),
    )

    # Rename timestamp for clarity
    merged = merged.rename(columns={"timestamp_hour": "event_hour"})
    return merged


def load_to_database(df: pd.DataFrame, table_name: str = "transport_traffic") -> None:
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def create_summary_report(df: pd.DataFrame, out_path: str) -> None:
    summary = (
        df.groupby(["route_id", "city_public", "bus_stop"])[["ridership", "congestion_index"]]
        .mean()
        .reset_index()
        .rename(
            columns={
                "ridership": "avg_ridership",
                "congestion_index": "avg_congestion_index",
            }
        )
    )
    summary.to_csv(out_path, index=False)


def maybe_git_pull() -> None:
    if os.getenv("DATA_GIT_PULL", "false").lower() == "true":
        try:
            logger.info("Running git pull for latest data and code")
            subprocess.run(["git", "pull", "--rebase"], cwd=BASE_DIR, check=False)
        except Exception as e:
            logger.warning(f"git pull failed: {e}")


def send_failure_email(subject: str, body: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    to_addr = os.getenv("ALERT_EMAIL_TO")
    from_addr = os.getenv("ALERT_EMAIL_FROM", smtp_user or "")
    if not (smtp_host and smtp_user and smtp_pass and to_addr and from_addr):
        logger.info("Email not sent; SMTP or recipient env vars missing")
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info("Failure email sent")
    except Exception as e:
        logger.warning(f"Failed to send email: {e}")


def run_pipeline() -> None:
    logger.info("Starting ETL pipeline")

    maybe_git_pull()

    public_csv = os.path.join(RAW_DIR, "transport.csv")
    traffic_json = os.path.join(RAW_DIR, "traffic.json")

    logger.info(f"Loading public transport data from {public_csv}")
    pub_df = load_public_transport_csv(public_csv)
    logger.info(f"Loaded {len(pub_df)} public transport records")

    logger.info(f"Loading traffic sensor data from {traffic_json} or TRAFFIC_API_URL")
    traf_df = load_traffic_data(traffic_json)
    logger.info(f"Loaded {len(traf_df)} traffic sensor records")

    logger.info("Validating and cleaning datasets")
    pub_df = validate_and_clean(pub_df, "timestamp")
    traf_df = validate_and_clean(traf_df, "timestamp")

    processed_csv = os.path.join(PROCESSED_DIR, "cleaned_unified_data.csv")

    logger.info("Building unified table")
    unified_df = build_unified_table(pub_df, traf_df)
    logger.info(f"Unified table has {len(unified_df)} rows")

    logger.info("Saving cleaned unified dataset to CSV")
    unified_df.to_csv(processed_csv, index=False)

    logger.info("Loading unified dataset into relational database")
    load_to_database(unified_df)
    logger.info(f"Data loaded into database at {DB_URL}")

    summary_csv = os.path.join(REPORTS_DIR, "summary_by_route.csv")
    logger.info("Creating summary report (avg ridership & congestion per route)")
    create_summary_report(unified_df, summary_csv)
    logger.info(f"Summary report written to {summary_csv}")

    logger.info("ETL pipeline completed successfully")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.exception("ETL pipeline failed")
        try:
            with open(os.path.join(LOGS_DIR, "etl.log"), "r", encoding="utf-8", errors="ignore") as f:
                tail = f.read()[-10000:]
        except Exception:
            tail = str(e)
        send_failure_email(
            subject="ETL Pipeline Failure",
            body=f"An error occurred during ETL.\n\nError: {e}\n\nLast log tail:\n{tail}",
        )
        raise
