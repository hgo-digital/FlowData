#!/usr/bin/env python3
# tap_flow_pipeline.py
# Ingest 15-min TAP CSVs, aggregate/dedupe, maintain lookups, export enriched data.

import argparse
import hashlib
import ipaddress
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional

import duckdb
import polars as pl
from dateutil import tz

LOG = logging.getLogger("tap_pipeline")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
LOG.addHandler(handler)

# ---------- Config / Columns ----------

FLOW_COLUMNS = [
    "Source Address",
    "Destination Address",
    "Application",
    "Destination Port",
    "Total Volume",
    "Dest to Src Volume",
    "Src to Dest Volume",
]

AGG_COLS_RENAMED = {
    "Source Address": "src",
    "Destination Address": "dst",
    "Application": "application",
    "Destination Port": "dport",
    "Total Volume": "total_volume",
    "Dest to Src Volume": "dst_to_src",
    "Src to Dest Volume": "src_to_dst",
}

# ---------- Helpers ----------

FILENAME_RE = re.compile(
    r"^TAP_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2})_(\d{2}-\d{2})\.csv$", re.IGNORECASE
)

def parse_window_from_filename(filename: str) -> Tuple[str, datetime, datetime]:
    """
    Returns (display_str, start_ts_utc, end_ts_utc)
    display_str is 'YYYY-MM-DD_HH-MM_HH-MM'
    """
    base = os.path.basename(filename)
    m = FILENAME_RE.match(base)
    if not m:
        raise ValueError(f"Filename does not match pattern: {base}")
    date_str, start_hm, end_hm = m.groups()
    display = f"{date_str}_{start_hm}_{end_hm}"

    # Parse as UTC
    start = datetime.strptime(f"{date_str} {start_hm}", "%Y-%m-%d %H-%M").replace(tzinfo=timezone.utc)
    end = datetime.strptime(f"{date_str} {end_hm}", "%Y-%m-%d %H-%M").replace(tzinfo=timezone.utc)
    # Handle rollover across midnight
    if end <= start:
        end = end + timedelta(days=1)
    return display, start, end

def ipv4_to_int(ip: str) -> int:
    return int(ipaddress.ip_address(ip))

def md5_key(src: str, dst: str, dport: int) -> str:
    s = f"{src}|{dst}|{dport}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def ensure_schema(con: duckdb.DuckDBPyConnection):
    con.execute("""
        PRAGMA threads=4;
        CREATE TABLE IF NOT EXISTS ingested_files (
            filename TEXT PRIMARY KEY,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            display TEXT,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        );
        CREATE TABLE IF NOT EXISTS flows_agg (
            src TEXT,
            dst TEXT,
            application TEXT,
            dport INTEGER,
            total_volume BIGINT,
            dst_to_src BIGINT,
            src_to_dst BIGINT,
            last_seen TEXT,
            last_seen_ts TIMESTAMP,
            src_int BIGINT,
            dst_int BIGINT,
            unique_key TEXT,
            PRIMARY KEY (src, dst, application, dport)
        );
        CREATE TABLE IF NOT EXISTS ip_hostnames_raw (
            address TEXT,
            hostname TEXT
        );
        CREATE TABLE IF NOT EXISTS ip_hostnames_expanded (
            address TEXT,
            hostname TEXT,
            start_int BIGINT,
            end_int BIGINT,
            prefix_len INTEGER
        );
        CREATE TABLE IF NOT EXISTS dest_app_map (
            dest_address TEXT,
            dest_port INTEGER,
            app_name TEXT,
            PRIMARY KEY (dest_address, dest_port)
        );
    """)
    # View (recreate to pick up changes)
    con.execute("""
        CREATE OR REPLACE VIEW enriched_flows AS
        WITH src_candidates AS (
            SELECT
                f.unique_key,
                h.hostname,
                h.prefix_len,
                ROW_NUMBER() OVER (PARTITION BY f.unique_key ORDER BY h.prefix_len DESC) AS rn
            FROM flows_agg f
            JOIN ip_hostnames_expanded h
              ON f.src_int BETWEEN h.start_int AND h.end_int
        ),
        best_src AS (
            SELECT unique_key, hostname
            FROM src_candidates
            WHERE rn = 1
        ),
        dst_candidates AS (
            SELECT
                f.unique_key,
                h.hostname,
                h.prefix_len,
                ROW_NUMBER() OVER (PARTITION BY f.unique_key ORDER BY h.prefix_len DESC) AS rn
            FROM flows_agg f
            JOIN ip_hostnames_expanded h
              ON f.dst_int BETWEEN h.start_int AND h.end_int
        ),
        best_dst AS (
            SELECT unique_key, hostname
            FROM dst_candidates
            WHERE rn = 1
        )
        SELECT
            f.src AS "Source Address",
            f.dst AS "Destination Address",
            f.application AS "Application",
            f.dport AS "Destination Port",
            f.total_volume AS "Total Volume",
            f.dst_to_src AS "Dest to Src Volume",
            f.src_to_dst AS "Src to Dest Volume",
            bs.hostname AS "MatchedSourceHostname",
            bd.hostname AS "MatchedDestHostname",
            dam.app_name AS "DestinationAppNameFromAddressandPort",
            f.last_seen AS "LastSeen",
            f.unique_key AS "uniquekey"
        FROM flows_agg f
        LEFT JOIN best_src bs USING (unique_key)
        LEFT JOIN best_dst bd USING (unique_key)
        LEFT JOIN dest_app_map dam
          ON dam.dest_address = f.dst AND dam.dest_port = f.dport;
    """)

def list_new_files(folder: str, con: duckdb.DuckDBPyConnection):
    all_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(".csv") and f.upper().startswith("TAP_")
    ]
    # Parse and sort by start_ts
    parsed = []
    for fp in all_files:
        try:
            display, start_ts, end_ts = parse_window_from_filename(fp)
            parsed.append((fp, display, start_ts, end_ts))
        except Exception as e:
            LOG.warning(f"Skipping file (name mismatch): {fp} ({e})")
    if not parsed:
        return []

    # Filter out already ingested
    ingested = set(
        r[0] for r in con.execute("SELECT filename FROM ingested_files").fetchall()
    )
    new_items = [p for p in parsed if p[1] not in ingested]
    new_items.sort(key=lambda t: t[2])
    return new_items

def read_and_aggregate_csv(path: str, display: str, end_ts: datetime) -> Tuple[pl.DataFrame, int]:
    """
    Reads a single CSV, coerces types, drops bad rows, groups by 4-tuple and sums volumes.
    Returns (aggregated_df, bad_rows_count).
    """
    # Read as strings first to allow safe casts
    df = pl.read_csv(
        path,
        has_header=True,
        infer_schema_length=0,
        ignore_errors=False,
        null_values=None,
    )
    # Normalize expected columns exist
    missing = [c for c in FLOW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {os.path.basename(path)}: {missing}")

    # Cast numeric fields (non-strict casts: bad -> null; we'll drop null rows)
    casted = df.with_columns([
        pl.col("Destination Port").cast(pl.Int64, strict=False),
        pl.col("Total Volume").cast(pl.Int64, strict=False),
        pl.col("Dest to Src Volume").cast(pl.Int64, strict=False),
        pl.col("Src to Dest Volume").cast(pl.Int64, strict=False),
    ])

    # Drop rows with nulls or empty IPs/apps
    cleaned = casted.filter(
        pl.col("Source Address").is_not_null()
        & pl.col("Destination Address").is_not_null()
        & pl.col("Application").is_not_null()
        & pl.col("Destination Port").is_not_null()
        & pl.col("Total Volume").is_not_null()
        & pl.col("Dest to Src Volume").is_not_null()
        & pl.col("Src to Dest Volume").is_not_null()
        & (pl.col("Source Address").str.len_chars() > 0)
        & (pl.col("Destination Address").str.len_chars() > 0)
        & (pl.col("Application").str.len_chars() > 0)
    )

    bad_rows = df.height - cleaned.height
    if bad_rows:
        LOG.warning(f"{os.path.basename(path)}: skipped {bad_rows} malformed rows")

    # Aggregate by 4-tuple and sum the volumes
    agg = (
        cleaned
        .group_by(["Source Address","Destination Address","Application","Destination Port"])
        .agg([
            pl.col("Total Volume").sum().alias("Total Volume"),
            pl.col("Dest to Src Volume").sum().alias("Dest to Src Volume"),
            pl.col("Src to Dest Volume").sum().alias("Src to Dest Volume"),
        ])
    )

    # Compute ints + keys + last_seen
    def _ipv4_to_int_series(s: pl.Series) -> pl.Series:
        return s.map_elements(ipv4_to_int, return_dtype=pl.Int64)

    def _md5_series(src_s: pl.Series, dst_s: pl.Series, dport_s: pl.Series) -> pl.Series:
        return pl.Series(
            [md5_key(src_s[i], dst_s[i], int(dport_s[i])) for i in range(len(src_s))],
            dtype=pl.Utf8,
        )

    agg = agg.rename(AGG_COLS_RENAMED)
    agg = agg.with_columns([
        _ipv4_to_int_series(pl.col("src")).alias("src_int"),
        _ipv4_to_int_series(pl.col("dst")).alias("dst_int"),
    ])
    agg = agg.with_columns([
        pl.Series(_md5_series(agg["src"], agg["dst"], agg["dport"])).alias("unique_key"),
        pl.lit(display).alias("last_seen"),
        pl.lit(end_ts.replace(tzinfo=timezone.utc)).alias("last_seen_ts"),
    ])

    # Reorder columns for merge clarity
    agg = agg.select([
        "src","dst","application","dport",
        "total_volume","dst_to_src","src_to_dst",
        "last_seen","last_seen_ts","src_int","dst_int","unique_key"
    ])
    return agg, bad_rows

def merge_aggregate(con: duckdb.DuckDBPyConnection, df: pl.DataFrame):
    con.register("file_agg_tmp", df.to_arrow())
    con.execute("""
        MERGE INTO flows_agg t
        USING file_agg_tmp s
        ON t.src = s.src AND t.dst = s.dst AND t.application = s.application AND t.dport = s.dport
        WHEN MATCHED THEN UPDATE SET
            total_volume = t.total_volume + s.total_volume,
            dst_to_src   = t.dst_to_src + s.dst_to_src,
            src_to_dst   = t.src_to_dst + s.src_to_dst,
            last_seen_ts = CASE WHEN s.last_seen_ts > t.last_seen_ts THEN s.last_seen_ts ELSE t.last_seen_ts END,
            last_seen    = CASE WHEN s.last_seen_ts > t.last_seen_ts THEN s.last_seen    ELSE t.last_seen    END
        WHEN NOT MATCHED THEN INSERT (
            src, dst, application, dport,
            total_volume, dst_to_src, src_to_dst,
            last_seen, last_seen_ts, src_int, dst_int, unique_key
        )
        VALUES (
            s.src, s.dst, s.application, s.dport,
            s.total_volume, s.dst_to_src, s.src_to_dst,
            s.last_seen, s.last_seen_ts, s.src_int, s.dst_int, s.unique_key
        );
    """)
    con.unregister("file_agg_tmp")

def cmd_import_flows(args):
    db = args.db
    folder = args.folder
    if not os.path.isdir(folder):
        LOG.error(f"Folder not found: {folder}")
        sys.exit(1)
    con = duckdb.connect(db)
    ensure_schema(con)

    items = list_new_files(folder, con)
    if not items:
        LOG.info("No new files to ingest.")
        return

    LOG.info(f"Found {len(items)} new file(s). Importing in time order...")
    total_rows = 0
    total_bad = 0
    for fp, display, start_ts, end_ts in items:
        LOG.info(f"Ingesting {os.path.basename(fp)} ({display})")
        try:
            agg, bad_rows = read_and_aggregate_csv(fp, display, end_ts)
            total_bad += bad_rows
            merge_aggregate(con, agg)
            con.execute(
                "INSERT INTO ingested_files (filename, start_ts, end_ts, display) VALUES (?, ?, ?, ?)",
                [display, start_ts, end_ts, display]
            )
            total_rows += agg.height
        except Exception as e:
            LOG.error(f"Failed to ingest {fp}: {e}")
            continue

    LOG.info(f"Done. Aggregated rows merged: {total_rows}. Malformed rows skipped: {total_bad}.")

def expand_host_row(address: str) -> Tuple[int,int,int]:
    """
    Returns (start_int, end_int, prefix_len). For single IP, prefix_len=32.
    """
    address = address.strip()
    if "/" in address:
        net = ipaddress.ip_network(address, strict=False)
        return int(net.network_address), int(net.broadcast_address), int(net.prefixlen)
    else:
        ipi = int(ipaddress.ip_address(address))
        return ipi, ipi, 32

def cmd_load_hosts(args):
    db = args.db
    csv_path = args.hosts
    if not os.path.isfile(csv_path):
        LOG.error(f"Hosts CSV not found: {csv_path}")
        sys.exit(1)

    con = duckdb.connect(db)
    ensure_schema(con)

    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    # Accept flexible header casing
    cols = {c.lower().strip(): c for c in df.columns}
    addr_col = cols.get("address")
    host_col = cols.get("hostname")
    if not addr_col or not host_col:
        raise ValueError("Hosts CSV must have columns: Address, Hostname")

    # Build expanded rows
    addrs = df[addr_col].to_list()
    hosts = df[host_col].to_list()

    expanded = []
    for a, h in zip(addrs, hosts):
        if a is None or h is None or str(a).strip() == "" or str(h).strip() == "":
            continue
        try:
            s, e, p = expand_host_row(str(a))
            expanded.append((str(a).strip(), str(h).strip(), s, e, p))
        except Exception as ex:
            LOG.warning(f"Skipping bad host mapping '{a},{h}': {ex}")

    pl_exp = pl.DataFrame(expanded, schema=["address","hostname","start_int","end_int","prefix_len"])

    with con:
        con.execute("DELETE FROM ip_hostnames_raw;")
        con.execute("DELETE FROM ip_hostnames_expanded;")
        con.register("host_tmp", pl_exp.to_arrow())
        con.execute("""
            INSERT INTO ip_hostnames_expanded (address, hostname, start_int, end_int, prefix_len)
            SELECT address, hostname, start_int, end_int, prefix_len FROM host_tmp;
        """)
        con.unregister("host_tmp")
    LOG.info(f"Loaded {pl_exp.height} hostname mappings.")

def cmd_load_dest_map(args):
    db = args.db
    csv_path = args.dest_map
    if not os.path.isfile(csv_path):
        LOG.error(f"Dest map CSV not found: {csv_path}")
        sys.exit(1)

    con = duckdb.connect(db)
    ensure_schema(con)

    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    # Normalize headers to support the misspelling "Destintation Port"
    normalized = {c.lower().strip().replace("  ", " "): c for c in df.columns}
    addr_col = normalized.get("destination address")
    port_col = normalized.get("destination port") or normalized.get("destintation port")
    app_col  = normalized.get("appname")
    if not addr_col or not port_col or not app_col:
        raise ValueError("Dest map CSV must have columns: Destination Address, Destination Port (or Destintation Port), AppName")

    pdf = (
        df.rename({addr_col: "dest_address", port_col: "dest_port", app_col: "app_name"})
          .with_columns(pl.col("dest_port").cast(pl.Int64, strict=False))
          .filter(pl.col("dest_address").is_not_null() & pl.col("app_name").is_not_null() & pl.col("dest_port").is_not_null())
          .select(["dest_address","dest_port","app_name"])
    )

    with con:
        con.execute("DELETE FROM dest_app_map;")
        con.register("dam_tmp", pdf.to_arrow())
        con.execute("""
            INSERT INTO dest_app_map (dest_address, dest_port, app_name)
            SELECT dest_address, dest_port, app_name FROM dam_tmp;
        """)
        con.unregister("dam_tmp")
    LOG.info(f"Loaded {pdf.height} destination/port→AppName mappings.")

def cmd_export(args):
    db = args.db
    out = args.out
    fmt = args.format.lower()
    if fmt not in ("parquet","csv"):
        LOG.error("Format must be parquet or csv")
        sys.exit(1)
    con = duckdb.connect(db)
    ensure_schema(con)

    # Order by time then addresses (nice for diffing)
    base_query = """
        SELECT * FROM enriched_flows
        ORDER BY "LastSeen" ASC, "Destination Port" ASC, "Source Address", "Destination Address", "Application";
    """
    if fmt == "parquet":
        con.execute(f"COPY ({base_query}) TO ? (FORMAT 'parquet');", [out])
    else:
        con.execute(f"COPY ({base_query}) TO ? (FORMAT 'csv', HEADER, DELIMITER ',');", [out])

    LOG.info(f"Exported enriched data to {out}")

def main():
    parser = argparse.ArgumentParser(description="Ingest TAP CSVs → DuckDB → Enriched export")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_imp = sub.add_parser("import-flows", help="Import TAP_*.csv from a folder (idempotent)")
    p_imp.add_argument("--db", required=True, help="Path to DuckDB file (e.g., ./flows.duckdb)")
    p_imp.add_argument("--folder", required=True, help="Folder containing TAP CSVs")
    p_imp.set_defaults(func=cmd_import_flows)

    p_hosts = sub.add_parser("load-hosts", help="Load Address↔Hostname (supports IPs and CIDRs)")
    p_hosts.add_argument("--db", required=True, help="Path to DuckDB file")
    p_hosts.add_argument("--hosts", required=True, help="CSV with columns: Address, Hostname")
    p_hosts.set_defaults(func=cmd_load_hosts)

    p_dam = sub.add_parser("load-dest-map", help="Load Destination Address + Port → AppName")
    p_dam.add_argument("--db", required=True, help="Path to DuckDB file")
    p_dam.add_argument("--dest-map", required=True, help="CSV with columns: Destination Address, Destination Port|Destintation Port, AppName")
    p_dam.set_defaults(func=cmd_load_dest_map)

    p_export = sub.add_parser("export", help="Export enriched_flows to Parquet or CSV")
    p_export.add_argument("--db", required=True, help="Path to DuckDB file")
    p_export.add_argument("--out", required=True, help="Output path (e.g., ./enriched_flows.parquet)")
    p_export.add_argument("--format", default="parquet", help="parquet (default) or csv")
    p_export.set_defaults(func=cmd_export)

    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        LOG.error("Interrupted.")
        sys.exit(130)

if __name__ == "__main__":
    main()
