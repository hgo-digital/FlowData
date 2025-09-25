#!/usr/bin/env python3
# tap_flow_pipeline.py
#
# Ingest 15-minute TAP CSVs → DuckDB → export enriched flows.
#
# Quick start:
#   pip install duckdb polars python-dateutil
#
#   # 1) Load/refresh lookups (run whenever your lookup CSVs change)
#   python tap_flow_pipeline.py load-hosts --db ./flows.duckdb --hosts ./hosts.csv
#   python tap_flow_pipeline.py load-dest-map --db ./flows.duckdb --dest-map ./dest_app_map.csv
#
#   # 2) Import new TAP CSVs (idempotent; skips previously ingested file windows)
#   python tap_flow_pipeline.py import-flows --db ./flows.duckdb --folder /path/to/input
#
#   # 3) Export enriched data (CSV by default; use --format parquet for Parquet)
#   python tap_flow_pipeline.py export --db ./flows.duckdb --out ./enriched_flows.csv
#
# Notes:
# - Filenames are UTC and must match: TAP_YYYY-MM-DD_HH-MM_HH-MM.csv
# - Dedup key for aggregation is (src, dst, application, dport); volumes are summed.
# - Unique key is md5("src|dst|dport") (no Application), per your request.
# - LastSeen is the filename token "YYYY-MM-DD_HH-MM_HH-MM" of the most recent window seen.
# - Lookups: Address↔Hostname supports exact IPv4 and CIDR, picking the most specific match.
# - Unmatched lookups remain NULL/blank.

import argparse
import hashlib
import ipaddress
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple, List

import duckdb
import polars as pl

# ---------------- Logging ----------------

LOG = logging.getLogger("tap_pipeline")
LOG.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
LOG.addHandler(_handler)

# ---------------- Constants ----------------

FLOW_COLUMNS = [
    "Source Address",
    "Destination Address",
    "Application",
    "Destination Port",
    "Total Volume",
    "Dest to Src Volume",
    "Src to Dest Volume",
]

RENAME_FLOW_COLS = {
    "Source Address": "src",
    "Destination Address": "dst",
    "Application": "application",
    "Destination Port": "dport",
    "Total Volume": "total_volume",
    "Dest to Src Volume": "dst_to_src",
    "Src to Dest Volume": "src_to_dst",
}

FILENAME_RE = re.compile(
    r"^TAP_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2})_(\d{2}-\d{2})\.csv$", re.IGNORECASE
)

# ---------------- Helpers ----------------

def parse_window_from_filename(path: str) -> Tuple[str, datetime, datetime, str]:
    """
    Parse TAP filename to (display_token, start_ts_utc, end_ts_utc, basename).
    display_token = 'YYYY-MM-DD_HH-MM_HH-MM'
    """
    base = os.path.basename(path)
    m = FILENAME_RE.match(base)
    if not m:
        raise ValueError(f"Filename does not match pattern: {base}")
    date_str, start_hm, end_hm = m.groups()
    display_token = f"{date_str}_{start_hm}_{end_hm}"
    # UTC start/end
    start = datetime.strptime(f"{date_str} {start_hm}", "%Y-%m-%d %H-%M").replace(tzinfo=timezone.utc)
    end = datetime.strptime(f"{date_str} {end_hm}", "%Y-%m-%d %H-%M").replace(tzinfo=timezone.utc)
    if end <= start:
        end += timedelta(days=1)
    return display_token, start, end, base

def ipv4_to_int(ip: str) -> int:
    """Convert IPv4 address to 64-bit integer; raises on invalid/IPv6."""
    ip_obj = ipaddress.ip_address(ip)
    if ip_obj.version != 4:
        raise ValueError("Only IPv4 is supported")
    return int(ip_obj)

def md5_key(src: str, dst: str, dport: int) -> str:
    return hashlib.md5(f"{src}|{dst}|{dport}".encode("utf-8")).hexdigest()

# ---------------- Schema ----------------

def ensure_schema(con: duckdb.DuckDBPyConnection):
    con.execute("""
        PRAGMA threads=4;

        CREATE TABLE IF NOT EXISTS ingested_files (
            file_token TEXT PRIMARY KEY,   -- e.g., 2025-09-25_00-00_00-15
            basename   TEXT,               -- e.g., TAP_2025-09-25_00-00_00-15.csv
            start_ts   TIMESTAMP,
            end_ts     TIMESTAMP,
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
            last_seen TEXT,        -- display token
            last_seen_ts TIMESTAMP,
            src_int BIGINT,
            dst_int BIGINT,
            unique_key TEXT,       -- md5(src|dst|dport)
            PRIMARY KEY (src, dst, application, dport)
        );

        CREATE TABLE IF NOT EXISTS ip_hostnames_expanded (
            address   TEXT,
            hostname  TEXT,
            start_int BIGINT,
            end_int   BIGINT,
            prefix_len INTEGER
        );

        CREATE TABLE IF NOT EXISTS dest_app_map (
            dest_address TEXT,
            dest_port    INTEGER,
            app_name     TEXT,
            PRIMARY KEY (dest_address, dest_port)
        );
    """)

    # Recreate the enriched view every time to reflect latest lookups.
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
            SELECT unique_key, hostname FROM src_candidates WHERE rn = 1
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
            SELECT unique_key, hostname FROM dst_candidates WHERE rn = 1
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
          ON dam.dest_address = f.dst AND dam.dest_port = f.dport
    """)

# ---------------- File discovery ----------------

def discover_new_files(folder: str, con: duckdb.DuckDBPyConnection) -> List[Tuple[str,str,datetime,datetime,str]]:
    """
    Return a list of (path, token, start_ts, end_ts, basename) for files not yet ingested,
    sorted by start_ts (UTC).
    """
    all_files = []
    for name in os.listdir(folder):
        if not name.lower().endswith(".csv"):
            continue
        if not name.upper().startswith("TAP_"):
            continue
        all_files.append(os.path.join(folder, name))

    parsed = []
    for fp in all_files:
        try:
            token, start_ts, end_ts, base = parse_window_from_filename(fp)
            parsed.append((fp, token, start_ts, end_ts, base))
        except Exception as e:
            LOG.warning(f"Skipping non-matching file: {os.path.basename(fp)} ({e})")

    if not parsed:
        return []

    ingested = {r[0] for r in con.execute("SELECT file_token FROM ingested_files").fetchall()}
    new_items = [p for p in parsed if p[1] not in ingested]
    new_items.sort(key=lambda t: t[2])  # by start_ts
    return new_items

# ---------------- Per-file ingest ----------------

def read_and_aggregate_csv(path: str, token: str, end_ts_utc: datetime) -> pl.DataFrame:
    """
    Read one TAP CSV, coerce types, drop bad rows, aggregate by (src,dst,application,dport),
    sum volumes, add keys/ints/last_seen.
    """
    df = pl.read_csv(path, has_header=True, infer_schema_length=0)

    # Validate columns
    missing = [c for c in FLOW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {os.path.basename(path)}: {missing}")

    # Cast numeric columns
    df = df.with_columns([
        pl.col("Destination Port").cast(pl.Int64, strict=False),
        pl.col("Total Volume").cast(pl.Int64, strict=False),
        pl.col("Dest to Src Volume").cast(pl.Int64, strict=False),
        pl.col("Src to Dest Volume").cast(pl.Int64, strict=False),
    ])

    # Drop rows with null/empty essentials
    df = df.filter(
        pl.all_horizontal(
            pl.col("Source Address").is_not_null() & (pl.col("Source Address").str.len_chars() > 0),
            pl.col("Destination Address").is_not_null() & (pl.col("Destination Address").str.len_chars() > 0),
            pl.col("Application").is_not_null() & (pl.col("Application").str.len_chars() > 0),
            pl.col("Destination Port").is_not_null(),
            pl.col("Total Volume").is_not_null(),
            pl.col("Dest to Src Volume").is_not_null(),
            pl.col("Src to Dest Volume").is_not_null(),
        )
    )

    # Aggregate within-file
    agg = (
        df.group_by(["Source Address","Destination Address","Application","Destination Port"])
          .agg([
              pl.col("Total Volume").sum().alias("Total Volume"),
              pl.col("Dest to Src Volume").sum().alias("Dest to Src Volume"),
              pl.col("Src to Dest Volume").sum().alias("Src to Dest Volume"),
          ])
          .rename(RENAME_FLOW_COLS)
    )

    # IPv4 ints (skip any that fail conversion)
    def safe_ipv4_to_int(val: str) -> int | None:
        try:
            return ipv4_to_int(val)
        except Exception:
            return None

    agg = agg.with_columns([
        pl.col("src").map_elements(safe_ipv4_to_int, return_dtype=pl.Int64).alias("src_int"),
        pl.col("dst").map_elements(safe_ipv4_to_int, return_dtype=pl.Int64).alias("dst_int"),
    ]).filter(
        pl.col("src_int").is_not_null() & pl.col("dst_int").is_not_null()
    )

    # Unique key (md5 of src|dst|dport) — no Application in the hash
    agg = agg.with_columns([
        pl.struct(["src","dst","dport"]).map_elements(
            lambda r: md5_key(r["src"], r["dst"], int(r["dport"]))
        ).alias("unique_key"),
        pl.lit(token).alias("last_seen"),
        pl.lit(end_ts_utc).alias("last_seen_ts"),
    ])

    # Final column order
    agg = agg.select([
        "src","dst","application","dport",
        "total_volume","dst_to_src","src_to_dst",
        "last_seen","last_seen_ts","src_int","dst_int","unique_key"
    ])
    return agg

def merge_into_flows(con: duckdb.DuckDBPyConnection, df: pl.DataFrame):
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

# ---------------- Commands ----------------

def cmd_import_flows(args):
    con = duckdb.connect(args.db)
    ensure_schema(con)

    folder = args.folder
    if not os.path.isdir(folder):
        LOG.error(f"Folder not found: {folder}")
        sys.exit(1)

    items = discover_new_files(folder, con)
    if not items:
        LOG.info("No new files to ingest.")
        return

    LOG.info(f"Found {len(items)} new file(s). Importing in time order...")
    total_rows = 0
    for path, token, start_ts, end_ts, base in items:
        LOG.info(f"Ingesting {base} ({token})")
        try:
            agg = read_and_aggregate_csv(path, token, end_ts)
            merge_into_flows(con, agg)
            con.execute(
                "INSERT INTO ingested_files (file_token, basename, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                [token, base, start_ts, end_ts]
            )
            total_rows += agg.height
        except Exception as e:
            LOG.error(f"Failed to ingest {base}: {e}")

    LOG.info(f"Done. Aggregated rows merged: {total_rows}.")

def expand_host_row(address: str) -> tuple[int,int,int]:
    """
    Expand an IP or CIDR to (start_int, end_int, prefix_len). Only IPv4 supported.
    """
    address = address.strip()
    if "/" in address:
        net = ipaddress.ip_network(address, strict=False)
        if net.version != 4:
            raise ValueError("Only IPv4 CIDRs supported")
        return int(net.network_address), int(net.broadcast_address), int(net.prefixlen)
    else:
        ip_obj = ipaddress.ip_address(address)
        if ip_obj.version != 4:
            raise ValueError("Only IPv4 addresses supported")
        ipi = int(ip_obj)
        return ipi, ipi, 32

def cmd_load_hosts(args):
    con = duckdb.connect(args.db)
    ensure_schema(con)

    csv_path = args.hosts
    if not os.path.isfile(csv_path):
        LOG.error(f"Hosts CSV not found: {csv_path}")
        sys.exit(1)

    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    cols = {c.lower().strip(): c for c in df.columns}
    addr_col = cols.get("address")
    host_col = cols.get("hostname")
    if not addr_col or not host_col:
        raise ValueError("Hosts CSV must have columns: Address, Hostname")

    expanded_rows = []
    for address, hostname in zip(df[addr_col], df[host_col]):
        if address is None or hostname is None:
            continue
        a = str(address).strip()
        h = str(hostname).strip()
        if not a or not h:
            continue
        try:
            s, e, p = expand_host_row(a)
            expanded_rows.append((a, h, s, e, p))
        except Exception as ex:
            LOG.warning(f"Skipping bad host mapping '{a},{h}': {ex}")

    pl_exp = pl.DataFrame(
        expanded_rows,
        schema=["address","hostname","start_int","end_int","prefix_len"],
        orient="row"
    )

    with con:
        con.execute("DELETE FROM ip_hostnames_expanded;")
        con.register("host_tmp", pl_exp.to_arrow())
        con.execute("""
            INSERT INTO ip_hostnames_expanded (address, hostname, start_int, end_int, prefix_len)
            SELECT address, hostname, start_int, end_int, prefix_len
            FROM host_tmp
        """)
        con.unregister("host_tmp")

    LOG.info(f"Loaded {pl_exp.height} hostname mappings.")

def cmd_load_dest_map(args):
    con = duckdb.connect(args.db)
    ensure_schema(con)

    csv_path = args.dest_map
    if not os.path.isfile(csv_path):
        LOG.error(f"Dest map CSV not found: {csv_path}")
        sys.exit(1)

    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    normalized = {c.lower().strip(): c for c in df.columns}
    addr_col = normalized.get("destination address")
    port_col = normalized.get("destination port") or normalized.get("destintation port")  # tolerate typo
    app_col  = normalized.get("appname")
    if not addr_col or not port_col or not app_col:
        raise ValueError("Dest map CSV needs: Destination Address, Destination Port (or Destintation Port), AppName")

    pdf = (
        df.rename({addr_col: "dest_address", port_col: "dest_port", app_col: "app_name"})
          .with_columns(pl.col("dest_port").cast(pl.Int64, strict=False))
          .filter(
              pl.col("dest_address").is_not_null()
              & pl.col("app_name").is_not_null()
              & pl.col("dest_port").is_not_null()
          )
          .select(["dest_address","dest_port","app_name"])
    )

    with con:
        con.execute("DELETE FROM dest_app_map;")
        con.register("dam_tmp", pdf.to_arrow())
        con.execute("""
            INSERT INTO dest_app_map (dest_address, dest_port, app_name)
            SELECT dest_address, dest_port, app_name FROM dam_tmp
        """)
        con.unregister("dam_tmp")

    LOG.info(f"Loaded {pdf.height} destination/port→AppName mappings.")

def cmd_export(args):
    con = duckdb.connect(args.db)
    ensure_schema(con)

    base_query = """
        SELECT * FROM enriched_flows
        ORDER BY "LastSeen" ASC, "Destination Port" ASC,
                 "Source Address", "Destination Address", "Application"
    """

    out = args.out
    fmt = args.format.lower()
    if fmt == "parquet":
        con.execute(f"COPY ({base_query}) TO ? (FORMAT 'parquet');", [out])
    elif fmt == "csv":
        con.execute(f"COPY ({base_query}) TO ? (FORMAT 'csv', HEADER, DELIMITER ',');", [out])
    else:
        LOG.error("Unsupported format. Use 'csv' or 'parquet'.")
        sys.exit(1)

    LOG.info(f"Exported enriched data to {out}")

# ---------------- Main ----------------

def main():
    parser = argparse.ArgumentParser(description="Ingest TAP CSVs → DuckDB → Enriched export (CSV/Parquet)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_imp = sub.add_parser("import-flows", help="Import TAP_*.csv from a folder (idempotent)")
    p_imp.add_argument("--db", required=True, help="Path to DuckDB file (e.g., ./flows.duckdb)")
    p_imp.add_argument("--folder", required=True, help="Folder containing TAP CSVs")
    p_imp.set_defaults(func=cmd_import_flows)

    p_hosts = sub.add_parser("load-hosts", help="Load Address↔Hostname (IPv4 IPs and CIDRs)")
    p_hosts.add_argument("--db", required=True, help="Path to DuckDB file")
    p_hosts.add_argument("--hosts", required=True, help="CSV: Address,Hostname")
    p_hosts.set_defaults(func=cmd_load_hosts)

    p_dam = sub.add_parser("load-dest-map", help="Load Destination Address + Port → AppName")
    p_dam.add_argument("--db", required=True, help="Path to DuckDB file")
    p_dam.add_argument("--dest-map", required=True, help="CSV: Destination Address,Destination Port,AppName")
    p_dam.set_defaults(func=cmd_load_dest_map)

    p_export = sub.add_parser("export", help="Export enriched_flows (CSV default; Parquet optional)")
    p_export.add_argument("--db", required=True, help="Path to DuckDB file")
    p_export.add_argument("--out", required=True, help="Output path (e.g., ./enriched_flows.csv)")
    p_export.add_argument("--format", default="csv", help="csv (default) or parquet")
    p_export.set_defaults(func=cmd_export)

    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        LOG.error("Interrupted.")
        sys.exit(130)

if __name__ == "__main__":
    main()