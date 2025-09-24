import io
import math
import re
import sys
import subprocess
import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------
# App meta
# -----------------------------
st.set_page_config(page_title="In Transit Time Report Generator", page_icon="📦", layout="wide")
st.title("In Transit Time Report Generator")

# -----------------------------
# Dependency helper for Excel
# -----------------------------
def _ensure_pkg(pkg_name, spec=None) -> bool:
    try:
        __import__(pkg_name); return True
    except Exception:
        pass
    try:
        with st.spinner(f"Installing dependency: {pkg_name}…"):
            subprocess.check_call([sys.executable, "-m", "pip", "install", spec or pkg_name],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        __import__(pkg_name); return True
    except Exception:
        return False

def pick_xlsx_engine() -> str:
    if _ensure_pkg("xlsxwriter", "xlsxwriter>=3.2.0"): return "xlsxwriter"
    if _ensure_pkg("openpyxl",  "openpyxl>=3.1.5"):    return "openpyxl"
    return ""  # neither available

# -----------------------------
# Parsing helpers
# -----------------------------
def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Drop unnamed / blank columns and collapse whitespace in headers."""
    df = df.copy()
    keep = []
    for c in df.columns:
        s = str(c).strip()
        if s and not s.lower().startswith("unnamed"):
            keep.append(c)
    df = df.loc[:, keep]
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df

def is_missing_like(x):
    if pd.isna(x): return True
    if isinstance(x, str) and x.strip().lower() in {"", "na", "n/a", "null", "none"}: return True
    return False

def is_true_like(v):
    if isinstance(v, bool): return v is True
    if is_missing_like(v):  return False
    if isinstance(v, (int, float)): return int(v) == 1
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"true", "yes", "y", "1"}
    return False

def is_false_like(v):
    if isinstance(v, bool): return v is False
    if is_missing_like(v):  return False
    if isinstance(v, (int, float)): return int(v) == 0
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"false", "no", "n", "0"}
    return False

def parse_timestamp_utc(s):
    """Parse anything-like-a-timestamp to UTC; invalid -> NaT."""
    if pd.isna(s): return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def round_half_up_days(x):
    """Half-up rounding (3.5->4, 3.4->3)."""
    if pd.isna(x): return np.nan
    return math.floor(x + 0.5)

def split_city_state(text: str):
    """
    Preferred split: 'City - ST'. If not present, try final 2-letter state token.
    Returns (city, state) (blank strings if missing).
    """
    if is_missing_like(text): return "", ""
    s = str(text).strip()

    parts = re.split(r"\s*-\s*", s, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()

    m = re.match(r"^(.*?)[\s,]+([A-Z]{2})$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    return s, ""

# -----------------------------
# Loader (CSV or Excel)
# -----------------------------
def load_table(uploaded_file) -> pd.DataFrame:
    raw_bytes = uploaded_file.read()
    name = (uploaded_file.name or "").lower()

    # Excel by extension or zip signature (keep native dtypes so datetimes parse)
    if name.endswith((".xlsx", ".xls")) or raw_bytes[:2] == b"PK":
        try:
            _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
            return pd.read_excel(io.BytesIO(raw_bytes))
        except Exception:
            pass

    # CSV: read as text to avoid auto-coercions; we parse what we need
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"]:
        try:
            return pd.read_csv(
                io.BytesIO(raw_bytes),
                encoding=enc,
                sep=None,
                engine="python",
                on_bad_lines="skip",
                dtype=str,
                keep_default_na=False,
            )
        except Exception:
            continue

    # Last resort try Excel again
    _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
    return pd.read_excel(io.BytesIO(raw_bytes))

# -----------------------------
# Mode-specific maps
# -----------------------------
# FTL: EXACTLY your current working mapping (unchanged)
FTL_MAP = {
    "bol": "Bill of Lading",
    "tracked": "Tracked",
    "p_name": "Pickup Name",
    "p_city_state": "Pickup City State",
    "p_country": "Pickup Country",
    "d_name": "Final Destination Name",
    "d_city_state": "Final Destination City State",
    "d_country": "Final Destination Country",
    "start_ts": "Pickup Departure Milestone (UTC)",
    "end_ts":   "Final Destination Arrival Milestone (UTC)",
    # Summary definitions row headers (D1/E1 equivalents)
    "def_col_d": "Average of In-Transit Time",
    "def_col_e": "Time taken from Departure to Arrival",
    # Main table headers (row 7)
    "main_headers": [
        "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
        "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country", "Average of In-Transit Time"
    ],
}

# LTL: per your detailed spec
LTL_MAP = {
    # columns present in the user's raw (B..AG); A is serial number ignored
    "pro": "PRO number",
    "tracked": "Tracked",
    "p_name": "Pickup Name",
    "p_city_state": "Pickup City State",
    "p_region": "Pickup Region",
    "dest_name": "Destination Name",
    "d_city_state": "Dropoff City State",
    "d_region": "Dropoff Country Region",
    "start_ts": "Pickup Utc Timestamp Time",
    "end_ts":   "Delivered Utc Timestamp Time",
    # Summary definitions row headers
    "def_col_d": "Average of In-Transit Time",
    "def_col_e": "Time taken from Picked up to Delivered",
    # Main table headers (row 7)
    "main_headers": [
        "Pro Number", "Pickup Name", "Pickup City", "Pickup State", "Pickup Region",
        "Destination Name", "Dropoff City", "Dropoff State", "Dropoff Region", "Average of In-Transit Time"
    ],
}

# -----------------------------
# Core builders
# -----------------------------
def build_ftl_tables(df_raw: pd.DataFrame):
    df = normalize_headers(df_raw).copy()

    # Ensure needed columns exist (create blanks if missing)
    for col in FTL_MAP.values():
        if isinstance(col, str) and col not in df.columns:
            df[col] = np.nan

    # Convenience
    trk = df[FTL_MAP["tracked"]]
    dep = df[FTL_MAP["start_ts"]]
    arr = df[FTL_MAP["end_ts"]]

    # Parse timestamps
    dep_ts = dep.apply(parse_timestamp_utc)
    arr_ts = arr.apply(parse_timestamp_utc)

    # Delta & validity
    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    # Categories
    is_untracked     = trk.apply(is_false_like)
    is_tracked_true  = trk.apply(is_true_like)
    is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
    is_tracked_good  = (~is_untracked) & is_tracked_true &  valid_transit.fillna(False)

    # Counts/avg
    cnt_untracked = int(is_untracked.sum())
    cnt_missing   = int(is_missing.sum())
    cnt_tracked   = int(is_tracked_good.sum())
    grand_total   = cnt_untracked + cnt_missing + cnt_tracked

    tracked_days = in_transit_days.where(is_tracked_good)
    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    # Small table (rows 1–5)
    small = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_missing, cnt_untracked, grand_total],
        "": ["", "", "", ""],
        FTL_MAP["def_col_d"]: ["", "", "", avg_tracked],
        FTL_MAP["def_col_e"]: ["", "", "", ""],
    })

    # Main table (tracked only)
    rows = df[is_tracked_good].copy()

    p_city, p_state = ([], [])
    d_city, d_state = ([], [])
    if len(rows):
        p_city, p_state = zip(*rows[FTL_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[FTL_MAP["d_city_state"]].astype(str).map(split_city_state))

    main = pd.DataFrame({
        "Bill of Lading": rows[FTL_MAP["bol"]].astype(str).str.strip(),
        "Pickup Name": rows[FTL_MAP["p_name"]].astype(str).str.strip(),
        "Pickup City": list(p_city),
        "Pickup State": list(p_state),
        "Pickup Country": rows[FTL_MAP["p_country"]].astype(str).str.strip(),
        "Dropoff Name": rows[FTL_MAP["d_name"]].astype(str).str.strip(),
        "Dropoff City": list(d_city),
        "Dropoff State": list(d_state),
        "Dropoff Country": rows[FTL_MAP["d_country"]].astype(str).str.strip(),
        "Average of In-Transit Time": tracked_days[is_tracked_good].astype("Int64"),
    })

    # Grand Total avg row
    javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean()) if len(main)>0 else ""
    total_row = {col: "" for col in main.columns}
    total_row["Bill of Lading"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main

def build_ltl_tables(df_raw: pd.DataFrame):
    df = normalize_headers(df_raw).copy()

    # Ensure needed columns exist (create blanks if missing)
    needed_cols = [
        LTL_MAP["pro"], LTL_MAP["tracked"], LTL_MAP["p_name"], LTL_MAP["p_city_state"], LTL_MAP["p_region"],
        LTL_MAP["dest_name"], LTL_MAP["d_city_state"], LTL_MAP["d_region"], LTL_MAP["start_ts"], LTL_MAP["end_ts"]
    ]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = np.nan

    trk = df[LTL_MAP["tracked"]]
    dep = df[LTL_MAP["start_ts"]]
    arr = df[LTL_MAP["end_ts"]]

    dep_ts = dep.apply(parse_timestamp_utc)
    arr_ts = arr.apply(parse_timestamp_utc)

    # Delta and rounding
    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    # Categories per your rules
    is_untracked     = trk.apply(is_false_like)
    is_tracked_true  = trk.apply(is_true_like)

    # Missing if negative duration OR any timestamp missing/empty/zero-equivalent
    # (valid_transit False includes negative and zero; we need to treat those as missing WHEN tracked_true)
    is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
    is_tracked_good  = (~is_untracked) & is_tracked_true &  valid_transit.fillna(False)

    # Small summary counts & average
    cnt_untracked = int(is_untracked.sum())
    cnt_missing   = int(is_missing.sum())
    cnt_tracked   = int(is_tracked_good.sum())
    grand_total   = cnt_untracked + cnt_missing + cnt_tracked

    tracked_days = in_transit_days.where(is_tracked_good)
    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    small = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_missing, cnt_untracked, grand_total],
        "": ["", "", "", ""],
        LTL_MAP["def_col_d"]: ["", "", "", avg_tracked],
        LTL_MAP["def_col_e"]: ["", "", "", ""],
    })

    # Main table (tracked only)
    rows = df[is_tracked_good].copy()

    p_city, p_state = ([], [])
    d_city, d_state = ([], [])
    if len(rows):
        p_city, p_state = zip(*rows[LTL_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[LTL_MAP["d_city_state"]].astype(str).map(split_city_state))

    main = pd.DataFrame({
        "Pro Number": rows[LTL_MAP["pro"]].astype(str).str.strip(),
        "Pickup Name": rows[LTL_MAP["p_name"]].astype(str).str.strip(),
        "Pickup City": list(p_city),
        "Pickup State": list(p_state),
        "Pickup Region": rows[LTL_MAP["p_region"]].astype(str).str.strip(),
        "Destination Name": rows[LTL_MAP["dest_name"]].astype(str).str.strip(),
        "Dropoff City": list(d_city),
        "Dropoff State": list(d_state),
        "Dropoff Region": rows[LTL_MAP["d_region"]].astype(str).str.strip(),
        "Average of In-Transit Time": tracked_days[is_tracked_good].astype("Int64"),
    })

    # Grand Total avg row
    javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean()) if len(main)>0 else ""
    total_row = {col: "" for col in main.columns}
    total_row["Pro Number"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main

# -----------------------------
# Excel / CSV exporters
# -----------------------------
def build_summary_excel(small_df: pd.DataFrame, main_df: pd.DataFrame, mode_name: str) -> bytes | None:
    engine = pick_xlsx_engine()
    if not engine:
        return None
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine=engine) as writer:
        small_df.to_excel(writer, sheet_name="Summary", index=False, startrow=0)
        main_df.to_excel(writer, sheet_name="Summary", index=False, startrow=6)  # row 7
        pd.DataFrame({"Mode":[mode_name]}).to_excel(writer, sheet_name="Meta", index=False)
    out.seek(0)
    return out.getvalue()

def build_summary_single_csv(small_df: pd.DataFrame, main_df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    small_df.to_csv(buf, index=False)
    buf.write("\n")  # blank row 6
    main_df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

# -----------------------------
# UI
# -----------------------------
mode = st.selectbox(
    "Choose Product",
    options=["FTL", "LTL"],
    index=0,
    help="FTL uses your original mapping; LTL uses Pickup→Delivered timestamps and PRO/Region fields."
)

uploaded = st.file_uploader("Upload RAW file (CSV or Excel)", type=["csv", "xlsx", "xls"], accept_multiple_files=False)
st.caption(f"Selected: **{mode}**")

if uploaded:
    try:
        df_raw = load_table(uploaded)
        st.write(f"**Rows loaded:** {len(df_raw):,} | **Columns:** {len(df_raw.columns)}")
        if mode == "FTL":
            small_df, main_df = build_ftl_tables(df_raw)
        else:
            small_df, main_df = build_ltl_tables(df_raw)

        st.success("Summary built successfully.")

        with st.expander("Preview — Small table (rows 1–5)"):
            st.dataframe(small_df, use_container_width=True)
        with st.expander("Preview — Main table (row 7 onward)"):
            st.dataframe(main_df.head(50), use_container_width=True)

        # Single CSV (both tables)
        single_csv_blob = build_summary_single_csv(small_df, main_df)
        st.download_button(
            "⬇️ Download Summary (Single CSV)",
            data=single_csv_blob,
            file_name=f"Summary_{mode}.csv",
            mime="text/csv",
            use_container_width=True
        )

        # Excel (single-sheet "Summary")
        excel_blob = build_summary_excel(small_df, main_df, mode)
        if excel_blob is not None:
            st.download_button(
                "⬇️ Download Summary (Excel)",
                data=excel_blob,
                file_name=f"Summary_{mode}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.info("Excel engine unavailable; CSV export works. Add `openpyxl` or `xlsxwriter` in requirements to enable Excel.")

        st.caption(
            f"Counts — Tracked: {int(small_df.loc[0, 'Shipment Count'])}, "
            f"Missed: {int(small_df.loc[1, 'Shipment Count'])}, "
            f"Untracked: {int(small_df.loc[2, 'Shipment Count'])}, "
            f"Total: {int(small_df.loc[3, 'Shipment Count'])}"
        )

    except Exception as e:
        st.error(f"Could not process this file. Details: {e}")
else:
    st.info("Upload your CSV/XLSX to generate the Summary.")
