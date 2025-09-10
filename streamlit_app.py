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
st.set_page_config(page_title="Entegris Reports", page_icon="📦", layout="wide")
st.title("Entegris Reports — Summary Builder (Tracked / Missed / Untracked)")

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
    if pd.isna(s): return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def round_half_up_days(x):
    if pd.isna(x): return np.nan
    return math.floor(x + 0.5)  # 3.5->4, 3.4->3

def split_city_state(text: str):
    """
    Preferred: split on first '-' (city - state).
    Else try last 2-letter uppercase token as state.
    Returns (city, state). If not present, leaves city with full string and state blank.
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
# Mode configs (columns & E1 text)
# -----------------------------
MODE_CONFIG = {
    "FTL": {
        "columns": {
            "bol": "Bill of Lading",
            "tracked": "Tracked",
            "p_name": "Pickup Name",
            "p_city_state": "Pickup City State",
            "p_country": "Pickup Country",
            "d_name": "Final Destination Name",
            "d_city_state": "Final Destination City State",
            "d_country": "Final Destination Country",
            "start_ts": "Pickup Departure Milestone (UTC)",              # AA
            "end_ts":   "Final Destination Arrival Milestone (UTC)",     # AB
        },
        "e1_text": "Time taken from Departure to Arrival",
    },
    "LTL": {
        "columns": {
            "bol": "Bill of Lading",
            "tracked": "Tracked",
            "p_name": "Pickup Name",
            "p_city_state": "Pickup City State",     # if missing in your LTL dump, will be blank
            "p_country": "Pickup Country",
            "d_name": "Destination Name",
            "d_city_state": "Dropoff City State",    # if missing, will be blank
            "d_country": "Dropoff Country",
            "start_ts": "Pickup Utc Timestamp Time",
            "end_ts":   "Delivered Utc Timestamp Time",
        },
        "e1_text": "Time taken from Pickup to Delivered",
    },
    "Parcel": {
        "columns": {
            "bol": "Bill of Lading",
            "tracked": "Tracked",
            "p_name": "Pickup Name",
            "p_city_state": "Pickup City State",        # Parcel dump doesn't include this; will be blank
            "p_country": "Pickup Country",
            "d_name": "Destination Name",
            "d_city_state": "Dropoff City State",       # Parcel dump doesn't include this; will be blank
            "d_country": "Dropoff Country",
            "start_ts": "Departed Utc Timestamp Time",
            "end_ts":   "Delivered Utc Timestamp Time",
        },
        "e1_text": "Time taken from Departed to Delivered",
    },
    # Placeholders (reuse FTL mapping until you share)
    "Ocean": {"columns": None, "e1_text": "Time taken from Departure to Arrival"},
    "Air":   {"columns": None, "e1_text": "Time taken from Departure to Arrival"},
}

def columns_for_mode(mode: str):
    cfg = MODE_CONFIG.get(mode, MODE_CONFIG["FTL"]).copy()
    if cfg["columns"] is None:
        cfg["columns"] = MODE_CONFIG["FTL"]["columns"]
    return cfg

# -----------------------------
# Loader (CSV or Excel)
# -----------------------------
def load_table(uploaded_file) -> pd.DataFrame:
    raw_bytes = uploaded_file.read()
    name = (uploaded_file.name or "").lower()

    # Excel by extension or zip signature
    if name.endswith((".xlsx", ".xls")) or raw_bytes[:2] == b"PK":
        try:
            _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
            return pd.read_excel(io.BytesIO(raw_bytes))
        except Exception:
            pass

    # CSV: read as text to avoid weird coercions
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
# Core builder: Summary only
# -----------------------------
def build_summary_tables(df_raw: pd.DataFrame, mode: str):
    df = normalize_headers(df_raw).copy()
    cfg = columns_for_mode(mode)
    C = cfg["columns"]

    # Ensure needed columns exist
    required = [C["bol"], C["tracked"], C["p_name"], C["p_city_state"], C["p_country"],
                C["d_name"], C["d_city_state"], C["d_country"], C["start_ts"], C["end_ts"]]
    for col in required:
        if col not in df.columns:
            df[col] = np.nan

    # Convenience Series
    trk   = df[C["tracked"]]
    dep   = df[C["start_ts"]]
    arr   = df[C["end_ts"]]

    # Parse timestamps
    dep_ts = dep.apply(parse_timestamp_utc)
    arr_ts = arr.apply(parse_timestamp_utc)

    # Compute transit days (raw float) & rounded days
    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    # Categories (mutually exclusive)
    is_untracked     = trk.apply(is_false_like)
    is_tracked_true  = trk.apply(is_true_like)
    is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
    is_tracked_good  = (~is_untracked) & is_tracked_true & valid_transit.fillna(False)

    # Small summary counts & average (Tracked group only)
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
        "": ["", "", "", ""],  # blank col (row 1 col C)
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        cfg["e1_text"]: ["", "", "", ""],  # customized E1 header
    })

    # Main table (only rows with numeric in-transit days = tracked_good)
    rows = df[is_tracked_good].copy()

    def as_str(s): return rows[s].astype(str) if s in rows.columns else pd.Series([""] * len(rows))
    p_city, p_state = [], []
    d_city, d_state = [], []
    if len(rows):
        p_city, p_state = zip(*as_str(C["p_city_state"]).map(split_city_state)) if C["p_city_state"] in rows.columns else ([], [])
        d_city, d_state = zip(*as_str(C["d_city_state"]).map(split_city_state)) if C["d_city_state"] in rows.columns else ([], [])

    main = pd.DataFrame({
        "Bill of Lading": as_str(C["bol"]).str.strip(),
        "Pickup Name": as_str(C["p_name"]).str.strip(),
        "Pickup City": list(p_city) if p_city else [""] * len(rows),
        "Pickup State": list(p_state) if p_state else [""] * len(rows),
        "Pickup Country": as_str(C["p_country"]).str.strip(),
        "Dropoff Name": as_str(C["d_name"]).str.strip(),
        "Dropoff City": list(d_city) if d_city else [""] * len(rows),
        "Dropoff State": list(d_state) if d_state else [""] * len(rows),
        "Dropoff Country": as_str(C["d_country"]).str.strip(),
        "Average of In-Transit Time": tracked_days[is_tracked_good].astype("Int64"),
    })

    # Append Grand Total average row
    if len(main) > 0:
        javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean())
    else:
        javg = ""
    total_row = {col: "" for col in main.columns}
    total_row["Bill of Lading"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main

# -----------------------------
# Excel writer (single sheet, no styling)
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

# -----------------------------
# Single CSV builder (both tables in one file)
# -----------------------------
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
    "Mode",
    options=list(MODE_CONFIG.keys()),
    index=0,
    help="FTL, LTL, and Parcel have exact mappings. Ocean/Air reuse FTL until you provide their columns."
)
uploaded = st.file_uploader("Upload RAW file (CSV or Excel)", type=["csv", "xlsx", "xls"], accept_multiple_files=False)
st.caption(f"Selected mode: **{mode}**")

if uploaded:
    try:
        df_raw = load_table(uploaded)
        small_df, main_df = build_summary_tables(df_raw, mode)

        st.success("Summary built successfully.")

        # Previews
        with st.expander("Preview — Small table (rows 1–5)"):
            st.dataframe(small_df, use_container_width=True)
        with st.expander("Preview — Main table (row 7 onward)"):
            st.dataframe(main_df.head(50), use_container_width=True)

        # Single CSV (both tables together)
        single_csv_blob = build_summary_single_csv(small_df, main_df)
        st.download_button(
            "⬇️ Download Summary (Single CSV)",
            data=single_csv_blob,
            file_name=f"Summary_{mode}.csv",
            mime="text/csv",
            use_container_width=True
        )

        # Excel (one sheet "Summary")
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

        # Quick sanity footer
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
