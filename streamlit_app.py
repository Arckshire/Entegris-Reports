import io
import math
import re
import sys
import subprocess
import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------------
# App meta
# -----------------------------------
st.set_page_config(page_title="Entegris Reports", page_icon="📦", layout="wide")
st.title("Entegris Reports — Summary Builder (Tracked / Missed / Untracked)")

# -----------------------------------
# Dependency helper (Excel engines)
# -----------------------------------
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

# -----------------------------------
# Header normalization + matching
# -----------------------------------
def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Drop empty/Unnamed columns, collapse whitespace."""
    df = df.copy()
    keep = []
    for c in df.columns:
        s = str(c).strip()
        if s and not s.lower().startswith("unnamed"):
            keep.append(c)
    df = df.loc[:, keep]
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df

def canon(s: str) -> str:
    """Canonicalize a header for matching."""
    if s is None:
        return ""
    s2 = str(s).replace("–","-").replace("—","-")
    s2 = re.sub(r"\s+", " ", s2.strip())
    return s2.lower()

def find_col(df: pd.DataFrame, targets: list[str]) -> str | None:
    if not len(df.columns):
        return None
    canon_map = {c: canon(c) for c in df.columns}
    tcanon = [canon(t) for t in targets]
    for c, cc in canon_map.items():
        if cc in tcanon:
            return c
    for pat in targets:
        cp = canon(pat)
        try:
            rx = re.compile(cp)
        except re.error:
            rx = re.compile(re.escape(cp))
        for c, cc in canon_map.items():
            if rx.fullmatch(cc) or rx.search(cc):
                return c
    return None

def find_cols_by_regex(df: pd.DataFrame, pattern: str) -> list[str]:
    rx = re.compile(pattern)
    out = []
    for c in df.columns:
        if rx.search(canon(c)):
            out.append(c)
    return out

# -----------------------------------
# Value helpers
# -----------------------------------
def is_missing_like(x):
    if pd.isna(x): return True
    if isinstance(x, str) and x.strip().lower() in {"", "na", "n/a", "null", "none"}: return True
    return False

def is_true_like(v):
    if isinstance(v, bool): return v is True
    if is_missing_like(v):  return False
    if isinstance(v, (int, float, np.floating)): return int(v) == 1
    if isinstance(v, str):  return v.strip().lower() in {"true","yes","y","1"}
    return False

def is_false_like(v):
    if isinstance(v, bool): return v is False
    if is_missing_like(v):  return False
    if isinstance(v, (int, float, np.floating)): return int(v) == 0
    if isinstance(v, str):  return v.strip().lower() in {"false","no","n","0"}
    return False

def parse_timestamp_utc(s):
    if pd.isna(s): return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def round_half_up_days_scalar(x):
    """x is a float or NaN; never pd.NA."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return np.nan
    return math.floor(x + 0.5)

def safe_mean(series) -> float | str:
    """Return float mean or '' if no numeric values (avoids float(pd.NA))."""
    s = pd.to_numeric(series, errors="coerce")
    s = s.dropna()
    return float(s.mean()) if len(s) > 0 else ""

def split_city_state(text: str):
    if is_missing_like(text): return "", ""
    s = str(text).strip()
    parts = re.split(r"\s*-\s*", s, maxsplit=1)
    if len(parts) == 2: return parts[0].strip(), parts[1].strip()
    m = re.match(r"^(.*?)[\s,]+([A-Z]{2})$", s)
    if m: return m.group(1).strip(), m.group(2).strip()
    return s, ""

# -----------------------------------
# Mode configs (columns & E1 text)
# -----------------------------------
MODE_CONFIG = {
    "FTL": {
        "columns": {
            "bol": ["Bill of Lading"],
            "tracked": ["Tracked"],
            "p_name": ["Pickup Name"],
            "p_city_state": ["Pickup City State"],
            "p_country": ["Pickup Country"],
            "d_name": ["Final Destination Name"],
            "d_city_state": ["Final Destination City State"],
            "d_country": ["Final Destination Country"],
            "start_ts": ["Pickup Departure Milestone (UTC)"],
            "end_ts":   ["Final Destination Arrival Milestone (UTC)"],
        },
        "e1_text": "Time taken from Departure to Arrival",
        "classification": "tracked_boolean",
        "main_first_col_header": "Bill of Lading",
    },
    "LTL": {
        "columns": {
            "bol": ["Bill of Lading"],
            "tracked": ["Tracked"],
            "p_name": ["Pickup Name"],
            "p_city_state": ["Pickup City State"],
            "p_country": ["Pickup Country"],
            "d_name": ["Destination Name"],
            "d_city_state": ["Dropoff City State"],
            "d_country": ["Dropoff Country"],
            "start_ts": ["Pickup Utc Timestamp Time"],
            "end_ts":   ["Delivered Utc Timestamp Time"],
        },
        "e1_text": "Time taken from Pickup to Delivered",
        "classification": "tracked_boolean",
        "main_first_col_header": "Bill of Lading",
    },
    "Parcel": {
        "columns": {
            "bol": ["Bill of Lading"],
            "tracked": ["Tracked"],
            "p_name": ["Pickup Name"],
            "p_city_state": ["Pickup City State"],
            "p_country": ["Pickup Country"],
            "d_name": ["Destination Name"],
            "d_city_state": ["Dropoff City State"],
            "d_country": ["Dropoff Country"],
            "start_ts": ["Departed Utc Timestamp Time"],
            "end_ts":   ["Delivered Utc Timestamp Time"],
        },
        "e1_text": "Time taken from Departed to Delivered",
        "classification": "tracked_boolean",
        "main_first_col_header": "Bill of Lading",
    },
    "Ocean": {
        "columns": {
            "bol": ["Container Number"],
            "fallback_bol": ["Shipment ID"],
            "tracked": None,
            "p_name": ["Pol"],
            "p_city_state": None,
            "p_country": None,
            "d_name": ["Pod"],
            "d_city_state": None,
            "d_country": None,
            "start_ts": [r"^\s*2\s*-\s*gate in timestamp\s*$", "2-Gate In Timestamp", "Gate In Timestamp"],
            "end_ts":   [r"^\s*7\s*-\s*gate out timestamp\s*$", "7-Gate Out Timestamp", "Gate Out Timestamp"],
            "miss_flags_hint": [r"\bmissed\b"],
        },
        "e1_text": "Time taken from Gate In to Gate Out",
        "classification": "ocean_rules",
        "main_first_col_header": "Container Number",
    },
}

def columns_for_mode(mode: str):
    cfg = MODE_CONFIG.get(mode, MODE_CONFIG["FTL"]).copy()
    if "classification" not in cfg:
        cfg["classification"] = "tracked_boolean"
    return cfg

# -----------------------------------
# Loader (CSV or Excel)
# -----------------------------------
def load_table(uploaded_file) -> pd.DataFrame:
    raw_bytes = uploaded_file.read()
    name = (uploaded_file.name or "").lower()

    if name.endswith((".xlsx", ".xls")) or raw_bytes[:2] == b"PK":
        try:
            _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
            return pd.read_excel(io.BytesIO(raw_bytes))
        except Exception:
            pass

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

    _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
    return pd.read_excel(io.BytesIO(raw_bytes))

# -----------------------------------
# Core builder: Summary only
# -----------------------------------
def build_summary_tables(df_raw: pd.DataFrame, mode: str):
    df = normalize_headers(df_raw).copy()
    cfg = columns_for_mode(mode)
    C = cfg["columns"]

    def get_series(targets, default_name=None):
        if targets is None:
            return pd.Series([np.nan]*len(df)), None
        colname = find_col(df, targets)
        if not colname and default_name:
            colname = find_col(df, [default_name])
        if not colname:
            return pd.Series([np.nan]*len(df)), None
        return df[colname], colname

    # Resolve fields
    s_bol, bol_colname       = get_series(C.get("bol"))
    s_pname, pname_colname   = get_series(C.get("p_name"))
    s_pcitystate, _          = get_series(C.get("p_city_state")) if C.get("p_city_state") else (pd.Series([""]*len(df)), None)
    s_pcountry, _            = get_series(C.get("p_country"))    if C.get("p_country")    else (pd.Series([""]*len(df)), None)
    s_dname, dname_colname   = get_series(C.get("d_name"))
    s_dcitystate, _          = get_series(C.get("d_city_state")) if C.get("d_city_state") else (pd.Series([""]*len(df)), None)
    s_dcountry, _            = get_series(C.get("d_country"))    if C.get("d_country")    else (pd.Series([""]*len(df)), None)
    s_start, start_colname   = get_series(C.get("start_ts"))
    s_end, end_colname       = get_series(C.get("end_ts"))

    if cfg["classification"] != "ocean_rules":
        s_tracked, tracked_colname = get_series(C.get("tracked"))
    else:
        s_tracked, tracked_colname = pd.Series([np.nan]*len(df)), None

    # Ocean miss flags (auto-detect & NA-safe)
    miss_cols = []
    if cfg["classification"] == "ocean_rules":
        miss_candidates = find_cols_by_regex(df, r"\bmissed\b")
        if miss_candidates:
            def miss_key(c):
                m = re.match(r"^\s*(\d+)", canon(c))
                return int(m.group(1)) if m else 999
            miss_candidates = sorted(miss_candidates, key=miss_key)
            miss_cols = miss_candidates[:8]
        else:
            miss_cols = []

    # Parse timestamps -> float days (never NAType)
    dep_ts = s_start.apply(parse_timestamp_utc)
    arr_ts = s_end.apply(parse_timestamp_utc)
    delta_sec = (arr_ts - dep_ts).dt.total_seconds()
    delta_days = (delta_sec / (24*3600)).astype("float64")  # ensure numpy float, NaN where missing
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: round_half_up_days_scalar(x) if not np.isnan(x) else np.nan)

    # Classification
    if cfg["classification"] == "ocean_rules":
        if miss_cols:
            miss_df = df[miss_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            all_missed = (miss_df == 1).all(axis=1)
        else:
            all_missed = pd.Series(False, index=df.index)

        missing = (~all_missed) & (dep_ts.isna() | arr_ts.isna() | ~valid_transit.fillna(False))
        tracked_good = ~(all_missed | missing)

        is_untracked = all_missed
        is_missing   = missing
        is_tracked_good = tracked_good
    else:
        is_untracked     = s_tracked.apply(is_false_like)
        is_tracked_true  = s_tracked.apply(is_true_like)
        is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
        is_tracked_good  = (~is_untracked) & is_tracked_true &  valid_transit.fillna(False)

    cnt_untracked = int(is_untracked.sum())
    cnt_missing   = int(is_missing.sum())
    cnt_tracked   = int(is_tracked_good.sum())
    grand_total   = cnt_untracked + cnt_missing + cnt_tracked

    tracked_days = pd.Series(in_transit_days).where(is_tracked_good)
    avg_tracked = safe_mean(tracked_days)

    # Small table (rows 1–5)
    small = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_missing, cnt_untracked, grand_total],
        "": ["", "", "", ""],
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        MODE_CONFIG[mode]["e1_text"]: ["", "", "", ""],
    })

    # Main table rows: only tracked_good
    rows_mask = is_tracked_good.fillna(False)
    rows = df[rows_mask].copy()

    def series_as_str(s):
        return s.astype(str) if len(s) else pd.Series([""] * len(rows))

    # City/state split helpers
    def split_series_col(col_name):
        if col_name and col_name in rows.columns and len(rows):
            s = rows[col_name].astype(str)
            if len(s):
                a, b = zip(*s.map(split_city_state))
                return list(a), list(b)
        return [""] * len(rows), [""] * len(rows)

    # First column label/values
    first_col_header = MODE_CONFIG[mode].get("main_first_col_header", "Bill of Lading")
    if mode == "Ocean":
        first_values = series_as_str(s_bol).str.strip()
        if (first_values == "").all():
            fb_col = find_col(rows, C.get("fallback_bol", [])) if len(rows) else None
            if fb_col and fb_col in rows.columns:
                first_values = rows[fb_col].astype(str).str.strip()
    else:
        first_values = series_as_str(s_bol).str.strip()

    # City/state
    p_city, p_state = split_series_col(find_col(df, C.get("p_city_state", [])) if C.get("p_city_state") else None)
    d_city, d_state = split_series_col(find_col(df, C.get("d_city_state", [])) if C.get("d_city_state") else None)

    # Build main
    main = pd.DataFrame({
        first_col_header: first_values,
        "Pickup Name": (rows[find_col(df, C["p_name"])].astype(str).str.strip() if find_col(df, C["p_name"]) else [""]*len(rows)),
        "Pickup City": p_city,
        "Pickup State": p_state,
        "Pickup Country": (rows[find_col(df, C["p_country"])].astype(str).str.strip() if C.get("p_country") and find_col(df, C["p_country"]) else [""]*len(rows)),
        "Dropoff Name": (rows[find_col(df, C["d_name"])].astype(str).str.strip() if find_col(df, C["d_name"]) else [""]*len(rows)),
        "Dropoff City": d_city,
        "Dropoff State": d_state,
        "Dropoff Country": (rows[find_col(df, C["d_country"])].astype(str).str.strip() if C.get("d_country") and find_col(df, C["d_country"]) else [""]*len(rows)),
        "Average of In-Transit Time": pd.to_numeric(pd.Series(in_transit_days)[rows_mask], errors="coerce").round().astype("Int64"),
    })

    # Append Grand Total average row
    javg = safe_mean(main["Average of In-Transit Time"])
    total_row = {col: "" for col in main.columns}
    total_row[first_col_header] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # Diagnostics
    diag = {}
    if mode == "Ocean":
        diag = {
            "Detected start_ts": start_colname or "(not found)",
            "Detected end_ts": end_colname or "(not found)",
            "Detected miss flags (first 8)": miss_cols if miss_cols else ["(none found)"],
            "Detected Container Number col": bol_colname or "(not found)",
            "Detected Pol": pname_colname or "(not found)",
            "Detected Pod": dname_colname or "(not found)",
        }
    else:
        diag = {
            "Detected Tracked col": tracked_colname or "(not found)",
            "Detected start_ts": start_colname or "(not found)",
            "Detected end_ts": end_colname or "(not found)",
        }

    return small, main, diag

# -----------------------------------
# Excel writer (single sheet, no styling)
# -----------------------------------
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

# -----------------------------------
# Single CSV builder (both tables in one file)
# -----------------------------------
def build_summary_single_csv(small_df: pd.DataFrame, main_df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    small_df.to_csv(buf, index=False)
    buf.write("\n")  # blank row 6
    main_df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

# -----------------------------------
# UI
# -----------------------------------
mode = st.selectbox(
    "Mode",
    options=["FTL", "LTL", "Parcel", "Ocean"],
    index=0,
    help="All four modes wired per your rules. Ocean uses robust column detection."
)
uploaded = st.file_uploader("Upload RAW file (CSV or Excel)", type=["csv", "xlsx", "xls"], accept_multiple_files=False)
st.caption(f"Selected mode: **{mode}**")

if uploaded:
    try:
        df_raw = load_table(uploaded)
        small_df, main_df, diag = build_summary_tables(df_raw, mode)

        st.success("Summary built successfully.")

        with st.expander("Preview — Small table (rows 1–5)"):
            st.dataframe(small_df, use_container_width=True)
        with st.expander("Preview — Main table (row 7 onward)"):
            st.dataframe(main_df.head(50), use_container_width=True)
        with st.expander("Diagnostics (detected columns)"):
            st.json(diag)

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
