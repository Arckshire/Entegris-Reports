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
    """Drop empty/Unnamed columns, collapse whitespace, standardize hyphens, lowercase copy for matching."""
    df = df.copy()
    keep = []
    for c in df.columns:
        s = str(c).strip()
        if s and not s.lower().startswith("unnamed"):
            keep.append(c)
    df = df.loc[:, keep]
    # Collapse whitespace
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df

def canon(s: str) -> str:
    """Canonicalize a header for matching: lowercase, collapse spaces, replace en-dash/em-dash with '-'."""
    if s is None:
        return ""
    s2 = str(s)
    s2 = s2.replace("–","-").replace("—","-")
    s2 = re.sub(r"\s+", " ", s2.strip())
    return s2.lower()

def find_col(df: pd.DataFrame, targets: list[str]) -> str | None:
    """
    Flexible header finder.
    - targets: list of exact strings or regex patterns (lowercased, with hyphen variants allowed)
    Returns the actual df column name (original case) or None.
    """
    if not len(df.columns):
        return None
    # Build canonical map
    canon_map = {c: canon(c) for c in df.columns}
    # Try exact (canonical) matches first
    tcanon = [canon(t) for t in targets]
    for c, cc in canon_map.items():
        if cc in tcanon:
            return c
    # Try regex over canonical names
    for pat in targets:
        cp = canon(pat)
        try:
            rx = re.compile(cp)
        except re.error:
            # If pattern not valid regex, escape it
            rx = re.compile(re.escape(cp))
        for c, cc in canon_map.items():
            if rx.fullmatch(cc) or rx.search(cc):
                return c
    return None

def find_cols_by_regex(df: pd.DataFrame, pattern: str) -> list[str]:
    """Return all columns whose canonical form matches the regex."""
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
    if isinstance(v, (int, float)): return int(v) == 1
    if isinstance(v, str):  return v.strip().lower() in {"true","yes","y","1"}
    return False

def is_false_like(v):
    if isinstance(v, bool): return v is False
    if is_missing_like(v):  return False
    if isinstance(v, (int, float)): return int(v) == 0
    if isinstance(v, str):  return v.strip().lower() in {"false","no","n","0"}
    return False

def parse_timestamp_utc(s):
    if pd.isna(s): return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def round_half_up_days(x):
    if pd.isna(x): return np.nan
    return math.floor(x + 0.5)  # 3.5->4, 3.4->3

def split_city_state(text: str):
    """Preferred: split on first '-' (city - state). Else try last 2-letter uppercase token."""
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
    # FTL (Truckload) — unchanged
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

    # LTL — unchanged logic; only timestamps differ
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

    # Parcel — unchanged logic; timestamps differ
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

    # Ocean — robust header matching & missed flags auto-detect
    "Ocean": {
        "columns": {
            "bol": ["Container Number"],  # fallback to Shipment ID if empty
            "fallback_bol": ["Shipment ID"],
            "tracked": None,  # not used
            "p_name": ["Pol"],
            "p_city_state": None,
            "p_country": None,
            "d_name": ["Pod"],
            "d_city_state": None,
            "d_country": None,
            # Use flexible matching for these timestamps:
            "start_ts": [r"^\s*2\s*-\s*gate in timestamp\s*$", "2-Gate In Timestamp", "Gate In Timestamp"],
            "end_ts":   [r"^\s*7\s*-\s*gate out timestamp\s*$", "7-Gate Out Timestamp", "Gate Out Timestamp"],
            # Miss flags: detect any columns that end with 'Missed'
            "miss_flags_hint": [r"\bmissed\b"],  # used to pick columns via regex
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

# -----------------------------------
# Core builder: Summary only
# -----------------------------------
def build_summary_tables(df_raw: pd.DataFrame, mode: str):
    df = normalize_headers(df_raw).copy()
    cfg = columns_for_mode(mode)
    C = cfg["columns"]

    # Resolve required columns with flexible finder
    def get_series(targets, default_name=None):
        if targets is None:
            return pd.Series([np.nan]*len(df)), None
        colname = find_col(df, targets)
        if not colname and default_name:
            colname = find_col(df, [default_name])
        if not colname:
            # create empty series
            return pd.Series([np.nan]*len(df)), None
        return df[colname], colname

    # Core fields
    s_bol, bol_colname       = get_series(C.get("bol"))
    s_pname, pname_colname   = get_series(C.get("p_name"))
    s_pcitystate, _          = get_series(C.get("p_city_state")) if C.get("p_city_state") else (pd.Series([""]*len(df)), None)
    s_pcountry, _            = get_series(C.get("p_country"))    if C.get("p_country")    else (pd.Series([""]*len(df)), None)
    s_dname, dname_colname   = get_series(C.get("d_name"))
    s_dcitystate, _          = get_series(C.get("d_city_state")) if C.get("d_city_state") else (pd.Series([""]*len(df)), None)
    s_dcountry, _            = get_series(C.get("d_country"))    if C.get("d_country")    else (pd.Series([""]*len(df)), None)
    s_start, start_colname   = get_series(C.get("start_ts"))
    s_end, end_colname       = get_series(C.get("end_ts"))

    # Tracked only for non-Ocean
    if cfg["classification"] != "ocean_rules":
        s_tracked, tracked_colname = get_series(C.get("tracked"))
    else:
        s_tracked, tracked_colname = pd.Series([np.nan]*len(df)), None

    # Ocean miss flags auto-detect
    miss_cols = []
    if cfg["classification"] == "ocean_rules":
        # Prefer exactly 8 columns that end with 'Missed'
        # First: find all columns whose canonical form contains 'missed'
        miss_candidates = find_cols_by_regex(df, r"\bmissed\b")
        # Keep the classic 8 if present; otherwise, use all candidates
        if miss_candidates:
            # Sort by the numeric prefix if present (1- .. 8- ..)
            def miss_key(c):
                m = re.match(r"^\s*(\d+)", canon(c))
                return int(m.group(1)) if m else 999
            miss_candidates = sorted(miss_candidates, key=miss_key)
            # Try to keep only 8; if more, take the first 8 by the sorted order
            miss_cols = miss_candidates[:8]
        else:
            miss_cols = []

    # ----- Compute transit -----
    dep_ts = s_start.apply(parse_timestamp_utc)
    arr_ts = s_end.apply(parse_timestamp_utc)
    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    # ----- Classification -----
    if cfg["classification"] == "ocean_rules":
        if miss_cols:
            miss_df = df[miss_cols].apply(pd.to_numeric, errors="coerce")
            all_missed = miss_df.apply(lambda r: np.all(r == 1), axis=1)
        else:
            # If we couldn't detect any miss columns, treat as all False (so not untracked)
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

    tracked_days = in_transit_days.where(is_tracked_good)
    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    # Build small table (rows 1–5)
    small = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_missing, cnt_untracked, grand_total],
        "": ["", "", "", ""],
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        MODE_CONFIG[mode]["e1_text"]: ["", "", "", ""],
    })

    # Main table rows: only tracked_good
    rows_mask = is_tracked_good
    rows = df[rows_mask].copy()

    def as_str(series):
        return series.astype(str) if len(series) else pd.Series([""] * len(rows))

    # City/state splits
    def split_series(full_series):
        if len(full_series) and len(rows):
            s = rows[full_series.name].astype(str)
            if len(s):
                a, b = zip(*s.map(split_city_state))
                return list(a), list(b)
        return [""] * len(rows), [""] * len(rows)

    # First column label/values
    first_col_header = MODE_CONFIG[mode].get("main_first_col_header", "Bill of Lading")
    if mode == "Ocean":
        first_values = as_str(s_bol).str.strip()
        if (first_values == "").all():
            # fallback to Shipment ID if configured and present
            fb_name = find_col(df, C.get("fallback_bol", []))
            if fb_name and fb_name in rows.columns:
                first_values = rows[fb_name].astype(str).str.strip()
    else:
        first_values = as_str(s_bol).str.strip()

    # City/state
    if C.get("p_city_state") and find_col(df, C["p_city_state"]):
        p_city, p_state = split_series(df[find_col(df, C["p_city_state"])])
    else:
        p_city, p_state = [""] * len(rows), [""] * len(rows)

    if C.get("d_city_state") and find_col(df, C["d_city_state"]):
        d_city, d_state = split_series(df[find_col(df, C["d_city_state"])])
    else:
        d_city, d_state = [""] * len(rows), [""] * len(rows)

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
        "Average of In-Transit Time": tracked_days[rows_mask].astype("Int64"),
    })

    # Append Grand Total average row
    if len(main) > 0:
        javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean())
    else:
        javg = ""
    total_row = {col: "" for col in main.columns}
    total_row[first_col_header] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # Diagnostics info for Ocean
    diag = {}
    if mode == "Ocean":
        diag = {
            "Detected start_ts": start_colname or "(not found)",
            "Detected end_ts": end_colname or "(not found)",
            "Detected miss flags": miss_cols if miss_cols else ["(none found)"],
            "Detected Container Number col": bol_colname or "(not found)",
            "Detected Pickup (Pol)": pname_colname or "(not found)",
            "Detected Dropoff (Pod)": dname_colname or "(not found)",
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
