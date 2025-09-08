import io
import math
import re
import sys
import subprocess
import zipfile
import numpy as np
import pandas as pd
import streamlit as st

# ---------- Try to ensure at least one Excel engine (xlsxwriter or openpyxl) ----------
def _ensure_pkg(pkg_name, spec=None):
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

def pick_xlsx_engine():
    if _ensure_pkg("xlsxwriter", "xlsxwriter>=3.2.0"): return "xlsxwriter"
    if _ensure_pkg("openpyxl",  "openpyxl>=3.1.5"):    return "openpyxl"
    return ""  # none available

# ---------- Helpers ----------
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

def parse_timestamp(s):
    if pd.isna(s): return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def as_int_or_nan(x):
    if is_missing_like(x): return np.nan
    try: return int(float(str(x).strip()))
    except Exception: return np.nan

def is_false_like(v):
    if isinstance(v, bool): return v is False
    if is_missing_like(v):  return False
    if isinstance(v, (int, float)): return int(v) == 0
    if isinstance(v, str):  return v.strip().lower() in {"false", "no", "0"}
    return False

def round_half_up_days(x):
    if pd.isna(x): return np.nan
    return math.floor(x + 0.5)

def split_city_state(value):
    if is_missing_like(value): return "", ""
    txt = str(value)
    parts = re.split(r"\s*-\s*", txt, maxsplit=1)
    if len(parts) == 1: parts = txt.split("-", 1)
    if len(parts) == 2: return parts[0].strip(), parts[1].strip()
    return txt.strip(), ""

# ---------- File loader (CSV/Excel) ----------
def load_table(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()
    name = (uploaded_file.name or "").lower()

    # Excel by extension or ZIP signature
    if name.endswith((".xlsx", ".xls")) or raw[:2] == b"PK":
        try:
            _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
            return pd.read_excel(io.BytesIO(raw))
        except Exception:
            pass

    # CSV with sniffer + encodings
    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"]:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            continue

    # Last resort try Excel again
    _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
    return pd.read_excel(io.BytesIO(raw))

# ---------- RAW → Data mapping ----------
DATA_COLUMNS_ORDER = [
    "Carrier Name","Bill of Lading","Tracked","Pickup Name","Pickup City State","Pickup Country",
    "Dropoff Name","Dropoff City State","Dropoff Country","Final Status Reason",
    "Pickup Arrival Utc Timestamp Raw","Pickup Departure Utc Timestamp Raw",
    "Dropoff Arrival Utc Timestamp Raw","Dropoff Departure Utc Timestamp Raw",
    "Nb Milestones Expected","Nb Milestones Received","Milestones Achieved Percentage",
    "Latency Updates Received","Latency Updates Passed","Shipment Latency Percentage",
    "Average Latency (min)",
]

def parse_received_expected(series_ratio: pd.Series):
    s = series_ratio.astype(str)
    m = s.str.extract(r"(-?\d+)\s*/\s*(-?\d+)")
    return pd.to_numeric(m[0], errors="coerce"), pd.to_numeric(m[1], errors="coerce")

def build_data_from_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_headers(df_raw); n = len(df)
    def col(name): return df.get(name, pd.Series([np.nan]*n))

    rec, exp = parse_received_expected(col("# Of Milestones received / # Of Milestones expected"))
    updates_total  = pd.to_numeric(col("# Updates Received"), errors="coerce")
    updates_passed = pd.to_numeric(col("# Updates Received < 10 mins"), errors="coerce")

    with np.errstate(divide="ignore", invalid="ignore"):
        milestones_pct  = (rec / exp) * 100.0
        ship_latency_pct = (updates_passed / updates_total) * 100.0

    data = pd.DataFrame({
        "Carrier Name": col("Carrier Name"),
        "Bill of Lading": col("Bill of Lading"),
        "Tracked": col("Tracked"),
        "Pickup Name": col("Pickup Name"),
        "Pickup City State": col("Pickup City State"),
        "Pickup Country": col("Pickup Country"),
        "Dropoff Name": col("Final Destination Name"),
        "Dropoff City State": col("Final Destination City State"),
        "Dropoff Country": col("Final Destination Country"),
        "Final Status Reason": col("Final Status Reason"),
        "Pickup Arrival Utc Timestamp Raw": col("Pickup Arrival Milestone (UTC)"),
        "Pickup Departure Utc Timestamp Raw": col("Pickup Departure Milestone (UTC)"),
        "Dropoff Arrival Utc Timestamp Raw": col("Final Destination Arrival Milestone (UTC)"),
        "Dropoff Departure Utc Timestamp Raw": col("Final Destination Departure Milestone (UTC)"),
        "Nb Milestones Expected": exp,
        "Nb Milestones Received": rec,
        "Milestones Achieved Percentage": milestones_pct,
        "Latency Updates Received": updates_total,
        "Latency Updates Passed": updates_passed,
        "Shipment Latency Percentage": ship_latency_pct,
        "Average Latency (min)": pd.to_numeric(col("Average Latency (min)"), errors="coerce"),
    })

    return data[DATA_COLUMNS_ORDER]

# ---------- V column + Summary builders ----------
def compute_in_transit_time_row(row):
    # Untracked if Tracked is FALSE-like OR Nb Milestones Received empty/0/NA
    tracked = row.get("Tracked", np.nan)
    nb_recv = as_int_or_nan(row.get("Nb Milestones Received", np.nan))
    if is_false_like(tracked) or (pd.isna(nb_recv) or nb_recv == 0):
        return "Untracked"

    # Compute days (Dropoff Arrival - Pickup Departure), round .5 up
    pick_dep = parse_timestamp(row.get("Pickup Departure Utc Timestamp Raw"))
    drop_arr = parse_timestamp(row.get("Dropoff Arrival Utc Timestamp Raw"))
    if pd.isna(pick_dep) or pd.isna(drop_arr):
        return "Missing Milestone"

    delta_days = (drop_arr - pick_dep).total_seconds() / (24*3600)
    if delta_days <= 0:
        return "Missing Milestone"

    return int(round_half_up_days(delta_days))

def build_summary(df_data):
    v = df_data["In-Transit Time"]
    is_num = pd.to_numeric(v, errors="coerce").notna()
    count_tracked  = int(is_num.sum())
    count_missing  = int((v == "Missing Milestone").sum())
    count_untracked= int((v == "Untracked").sum())
    grand_total    = count_tracked + count_missing + count_untracked

    numeric_vals = pd.to_numeric(v, errors="coerce")
    avg_days_all = float(numeric_vals.dropna().mean()) if numeric_vals.notna().any() else np.nan

    # small summary table (no formatting, just values)
    small = pd.DataFrame({
        "Label": ["Tracked","Missed Milestone","Untracked","Grand Total"],
        "Shipment Count": [count_tracked, count_missing, count_untracked, count_tracked + count_missing + count_untracked],
        "": ["","","",""],  # blank column
        "Average of In-Transit Time": ["","","", avg_days_all if pd.notna(avg_days_all) else ""],
        "Time taken from Departure to Arrival": ["","","",""],  # keep blank unless you want another KPI
    })

    # main table (only numeric V)
    df_num = df_data[is_num].copy()
    pick_city, pick_state = zip(*df_num["Pickup City State"].map(split_city_state)) if len(df_num) else ([],[])
    drop_city, drop_state = zip(*df_num["Dropoff City State"].map(split_city_state)) if len(df_num) else ([],[])

    main = pd.DataFrame({
        "Bill of Lading": df_num["Bill of Lading"].astype(str),
        "Pickup Name": df_num["Pickup Name"].astype(str),
        "Pickup City": list(pick_city),
        "Pickup State": list(pick_state),
        "Pickup Country": df_num["Pickup Country"].astype(str),
        "Dropoff Name": df_num["Dropoff Name"].astype(str),
        "Dropoff City": list(drop_city),
        "Dropoff State": list(drop_state),
        "Dropoff Country": df_num["Dropoff Country"].astype(str),
        "Average of In-Transit Time": pd.to_numeric(df_num["In-Transit Time"], errors="coerce").astype("Int64"),
    })

    # append Grand Total average row at the end of main
    if len(main) > 0:
        javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean())
    else:
        javg = np.nan
    total_row = {col: "" for col in main.columns}
    total_row["Bill of Lading"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg if not pd.isna(javg) else ""
    main_with_total = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main_with_total

# ---------- Write one XLSX OR ZIP CSV fallback ----------
def build_report_blob(df_data, small_summary, main_summary):
    engine = pick_xlsx_engine()

    if engine:
        # Write clean .xlsx (no styling), Summary top table at rows 1–5, blank row 6, main from row 7
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine=engine) as writer:
            # Data sheet
            df_data.to_excel(writer, sheet_name="Data", index=False)

            # Summary sheet: small table at top
            small_summary.to_excel(writer, sheet_name="Summary", index=False, startrow=0)

            # blank row 6 (index 5) will remain blank because we start next table at startrow=6
            # Main table starting row 7 (index 6)
            main_summary.to_excel(writer, sheet_name="Summary", index=False, startrow=6)

        buf.seek(0)
        return buf.getvalue(), "xlsx", "FTL_Data_and_Summary.xlsx"

    # Fallback: one ZIP containing Data.csv + Summary_small.csv + Summary_main.csv
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Data.csv", df_data.to_csv(index=False))
        z.writestr("Summary_small.csv", small_summary.to_csv(index=False))
        z.writestr("Summary_main.csv", main_summary.to_csv(index=False))
    zbuf.seek(0)
    return zbuf.getvalue(), "zip", "FTL_Report.zip"

# ---------- Streamlit UI ----------
st.set_page_config(page_title="FTL In-Transit Builder", page_icon="🚚", layout="wide")
st.title("FTL In-Transit Time Processor (RAW → Data → Summary)")

uploaded = st.file_uploader("Upload RAW CSV or Excel", type=["csv", "xlsx", "xls"])

if uploaded:
    try:
        raw_df  = load_table(uploaded)
        data_df = build_data_from_raw(raw_df)

        # Column V: In-Transit Time (Untracked / Missing Milestone / rounded days)
        data_df["In-Transit Time"] = data_df.apply(compute_in_transit_time_row, axis=1)

        # Summary sheets
        small_df, main_df = build_summary(data_df)

        # Build single report (XLSX if engine available; else ZIP with CSVs)
        blob, ext, fname = build_report_blob(data_df, small_df, main_df)

        st.success("Processed! Download your report below.")
        st.download_button(
            "⬇️ Download Report" + (" (Excel .xlsx)" if ext=="xlsx" else " (ZIP: CSV files)"),
            data=blob,
            file_name=fname,
            mime=("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if ext=="xlsx" else "application/zip"),
            use_container_width=True
        )

        with st.expander("Preview: Data (A..U + V)"):
            st.dataframe(data_df.head(50), use_container_width=True)
        with st.expander("Preview: Summary main table"):
            st.dataframe(main_df.head(50), use_container_width=True)

    except Exception as e:
        st.error(f"Could not process this file. Details: {e}")
else:
    st.info("Upload your raw file to begin.")
