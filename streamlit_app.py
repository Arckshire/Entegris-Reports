import io
import math
import re
import xlsxwriter
import codecs
from typing import Tuple

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------
# Utilities
# ---------------------------------------------
def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Drop columns with empty header like "", "Unnamed: 0"
    df = df.loc[:, [c for c in df.columns if str(c).strip() != ""]]
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df

def is_missing_like(x):
    if pd.isna(x):
        return True
    if isinstance(x, str) and x.strip().lower() in {"", "na", "n/a", "null", "none"}:
        return True
    return False

def parse_timestamp(s):
    if pd.isna(s):
        return pd.NaT
    return pd.to_datetime(s, utc=True, errors="coerce")

def as_int_or_nan(x):
    if is_missing_like(x):
        return np.nan
    try:
        return int(float(str(x).strip()))
    except Exception:
        return np.nan

def is_false_like(value):
    if isinstance(value, bool):
        return value is False
    if is_missing_like(value):
        return False
    if isinstance(value, (int, float)):
        return int(value) == 0
    if isinstance(value, str):
        return value.strip().lower() in {"false", "no", "0"}
    return False

def round_half_up_days(days_float):
    if pd.isna(days_float):
        return np.nan
    return math.floor(days_float + 0.5)

def split_city_state(value):
    if is_missing_like(value):
        return "", ""
    txt = str(value)
    parts = re.split(r"\s*-\s*", txt, maxsplit=1)
    if len(parts) == 1:
        parts = txt.split("-", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return txt.strip(), ""

# ---------------------------------------------
# Robust loader (CSV or Excel; various encodings/delimiters)
# ---------------------------------------------
def load_table(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()  # bytes
    name = (uploaded_file.name or "").lower()

    # Excel?
    if name.endswith((".xlsx", ".xls")) or raw[:2] == b"PK":
        try:
            return pd.read_excel(io.BytesIO(raw))
        except Exception:
            pass

    # Try CSV with multiple encodings + auto delimiter
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"]
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            continue

    # As a last resort, try Excel again
    return pd.read_excel(io.BytesIO(raw))

# ---------------------------------------------
# RAW → Data (select, rename, derive)
# ---------------------------------------------
DATA_COLUMNS_ORDER = [
    "Carrier Name",
    "Bill of Lading",
    "Tracked",
    "Pickup Name",
    "Pickup City State",
    "Pickup Country",
    "Dropoff Name",
    "Dropoff City State",
    "Dropoff Country",
    "Final Status Reason",
    "Pickup Arrival Utc Timestamp Raw",
    "Pickup Departure Utc Timestamp Raw",
    "Dropoff Arrival Utc Timestamp Raw",
    "Dropoff Departure Utc Timestamp Raw",
    "Nb Milestones Expected",
    "Nb Milestones Received",
    "Milestones Achieved Percentage",
    "Latency Updates Received",
    "Latency Updates Passed",
    "Shipment Latency Percentage",
    "Average Latency (min)",
]

def parse_received_expected(series_ratio: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Extract 'received' and 'expected' from strings like '3 / 10'.
    Returns numeric Series (float) with NaN on failure.
    """
    s = series_ratio.astype(str)
    m = s.str.extract(r"(-?\d+)\s*/\s*(-?\d+)")
    received = pd.to_numeric(m[0], errors="coerce")
    expected = pd.to_numeric(m[1], errors="coerce")
    return received, expected

def build_data_from_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_headers(df_raw)
    n = len(df)

    def col(name):
        return df.get(name, pd.Series([np.nan] * n))

    # Parse milestones received/expected from the ratio column
    rec, exp = parse_received_expected(col("# Of Milestones received / # Of Milestones expected"))

    # Shipment latency % from updates within 10 mins vs total updates
    updates_total = pd.to_numeric(col("# Updates Received"), errors="coerce")
    updates_passed = pd.to_numeric(col("# Updates Received < 10 mins"), errors="coerce")

    with np.errstate(divide="ignore", invalid="ignore"):
        milestones_pct = (rec / exp) * 100.0
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

    # Reorder to exact A..U order
    data = data[DATA_COLUMNS_ORDER]

    return data

# ---------------------------------------------
# V column logic + Summary
# ---------------------------------------------
def compute_in_transit_time_row(row):
    # Untracked rule
    tracked_val = row.get("Tracked", np.nan)
    nb_recv = row.get("Nb Milestones Received", np.nan)
    milestones_i = as_int_or_nan(nb_recv)
    if is_false_like(tracked_val) or (pd.isna(milestones_i) or milestones_i == 0):
        return "Untracked"

    # Compute days = Dropoff Arrival - Pickup Departure
    pick_dep = parse_timestamp(row.get("Pickup Departure Utc Timestamp Raw"))
    drop_arr = parse_timestamp(row.get("Dropoff Arrival Utc Timestamp Raw"))
    if pd.isna(pick_dep) or pd.isna(drop_arr):
        return "Missing Milestone"

    delta_days = (drop_arr - pick_dep).total_seconds() / (24 * 3600)
    if delta_days <= 0:
        return "Missing Milestone"

    return int(round_half_up_days(delta_days))

def build_summary_sheet(df_data):
    v = df_data["In-Transit Time"]
    is_numeric_v = pd.to_numeric(v, errors="coerce").notna()

    count_tracked = int(is_numeric_v.sum())
    count_missing = int((v == "Missing Milestone").sum())
    count_untracked = int((v == "Untracked").sum())
    grand_total = count_tracked + count_missing + count_untracked

    numeric_vals = pd.to_numeric(v, errors="coerce")
    avg_days_all = float(numeric_vals.dropna().mean()) if numeric_vals.notna().any() else np.nan

    df_numeric = df_data[is_numeric_v].copy()

    # Split city/state for main table
    pick_city, pick_state = zip(*df_numeric["Pickup City State"].map(split_city_state)) if len(df_numeric) else ([], [])
    drop_city, drop_state = zip(*df_numeric["Dropoff City State"].map(split_city_state)) if len(df_numeric) else ([], [])

    summary_main = pd.DataFrame({
        "Bill of Lading": df_numeric["Bill of Lading"].astype(str),
        "Pickup Name": df_numeric["Pickup Name"].astype(str),
        "Pickup City": list(pick_city),
        "Pickup State": list(pick_state),
        "Pickup Country": df_numeric["Pickup Country"].astype(str),
        "Dropoff Name": df_numeric["Dropoff Name"].astype(str),
        "Dropoff City": list(drop_city),
        "Dropoff State": list(drop_state),
        "Dropoff Country": df_numeric["Dropoff Country"].astype(str),
        "Average of In-Transit Time": pd.to_numeric(df_numeric["In-Transit Time"], errors="coerce").astype("Int64"),
    })

    # Small top table (we’ll place it in Excel with formatting)
    small_table = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [count_tracked, count_missing, count_untracked, grand_total],
        "Average of In-Transit Time (days)": [np.nan, np.nan, np.nan, avg_days_all],
        "Time taken from Departure to Arrival": [np.nan, np.nan, np.nan, avg_days_all],
    })

    return summary_main, small_table

# ---------------------------------------------
# Excel writer with styling
# ---------------------------------------------
def write_excel_with_formatting(df_data, summary_main, small_table):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # ---- Data sheet (ONLY the selected columns + V) ----
        df_data.to_excel(writer, sheet_name="Data", index=False)

        wb = writer.book
        ws = wb.add_worksheet("Summary")

        fmt_header = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})
        fmt_bold = wb.add_format({"bold": True})
        fmt_border = wb.add_format({"border": 1})
        fmt_blue_bold_border = fmt_header

        # Column widths (Summary)
        ws.set_column("A:A", 22)
        ws.set_column("B:B", 18)
        ws.set_column("C:C", 2)   # blank separator
        ws.set_column("D:D", 28)
        ws.set_column("E:E", 34)

        # ---- Small table rows 1–5 ----
        ws.write(0, 0, "Label", fmt_blue_bold_border)                 # A1
        ws.write(0, 1, "Shipment Count", fmt_blue_bold_border)        # B1
        ws.write(0, 2, "", fmt_border)                                # C1 blank
        ws.write(0, 3, "Average of In-Transit Time", fmt_blue_bold_border)  # D1
        ws.write(0, 4, "Time taken from Departure to Arrival", fmt_border)   # E1

        ws.write(1, 0, "Tracked")
        ws.write(2, 0, "Missed Milestone")
        ws.write(3, 0, "Untracked")
        ws.write(4, 0, "Grand Total", fmt_blue_bold_border)

        ws.write_number(1, 1, int(small_table.loc[0, "Shipment Count"]))
        ws.write_number(2, 1, int(small_table.loc[1, "Shipment Count"]))
        ws.write_number(3, 1, int(small_table.loc[2, "Shipment Count"]))
        ws.write_number(4, 1, int(small_table.loc[3, "Shipment Count"]))

        avg_days_all = small_table.loc[3, "Average of In-Transit Time (days)"]
        if pd.notna(avg_days_all):
            ws.write_number(4, 3, float(avg_days_all))
            ws.write_number(4, 4, float(avg_days_all))
        else:
            ws.write(4, 3, "")
            ws.write(4, 4, "")

        for r in range(1, 5):
            for c in range(0, 5):
                ws.write_blank(r, c, None, fmt_border)

        # ---- Main table starting row 7 ----
        start_row = 6
        headers = [
            "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
            "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country",
            "Average of In-Transit Time"
        ]
        for c, h in enumerate(headers):
            ws.write(start_row, c, h, fmt_blue_bold_border)

        for i, (_, row) in enumerate(summary_main.iterrows(), start=1):
            r = start_row + i
            for c_idx, col_name in enumerate(headers):
                val = row[col_name]
                if col_name == "Bill of Lading":
                    ws.write(r, c_idx, "" if pd.isna(val) else str(val), fmt_bold)
                elif col_name == "Average of In-Transit Time":
                    if pd.isna(val):
                        ws.write(r, c_idx, "")
                    else:
                        try:
                            ws.write_number(r, c_idx, float(val))
                        except Exception:
                            ws.write(r, c_idx, str(val))
                else:
                    ws.write(r, c_idx, "" if pd.isna(val) else str(val))

        last_data_row = start_row + len(summary_main) + 1
        ws.write(last_data_row, 0, "Grand Total", fmt_blue_bold_border)
        if len(summary_main) > 0:
            first_j = start_row + 1
            last_j = start_row + len(summary_main)
            ws.write_formula(last_data_row, 9, f"=AVERAGE(J{first_j+1}:J{last_j+1})", fmt_blue_bold_border)
        else:
            ws.write(last_data_row, 9, "", fmt_blue_bold_border)

        # Autosize Data sheet
        ws_data = writer.sheets.get("Data")
        if ws_data:
            for idx, col in enumerate(df_data.columns):
                width = min(50, max(12, int(df_data[col].astype(str).str.len().quantile(0.9)) + 2))
                ws_data.set_column(idx, idx, width)

    output.seek(0)
    return output

# ---------------------------------------------
# Streamlit workflow
# ---------------------------------------------
st.set_page_config(page_title="FTL In-Transit Builder", page_icon="🚚", layout="wide")
st.title("FTL In-Transit Time Processor (RAW → Data → Summary)")

mode_col, info_col = st.columns([1, 3])
with mode_col:
    mode = st.selectbox("Mode", options=["FTL"], index=0)
with info_col:
    st.caption("Upload your raw CSV or Excel. We build the Data sheet with your exact columns, add column V "
               "In-Transit Time (round: <.5 down, ≥.5 up), then generate the Summary sheet.")

uploaded = st.file_uploader("Upload RAW CSV or Excel", type=["csv", "xlsx", "xls"])

if uploaded is not None:
    try:
        df_raw = load_table(uploaded)
        df_data = build_data_from_raw(df_raw)

        # Compute V on the Data sheet
        df_data["In-Transit Time"] = df_data.apply(compute_in_transit_time_row, axis=1)

        # Summary
        summary_main, small_table = build_summary_sheet(df_data)

        # Full Excel (Data + Summary)
        excel_bytes = write_excel_with_formatting(df_data, summary_main, small_table)

        st.success("Processed! Preview below and use the download buttons.")

        with st.expander("Preview: Data (A..U + V)"):
            st.dataframe(df_data.head(50))
        with st.expander("Preview: Summary main table"):
            st.dataframe(summary_main.head(50))

        # Downloads
        data_csv = df_data.to_csv(index=False).encode("utf-8")
        summary_csv = summary_main.to_csv(index=False).encode("utf-8")

        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Download Data (CSV)", data=data_csv, file_name="Data_FTL.csv",
                               mime="text/csv", use_container_width=True)
            st.download_button("⬇️ Download Data (Excel)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        with c2:
            st.download_button("⬇️ Download Summary (CSV)", data=summary_csv, file_name="Summary_FTL.csv",
                               mime="text/csv", use_container_width=True)
            st.download_button("⬇️ Download Full Excel (Data + Summary)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    except Exception as e:
        st.error(f"Could not process this file. Details: {e}")
else:
    st.info("Upload your raw file to begin.")
