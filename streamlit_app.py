import io
import math
import re
import codecs
import csv
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------
# Helper functions
# ---------------------------------------------
def parse_timestamp(s):
    if pd.isna(s):
        return pd.NaT
    try:
        return pd.to_datetime(s, utc=True, errors="coerce")
    except Exception:
        return pd.NaT

def is_missing_like(x):
    if pd.isna(x):
        return True
    if isinstance(x, str):
        val = x.strip().lower()
        return val in {"", "na", "n/a", "null", "none"}
    return False

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
        v = value.strip().lower()
        return v in {"false", "no", "0"}
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

def compute_in_transit_time_row(row):
    tracked_val = row.get("Tracked", np.nan)
    milestones_received = row.get("Nb Milestones Received", np.nan)

    milestones_i = as_int_or_nan(milestones_received)
    untracked_condition = is_false_like(tracked_val) or (pd.isna(milestones_i) or milestones_i == 0)
    if untracked_condition:
        return "Untracked"

    pick_dep = parse_timestamp(row.get("Pickup Departure Utc Timestamp Raw", np.nan))
    drop_arr = parse_timestamp(row.get("Dropoff Arrival Utc Timestamp Raw", np.nan))
    if pd.isna(pick_dep) or pd.isna(drop_arr):
        return "Missing Milestone"

    delta_days = (drop_arr - pick_dep).total_seconds() / (24 * 3600)
    if delta_days <= 0:
        return "Missing Milestone"

    return int(round_half_up_days(delta_days))

# ---------- NEW: robust file loader ----------
def load_table(uploaded_file: "streamlit.UploadedFile") -> pd.DataFrame:
    """
    Robustly load CSV or Excel:
    - Detect Excel by extension or ZIP signature.
    - Try multiple encodings and delimiters for CSV.
    - Skip malformed lines instead of failing.
    """
    raw = uploaded_file.read()  # bytes
    name = (uploaded_file.name or "").lower()

    # If it's clearly Excel (by extension or ZIP signature)
    if name.endswith((".xlsx", ".xls")) or raw[:2] == b"PK":
        try:
            return pd.read_excel(io.BytesIO(raw))
        except Exception as e:
            # Fall through to CSV attempts if Excel parse fails
            pass

    # Heuristic: assemble encodings to try (BOM-aware)
    if raw.startswith(codecs.BOM_UTF16_LE) or raw.startswith(codecs.BOM_UTF16_BE):
        encodings = ["utf-16", "utf-16-le", "utf-16-be", "utf-8-sig", "utf-8", "cp1252", "latin-1"]
    else:
        encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"]

    # Primary attempt: sep=None (python engine) to sniff delimiter, skip bad lines
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            continue

    # Secondary: explicitly try tab
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep="\t", engine="python", on_bad_lines="skip")
        except Exception:
            continue

    # Last resort: try Excel again (some CSVs are actually XLSX bytes but renamed)
    try:
        return pd.read_excel(io.BytesIO(raw))
    except Exception as e:
        # Re-raise with helpful message for Streamlit
        raise RuntimeError(
            "Unable to parse the uploaded file as CSV or Excel. "
            "Please ensure it's a valid CSV (comma/semicolon/tab) or an .xlsx file."
        ) from e

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # Strip and collapse internal whitespace in headers
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df

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

    pick_city, pick_state = zip(*df_numeric.get("Pickup City State", pd.Series([], dtype=object)).map(split_city_state)) if len(df_numeric) else ([], [])
    drop_city, drop_state = zip(*df_numeric.get("Dropoff City State", pd.Series([], dtype=object)).map(split_city_state)) if len(df_numeric) else ([], [])

    summary_main = pd.DataFrame({
        "Bill of Lading": df_numeric.get("Bill of Lading", pd.Series(dtype=str)).astype(str),
        "Pickup Name": df_numeric.get("Pickup Name", pd.Series(dtype=str)).astype(str),
        "Pickup City": list(pick_city),
        "Pickup State": list(pick_state),
        "Pickup Country": df_numeric.get("Pickup Country", pd.Series(dtype=str)).astype(str),
        "Dropoff Name": df_numeric.get("Dropoff Name", pd.Series(dtype=str)).astype(str),
        "Dropoff City": list(drop_city),
        "Dropoff State": list(drop_state),
        "Dropoff Country": df_numeric.get("Dropoff Country", pd.Series(dtype=str)).astype(str),
        "Average of In-Transit Time": pd.to_numeric(df_numeric.get("In-Transit Time"), errors="coerce").astype("Int64"),
    })

    small_table = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [count_tracked, count_missing, count_untracked, grand_total],
        "Average of In-Transit Time (days)": [np.nan, np.nan, np.nan, avg_days_all],
        "Time taken from Departure to Arrival": [np.nan, np.nan, np.nan, avg_days_all],
    })

    return summary_main, small_table, {
        "count_tracked": count_tracked,
        "count_missing": count_missing,
        "count_untracked": count_untracked,
        "grand_total": grand_total,
        "avg_days_all": avg_days_all,
    }

def write_excel_with_formatting(df_data, summary_main, small_table):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_data.to_excel(writer, sheet_name="Data", index=False)

        wb = writer.book
        ws = wb.add_worksheet("Summary")

        fmt_header_blue_bold = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})
        fmt_bold = wb.add_format({"bold": True})
        fmt_border = wb.add_format({"border": 1})
        fmt_blue_bold_border = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})

        ws.set_column("A:A", 22)
        ws.set_column("B:B", 18)
        ws.set_column("C:C", 2)
        ws.set_column("D:D", 28)
        ws.set_column("E:E", 34)

        # Small table headers
        ws.write(0, 0, "Label", fmt_blue_bold_border)
        ws.write(0, 1, "Shipment Count", fmt_blue_bold_border)
        ws.write(0, 2, "", fmt_border)
        ws.write(0, 3, "Average of In-Transit Time", fmt_blue_bold_border)
        ws.write(0, 4, "Time taken from Departure to Arrival", fmt_border)

        # Labels A2..A4 + Grand Total A5
        ws.write(1, 0, "Tracked")
        ws.write(2, 0, "Missed Milestone")
        ws.write(3, 0, "Untracked")
        ws.write(4, 0, "Grand Total", fmt_blue_bold_border)

        # Counts B2..B5
        ws.write_number(1, 1, int(small_table.loc[0, "Shipment Count"]))
        ws.write_number(2, 1, int(small_table.loc[1, "Shipment Count"]))
        ws.write_number(3, 1, int(small_table.loc[2, "Shipment Count"]))
        ws.write_number(4, 1, int(small_table.loc[3, "Shipment Count"]))

        # D5/E5 averages
        avg_days_all = small_table.loc[3, "Average of In-Transit Time (days)"]
        if pd.notna(avg_days_all):
            ws.write_number(4, 3, float(avg_days_all))
            ws.write_number(4, 4, float(avg_days_all))
        else:
            ws.write(4, 3, "")
            ws.write(4, 4, "")

        # Add borders to the small area
        for r in range(1, 5):
            for c in range(0, 5):
                ws.write_blank(r, c, None, fmt_border)

        # Main table headers (row 7)
        start_row = 6
        headers = [
            "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
            "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country",
            "Average of In-Transit Time"
        ]
        for col_idx, h in enumerate(headers):
            ws.write(start_row, col_idx, h, fmt_blue_bold_border)

        # Main table data
        for i, (_, row) in enumerate(summary_main.iterrows(), start=1):
            r = start_row + i
            for c_idx, col_name in enumerate(headers):
                val = row[col_name]
                if col_name == "Bill of Lading":
                    ws.write(r, c_idx, "" if pd.isna(val) else str(val), wb.add_format({"bold": True}))
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

        # Grand Total row at the end
        last_data_row = start_row + len(summary_main) + 1
        ws.write(last_data_row, 0, "Grand Total", fmt_blue_bold_border)
        if len(summary_main) > 0:
            first_j = start_row + 1
            last_j = start_row + len(summary_main)
            ws.write_formula(last_data_row, 9, f"=AVERAGE(J{first_j+1}:J{last_j+1})", fmt_blue_bold_border)
        else:
            ws.write(last_data_row, 9, "", fmt_blue_bold_border)

        # Autosize Data sheet columns
        ws_data = writer.sheets["Data"]
        for idx, col in enumerate(df_data.columns):
            width = min(50, max(12, int(df_data[col].astype(str).str.len().quantile(0.9)) + 2))
            ws_data.set_column(idx, idx, width)

    output.seek(0)
    return output

# ---------- UPDATED: process any uploaded file ----------
def process_uploaded_file(uploaded_file, selected_mode="FTL"):
    df = load_table(uploaded_file)
    df = normalize_headers(df)

    # Expected headers (we proceed even if some are missing)
    required_cols = [
        "Carrier Name", "Bill of Lading", "Tracked", "Pickup Name", "Pickup City State", "Pickup Country",
        "Dropoff Name", "Dropoff City State", "Dropoff Country", "Final Status Reason",
        "Pickup Arrival Utc Timestamp Raw", "Pickup Departure Utc Timestamp Raw",
        "Dropoff Arrival Utc Timestamp Raw", "Dropoff Departure Utc Timestamp Raw",
        "Nb Milestones Expected", "Nb Milestones Received", "Milestones Achieved Percentage",
        "Latency Updates Received", "Latency Updates Passed", "Shipment Latency Percentage",
        "Average Latency (min)"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.warning(f"Missing expected columns (continuing anyway): {missing}")

    df["In-Transit Time"] = df.apply(compute_in_transit_time_row, axis=1)

    summary_main, small_table, stats = build_summary_sheet(df)
    excel_bytes = write_excel_with_formatting(df, summary_main, small_table)
    return df, summary_main, excel_bytes

# ---------------------------------------------
# Streamlit UI
# ---------------------------------------------
st.set_page_config(page_title="FTL In-Transit Builder", page_icon="🚚", layout="wide")
st.title("Integris Report")

col_mode, col_info = st.columns([1, 3])
with col_mode:
    mode = st.selectbox("Mode", options=["FTL"], index=0, help="More modes (Ocean, Air, Parcel, LTL) coming next.")
with col_info:
    st.caption("Upload CSV or Excel. Rounding: `< .5` down, `≥ .5` up. City/State split on first hyphen with trimming.")

uploaded = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx", "xls"])

if uploaded is not None:
    try:
        df_data, summary_main, excel_bytes = process_uploaded_file(uploaded, selected_mode=mode)
        st.success("Processing complete.")

        with st.expander("Preview: Data (with In-Transit Time)"):
            st.dataframe(df_data.head(50))
        with st.expander("Preview: Summary main table"):
            st.dataframe(summary_main.head(50))

        data_csv = df_data.to_csv(index=False).encode("utf-8")
        summary_csv = summary_main.to_csv(index=False).encode("utf-8")

        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            st.download_button("⬇️ Download Data (CSV)", data=data_csv, file_name="Data_FTL.csv", mime="text/csv", use_container_width=True)
            st.download_button("⬇️ Download Data (Excel)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        with dl_col2:
            st.download_button("⬇️ Download Summary (CSV)", data=summary_csv, file_name="Summary_FTL.csv", mime="text/csv", use_container_width=True)
            st.download_button("⬇️ Download Full Excel (Data + Summary)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    except Exception as e:
        st.error(f"Sorry—couldn’t parse this file as CSV or Excel. Details: {e}")
else:
    st.info("Please upload your raw CSV or Excel to begin.")
