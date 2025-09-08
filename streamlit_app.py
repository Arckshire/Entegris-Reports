import io
import math
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------
# Helper functions
# ---------------------------------------------
def parse_timestamp(s):
    """
    Robust UTC timestamp parser. Accepts strings or numbers.
    Returns pandas.Timestamp (utc) or NaT.
    """
    if pd.isna(s):
        return pd.NaT
    try:
        # Let pandas handle most formats. Force UTC when possible.
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        return ts
    except Exception:
        return pd.NaT

def is_missing_like(x):
    """
    Returns True if x should be treated as missing for logic purposes.
    Interprets: NaN/None/empty-string/'NA'/'N/A'/'NULL'/'Null'/'None'.
    """
    if pd.isna(x):
        return True
    if isinstance(x, str):
        val = x.strip().lower()
        return val in {"", "na", "n/a", "null", "none"}
    return False

def as_int_or_nan(x):
    """
    Try to coerce milestones to integer; return np.nan if not valid.
    """
    if is_missing_like(x):
        return np.nan
    try:
        return int(float(str(x).strip()))
    except Exception:
        return np.nan

def is_false_like(value):
    """
    Interpret 'Tracked' FALSE semantics.
    """
    if isinstance(value, bool):
        return value is False
    if is_missing_like(value):
        return False  # not false, just missing; we won't mark untracked solely due to missing Tracked
    if isinstance(value, (int, float)):
        # Treat 0 as False, 1 as True
        return int(value) == 0
    if isinstance(value, str):
        v = value.strip().lower()
        return v in {"false", "no", "0"}
    return False

def round_half_up_days(days_float):
    """
    Custom rounding rule:
    - < .5 -> round down
    - >= .5 -> round up
    """
    # handle negatives explicitly
    if pd.isna(days_float):
        return np.nan
    return math.floor(days_float + 0.5)

def split_city_state(value):
    """
    Split 'City - State' or 'City-State' into (city, state),
    trimming whitespace around both sides.
    If hyphen not present, city=value, state="".
    """
    if is_missing_like(value):
        return "", ""
    txt = str(value)
    # Split on the first hyphen
    parts = re.split(r"\s*-\s*", txt, maxsplit=1)
    if len(parts) == 1:
        # try bare hyphen
        parts = txt.split("-", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return txt.strip(), ""

def compute_in_transit_time_row(row):
    """
    Implements your V-column logic:

    - If Tracked == FALSE OR Nb Milestones Received missing/0/NA -> "Untracked"
    - Else compute days = Dropoff Arrival - Pickup Departure (in days)
        * If either timestamp missing OR result <= 0 -> "Missing Milestone"
        * Else round-half-up to integer days; put the integer
    """
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

    # Compute difference in days (float)
    delta = (drop_arr - pick_dep).total_seconds() / (24 * 3600)

    if delta <= 0:
        return "Missing Milestone"

    return int(round_half_up_days(delta))

def build_summary_sheet(df_data):
    """
    Returns:
      summary_df_main: the main table (row 7 onward in Excel)
      small_table: the small table (rows 1-5 conceptual data)
      counts dict & averages for convenience
    """
    # Count logic from V
    v = df_data["In-Transit Time"]
    is_numeric_v = pd.to_numeric(v, errors="coerce").notna()
    count_tracked = int(is_numeric_v.sum())
    count_missing = int((v == "Missing Milestone").sum())
    count_untracked = int((v == "Untracked").sum())
    grand_total = count_tracked + count_missing + count_untracked

    # Average of numeric V (in days)
    numeric_vals = pd.to_numeric(v, errors="coerce")
    avg_days_all = float(numeric_vals.dropna().mean()) if numeric_vals.notna().any() else np.nan

    # Filter rows for main table: only numeric V
    df_numeric = df_data[is_numeric_v].copy()

    # Create columns per spec
    # Summary main columns:
    # A Bill of Lading (bold)         -> Data['Bill of Lading']
    # B Pickup Name                   -> Data['Pickup Name']
    # C Pickup City                   -> from 'Pickup City State' (city part)
    # D Pickup State                  -> from 'Pickup City State' (state part)
    # E Pickup Country                -> Data['Pickup Country']
    # F Dropoff Name                  -> Data['Dropoff Name']
    # G Dropoff City                  -> from 'Dropoff City State'
    # H Dropoff State                 -> from 'Dropoff City State'
    # I Dropoff Country               -> Data['Dropoff Country']
    # J Average of In-Transit Time    -> Data['In-Transit Time'] (numeric V)
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

    # small summary “top” table (conceptual; we will format into the Excel layout):
    small_table = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [count_tracked, count_missing, count_untracked, grand_total],
        "Average of In-Transit Time (days)": [np.nan, np.nan, np.nan, avg_days_all],
        "Time taken from Departure to Arrival": [np.nan, np.nan, np.nan, avg_days_all],  # duplicating average unless you prefer a different metric
    })

    return summary_main, small_table, {
        "count_tracked": count_tracked,
        "count_missing": count_missing,
        "count_untracked": count_untracked,
        "grand_total": grand_total,
        "avg_days_all": avg_days_all,
    }

def write_excel_with_formatting(df_data, summary_main, small_table):
    """
    Create an Excel file in-memory with:
      - Sheet 'Data' (df_data)
      - Sheet 'Summary' with required layout and formatting
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Write 'Data' sheet
        df_data.to_excel(writer, sheet_name="Data", index=False)

        # Prepare 'Summary' sheet with custom layout
        wb = writer.book
        ws = wb.add_worksheet("Summary")

        # Formats
        fmt_header_blue_bold = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})
        fmt_bold = wb.add_format({"bold": True})
        fmt_blue_bold = wb.add_format({"bold": True, "bg_color": "#D9EDF7"})
        fmt_border = wb.add_format({"border": 1})
        fmt_blue_bold_border = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})

        # Column widths for readability
        ws.set_column("A:A", 22)
        ws.set_column("B:B", 18)
        ws.set_column("C:C", 2)    # blank separator column
        ws.set_column("D:D", 28)
        ws.set_column("E:E", 34)

        # ---- Small table (Rows 1–5) ----
        # Headers: A1=Label, B1=Shipment Count, C1 blank, D1=Average of In-Transit Time, E1=Time taken from Departure to Arrival
        ws.write(0, 0, "Label", fmt_blue_bold_border)            # A1
        ws.write(0, 1, "Shipment Count", fmt_blue_bold_border)   # B1
        ws.write(0, 2, "", fmt_border)                           # C1 (blank)
        ws.write(0, 3, "Average of In-Transit Time", fmt_blue_bold_border)  # D1 (explicitly blue+bold per spec)
        ws.write(0, 4, "Time taken from Departure to Arrival", fmt_border)   # E1 (normal unless you want also blue)

        # Rows A2..A4 labels as provided; A5/B5 Grand Total with blue+bold
        # A2 Tracked, A3 Missed Milestone (label differs), A4 Untracked
        ws.write(1, 0, "Tracked")
        ws.write(2, 0, "Missed Milestone")  # label as requested in Summary
        ws.write(3, 0, "Untracked")
        ws.write(4, 0, "Grand Total", fmt_blue_bold_border)

        # Shipment counts from small_table (B2..B5)
        ws.write_number(1, 1, int(small_table.loc[0, "Shipment Count"]))
        ws.write_number(2, 1, int(small_table.loc[1, "Shipment Count"]))
        ws.write_number(3, 1, int(small_table.loc[2, "Shipment Count"]))
        ws.write_number(4, 1, int(small_table.loc[3, "Shipment Count"]))  # Grand Total

        # Average in-transit time overall at D5
        avg_days_all = small_table.loc[3, "Average of In-Transit Time (days)"]
        if pd.notna(avg_days_all):
            ws.write_number(4, 3, float(avg_days_all))
        else:
            ws.write(4, 3, "")

        # Optionally mirror into E5 (same metric, unless you decide a different one later)
        if pd.notna(avg_days_all):
            ws.write_number(4, 4, float(avg_days_all))
        else:
            ws.write(4, 4, "")

        # Grid borders for small table area
        for r in range(1, 5):
            ws.write_blank(r, 0, None, fmt_border)
            ws.write_blank(r, 1, None, fmt_border)
            ws.write_blank(r, 2, None, fmt_border)
            ws.write_blank(r, 3, None, fmt_border)
            ws.write_blank(r, 4, None, fmt_border)

        # ---- Main table starting row 7 (index 6) ----
        start_row = 6  # 0-indexed -> row 7 in Excel
        headers = [
            "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
            "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country",
            "Average of In-Transit Time"
        ]
        for col_idx, h in enumerate(headers):
            ws.write(start_row, col_idx, h, fmt_blue_bold_border)

        # Write data rows
        for i, (_, row) in enumerate(summary_main.iterrows(), start=1):
            r = start_row + i
            for c_idx, col_name in enumerate(headers):
                val = row[col_name]
                if col_name == "Bill of Lading":
                    # Bold Bill of Lading values
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

        # Grand Total row after data
        last_data_row = start_row + len(summary_main) + 1  # +1 because we started at start_row+1 for first data
        ws.write(last_data_row, 0, "Grand Total", fmt_blue_bold_border)
        # Average of J column (Average of In-Transit Time)
        if len(summary_main) > 0:
            # Excel formula for average of J column data region
            first_j = start_row + 1
            last_j = start_row + len(summary_main)
            formula = f"=AVERAGE(J{first_j+1}:J{last_j+1})"
            ws.write_formula(last_data_row, 9, formula, fmt_blue_bold_border)
        else:
            ws.write(last_data_row, 9, "", fmt_blue_bold_border)

        # Write a plain Summary sheet as DataFrame as well (hidden): optional, but not needed.

        # Save Data sheet already written by pandas; apply some column widths for readability
        ws_data = writer.sheets["Data"]
        for idx, col in enumerate(df_data.columns):
            # autosize-ish
            width = min(50, max(12, int(df_data[col].astype(str).str.len().quantile(0.9)) + 2))
            ws_data.set_column(idx, idx, width)

    output.seek(0)
    return output

def process_uploaded_csv(uploaded_csv, selected_mode="FTL"):
    """
    Reads CSV into DataFrame, applies V-column logic, builds Summary.
    Returns processed Data (df_data), Summary main table (summary_main), and BytesIO Excel.
    """
    # Read CSV (assume utf-8; fall back to latin-1 if needed)
    try:
        df = pd.read_csv(uploaded_csv)
    except UnicodeDecodeError:
        uploaded_csv.seek(0)
        df = pd.read_csv(uploaded_csv, encoding="latin-1")

    # Ensure required columns exist
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
        st.warning(f"These columns are missing from your CSV and are expected: {missing}. "
                   f"I'll proceed where possible; please verify your headers.")
        # We still proceed. Any missing column references will yield NaN via .get()

    # Compute column V = In-Transit Time
    df["In-Transit Time"] = df.apply(compute_in_transit_time_row, axis=1)

    # Build Summary sheet content
    summary_main, small_table, stats = build_summary_sheet(df)

    # Build Excel (Data + Summary with formatting)
    excel_bytes = write_excel_with_formatting(df, summary_main, small_table)

    return df, summary_main, excel_bytes

# ---------------------------------------------
# Streamlit UI
# ---------------------------------------------
st.set_page_config(page_title="FTL In-Transit Builder", page_icon="🚚", layout="wide")

st.title("FTL In-Transit Time Processor")
st.write("Upload your **FTL** raw CSV, and I’ll add the **In-Transit Time** column (V) and create a **Summary** sheet per your rules.")

col_mode, col_info = st.columns([1, 3])
with col_mode:
    mode = st.selectbox("Mode", options=["FTL"], index=0, help="More modes (Ocean, Air, Parcel, LTL) coming next.")
with col_info:
    st.caption("Rounding rule: values `< .5` round down; `≥ .5` round up. "
               "City/State are split on the first hyphen and trimmed.")

uploaded = st.file_uploader("Upload CSV (raw export)", type=["csv"])

if uploaded is not None:
    df_data, summary_main, excel_bytes = process_uploaded_csv(uploaded, selected_mode=mode)

    st.success("Processing complete.")

    # Preview
    with st.expander("Preview: Data (with In-Transit Time)"):
        st.dataframe(df_data.head(50))
    with st.expander("Preview: Summary main table"):
        st.dataframe(summary_main.head(50))

    # Build CSVs for download
    data_csv = df_data.to_csv(index=False).encode("utf-8")
    summary_csv = summary_main.to_csv(index=False).encode("utf-8")

    # --- Download buttons ---
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        st.download_button(
            label="⬇️ Download Data (CSV)",
            data=data_csv,
            file_name="Data_FTL.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.download_button(
            label="⬇️ Download Data (Excel)",
            data=excel_bytes,  # contains both sheets but this button is labeled Data; keep for parity
            file_name="Data_and_Summary_FTL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with dl_col2:
        st.download_button(
            label="⬇️ Download Summary (CSV)",
            data=summary_csv,
            file_name="Summary_FTL.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.download_button(
            label="⬇️ Download Full Excel (Data + Summary)",
            data=excel_bytes,
            file_name="Data_and_Summary_FTL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
else:
    st.info("Please upload your raw CSV to begin.")
