import io
import math
import re
import sys
import subprocess
import numpy as np
import pandas as pd
import streamlit as st

# --------------------------------------------------
# Tiny installer. We'll try to install missing deps at runtime (Streamlit Cloud supports this).
# --------------------------------------------------
def _ensure_pkg(pkg_name, spec=None):
    """
    Ensure a package is importable; if not, pip install it.
    Returns True if importable after, else False.
    """
    try:
        __import__(pkg_name)
        return True
    except Exception:
        pass
    try:
        # Show a small message in the UI while installing
        with st.spinner(f"Installing dependency: {pkg_name}…"):
            cmd = [sys.executable, "-m", "pip", "install", spec or pkg_name]
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        __import__(pkg_name)
        return True
    except Exception:
        return False

@st.cache_resource(show_spinner=False)
def ensure_excel_engine_for_write() -> str:
    """
    Prefer xlsxwriter for better styling. If not available, ensure openpyxl.
    Returns the engine name we will use for writing: 'xlsxwriter' or 'openpyxl'.
    """
    if _ensure_pkg("xlsxwriter", "xlsxwriter>=3.2.0"):
        return "xlsxwriter"
    if _ensure_pkg("openpyxl", "openpyxl>=3.1.5"):
        return "openpyxl"
    # If both fail, we can't write Excel; we'll handle that later in UI.
    return ""

@st.cache_resource(show_spinner=False)
def ensure_excel_reader():
    """
    Ensure we can read .xlsx. Pandas prefers openpyxl for .xlsx.
    """
    _ensure_pkg("openpyxl", "openpyxl>=3.1.5")


# --------------------------------------------------
# Header / text helpers
# --------------------------------------------------
def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Drop empty/unnamed columns
    keep_cols = []
    for c in df.columns:
        s = str(c).strip()
        if s and not s.lower().startswith("unnamed"):
            keep_cols.append(c)
    df = df.loc[:, keep_cols]
    # Clean spaces
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


# --------------------------------------------------
# Robust loader (CSV / Excel)
# --------------------------------------------------
def load_table(uploaded_file) -> pd.DataFrame:
    raw = uploaded_file.read()
    name = (uploaded_file.name or "").lower()

    # Excel by extension or ZIP signature
    if name.endswith((".xlsx", ".xls")) or raw[:2] == b"PK":
        try:
            ensure_excel_reader()
            return pd.read_excel(io.BytesIO(raw))
        except Exception:
            # fall through to CSV attempts
            pass

    # Try CSV with multiple encodings + sniffer
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"]
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            continue

    # Last resort try Excel again (some CSVs are disguised xlsx)
    ensure_excel_reader()
    return pd.read_excel(io.BytesIO(raw))


# --------------------------------------------------
# RAW → Data mapping (A..U)
# --------------------------------------------------
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

def parse_received_expected(series_ratio: pd.Series):
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

    data = data[DATA_COLUMNS_ORDER]
    return data


# --------------------------------------------------
# V column logic + Summary
# --------------------------------------------------
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

    small_table = pd.DataFrame({
        "Label": ["Tracked", "Missed Milestone", "Untracked", "Grand Total"],
        "Shipment Count": [count_tracked, count_missing, count_untracked, grand_total],
        "Average of In-Transit Time (days)": [np.nan, np.nan, np.nan, avg_days_all],
        "Time taken from Departure to Arrival": [np.nan, np.nan, np.nan, avg_days_all],
    })

    return summary_main, small_table


# --------------------------------------------------
# Excel writer (uses whichever engine we have)
# --------------------------------------------------
def write_excel_with_formatting(df_data, summary_main, small_table):
    engine = ensure_excel_engine_for_write()
    if engine == "xlsxwriter":
        return _write_with_xlsxwriter(df_data, summary_main, small_table)
    elif engine == "openpyxl":
        return _write_with_openpyxl(df_data, summary_main, small_table)
    else:
        # No engine available (install failed). We’ll raise and let UI show a friendly note.
        raise RuntimeError("Neither 'xlsxwriter' nor 'openpyxl' are available to write Excel.")

def _write_with_xlsxwriter(df_data, summary_main, small_table):
    import xlsxwriter  # noqa: F401
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_data.to_excel(writer, sheet_name="Data", index=False)

        wb = writer.book
        ws = wb.add_worksheet("Summary")

        fmt_header = wb.add_format({"bold": True, "bg_color": "#D9EDF7", "border": 1})
        fmt_bold = wb.add_format({"bold": True})
        fmt_border = wb.add_format({"border": 1})

        # Column widths (Summary)
        ws.set_column("A:A", 22)
        ws.set_column("B:B", 18)
        ws.set_column("C:C", 2)   # blank separator
        ws.set_column("D:D", 28)
        ws.set_column("E:E", 34)
        ws.set_column("F:J", 22)

        # Small table rows 1–5
        ws.write(0, 0, "Label", fmt_header)
        ws.write(0, 1, "Shipment Count", fmt_header)
        ws.write(0, 2, "", fmt_border)
        ws.write(0, 3, "Average of In-Transit Time", fmt_header)
        ws.write(0, 4, "Time taken from Departure to Arrival", fmt_border)

        ws.write(1, 0, "Tracked")
        ws.write(2, 0, "Missed Milestone")
        ws.write(3, 0, "Untracked")
        ws.write(4, 0, "Grand Total", fmt_header)

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

        # Borders on A2:E5
        for r in range(1, 5):
            for c in range(0, 5):
                ws.write_blank(r, c, None, fmt_border)

        # Main table starting row 7
        start_row = 6
        headers = [
            "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
            "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country",
            "Average of In-Transit Time"
        ]
        for c, h in enumerate(headers):
            ws.write(start_row, c, h, fmt_header)

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
        ws.write(last_data_row, 0, "Grand Total", fmt_header)
        if len(summary_main) > 0:
            first_j = start_row + 1
            last_j = start_row + len(summary_main)
            ws.write_formula(last_data_row, 9, f"=AVERAGE(J{first_j+1}:J{last_j+1})", fmt_header)
        else:
            ws.write(last_data_row, 9, "", fmt_header)

        # Autosize Data sheet columns
        ws_data = writer.sheets["Data"]
        for idx, col in enumerate(df_data.columns):
            width = min(50, max(12, int(df_data[col].astype(str).str.len().quantile(0.9)) + 2))
            ws_data.set_column(idx, idx, width)

    output.seek(0)
    return output

def _write_with_openpyxl(df_data, summary_main, small_table):
    # openpyxl path
    _ = _ensure_pkg("openpyxl", "openpyxl>=3.1.5")
    from openpyxl.styles import Font, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_data.to_excel(writer, sheet_name="Data", index=False)
        wb = writer.book
        ws = wb.create_sheet("Summary")

        blue_fill = PatternFill(fill_type="solid", start_color="FFD9EDF7", end_color="FFD9EDF7")
        bold_font = Font(bold=True)
        thin = Side(style="thin", color="FF000000")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)

        def set_cell(r, c, value, blue=False, bold=False, bordered=False):
            cell = ws.cell(row=r, column=c, value=value)
            if blue:
                cell.fill = blue_fill
            if bold:
                cell.font = Font(bold=True)
            if bordered:
                cell.border = border
            return cell

        # Set column widths
        widths = {1: 22, 2: 18, 3: 2, 4: 28, 5: 34, 6: 22, 7: 22, 8: 18, 9: 18, 10: 22}
        for col_idx, w in widths.items():
            ws.column_dimensions[get_column_letter(col_idx)].width = w

        # Small table headers and rows
        set_cell(1, 1, "Label", blue=True, bold=True, bordered=True)
        set_cell(1, 2, "Shipment Count", blue=True, bold=True, bordered=True)
        set_cell(1, 3, "", bordered=True)
        set_cell(1, 4, "Average of In-Transit Time", blue=True, bold=True, bordered=True)
        set_cell(1, 5, "Time taken from Departure to Arrival", bordered=True)

        set_cell(2, 1, "Tracked")
        set_cell(3, 1, "Missed Milestone")
        set_cell(4, 1, "Untracked")
        set_cell(5, 1, "Grand Total", blue=True, bold=True, bordered=True)

        set_cell(2, 2, int(small_table.loc[0, "Shipment Count"]))
        set_cell(3, 2, int(small_table.loc[1, "Shipment Count"]))
        set_cell(4, 2, int(small_table.loc[2, "Shipment Count"]))
        set_cell(5, 2, int(small_table.loc[3, "Shipment Count"]))

        avg_days_all = small_table.loc[3, "Average of In-Transit Time (days)"]
        set_cell(5, 4, float(avg_days_all) if pd.notna(avg_days_all) else "")
        set_cell(5, 5, float(avg_days_all) if pd.notna(avg_days_all) else "")

        # borders for the small area A2:E5
        for r in range(2, 6):
            for c in range(1, 6):
                ws.cell(row=r, column=c).border = border

        # Main table
        header_row = 7
        headers = [
            "Bill of Lading", "Pickup Name", "Pickup City", "Pickup State", "Pickup Country",
            "Dropoff Name", "Dropoff City", "Dropoff State", "Dropoff Country",
            "Average of In-Transit Time"
        ]
        for c, h in enumerate(headers, start=1):
            set_cell(header_row, c, h, blue=True, bold=True, bordered=True)

        for i, (_, row) in enumerate(summary_main.iterrows(), start=1):
            r = header_row + i
            for c_idx, col_name in enumerate(headers, start=1):
                val = row[col_name]
                cell = ws.cell(row=r, column=c_idx, value=("" if pd.isna(val) else str(val)))
                if col_name == "Bill of Lading":
                    cell.font = bold_font
                if col_name == "Average of In-Transit Time":
                    if pd.isna(val):
                        cell.value = ""
                    else:
                        try:
                            cell.value = float(val)
                        except Exception:
                            pass

        # Grand total row
        last_data_row = header_row + len(summary_main) + 1
        set_cell(last_data_row, 1, "Grand Total", blue=True, bold=True, bordered=True)
        if len(summary_main) > 0:
            first_j = header_row + 1
            last_j = header_row + len(summary_main)
            ws.cell(row=last_data_row, column=10).value = f"=AVERAGE(J{first_j}:J{last_j})"
            cell = ws.cell(row=last_data_row, column=10)
            cell.fill = blue_fill
            cell.font = bold_font
            cell.border = border
        else:
            set_cell(last_data_row, 10, "", blue=True, bold=True, bordered=True)

        # Autosize-ish Data sheet columns
        ws_data = writer.sheets.get("Data")
        if ws_data is not None:
            df_cols = list(df_data.columns)
            from openpyxl.utils import get_column_letter as _gcl
            for idx, col in enumerate(df_cols, start=1):
                lens = df_data[col].astype(str).str.len()
                q = int(lens.quantile(0.9)) if len(lens) else 12
                width = min(50, max(12, q + 2))
                ws_data.column_dimensions[_gcl(idx)].width = width

    output.seek(0)
    return output


# --------------------------------------------------
# Streamlit UI and flow
# --------------------------------------------------
st.set_page_config(page_title="FTL In-Transit Builder", page_icon="🚚", layout="wide")
st.title("FTL In-Transit Time Processor (RAW → Data → Summary)")

mode_col, info_col = st.columns([1, 3])
with mode_col:
    mode = st.selectbox("Mode", options=["FTL"], index=0)
with info_col:
    eng = ensure_excel_engine_for_write()
    engine_msg = eng if eng else "none (will try to install when exporting)"
    st.caption("Upload your RAW CSV/XLSX. We build the Data sheet (A..U), add V=In-Transit Time "
               "(round: <.5 down, ≥.5 up), then generate the Summary sheet. "
               f"Excel writer engine: **{engine_msg}**")

uploaded = st.file_uploader("Upload RAW CSV or Excel", type=["csv", "xlsx", "xls"])

if uploaded is not None:
    try:
        df_raw = load_table(uploaded)
        df_data = build_data_from_raw(df_raw)

        # Compute V on the Data sheet
        df_data["In-Transit Time"] = df_data.apply(compute_in_transit_time_row, axis=1)

        # Summary
        summary_main, small_table = build_summary_sheet(df_data)

        # Full Excel (Data + Summary), with engine fallback
        try:
            excel_bytes = write_excel_with_formatting(df_data, summary_main, small_table)
            excel_ok = True
        except Exception as ex:
            excel_ok = False
            st.warning(f"Excel export engine unavailable. You can still download CSVs. Details: {ex}")

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
            if excel_ok:
                st.download_button("⬇️ Download Data (Excel)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        with c2:
            st.download_button("⬇️ Download Summary (CSV)", data=summary_csv, file_name="Summary_FTL.csv",
                               mime="text/csv", use_container_width=True)
            if excel_ok:
                st.download_button("⬇️ Download Full Excel (Data + Summary)", data=excel_bytes, file_name="Data_and_Summary_FTL.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

    except Exception as e:
        st.error(f"Could not process this file. Details: {e}")
else:
    st.info("Upload your raw file to begin.")
