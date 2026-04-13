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
    return ""

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
    return math.floor(x + 0.5)

def split_city_state(text: str):
    if is_missing_like(text): return "", ""
    s = str(text).strip()
    parts = re.split(r"\s*-\s*", s, maxsplit=1)
    if len(parts) == 2: return parts[0].strip(), parts[1].strip()
    m = re.match(r"^(.*?)[\s,]+([A-Z]{2})$", s)
    if m: return m.group(1).strip(), m.group(2).strip()
    return s, ""

def _parse_ts_zero_ok(x):
    if pd.isna(x): return pd.NaT
    if isinstance(x, str) and x.strip() == "0": return pd.NaT
    if isinstance(x, (int, float)) and float(x) == 0.0: return pd.NaT
    return pd.to_datetime(x, utc=True, errors="coerce")

# -----------------------------
# Loader
# -----------------------------
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

# =============================
# FTL/TL
# =============================
FTL_RAW_MAP = {
    "bol": "Bill of Lading",
    "tracked": "Tracked",
    "p_name": "Pickup Name",
    "p_city_state": "Pickup City State",
    "p_country": "Pickup Country",
    "d_name": "Final Destination Name",
    "d_city_state": "Final Destination City State",
    "d_country": "Final Destination Country",
    "pickup_departure_utc": "Pickup Departure Milestone (UTC)",
    "dropoff_arrival_utc": "Final Destination Arrival Milestone (UTC)",
}

FTL_DATA_COLUMNS = [
    "Tenant Name", "Carrier Name", "Carrier Identifier Selection", "SCAC", "Bill of Lading", 
    "Order Number", "Tracked", "Tracking Type", "Tracking Method", "Active Equipment ID", 
    "Historical Equipment ID", "Pickup Name", "Pickup City State", "Pickup Country", "Pickup Region",
    "Dropoff Name", "Dropoff City State", "Dropoff Country", "Dropoff Country Region", 
    "Final Status Reason", "Created Timestamp Date", "Pickup Arrival Utc Timestamp Raw",
    "Pickup Departure Utc Timestamp Raw", "Dropoff Arrival Utc Timestamp Raw", 
    "Dropoff Departure Utc Timestamp Raw", "Transit Time", "Nb Milestones Expected",
    "Nb Milestones Received", "Milestones Achieved Percentage", "Latency Updates Received",
    "Latency Updates Passed", "Shipment Latency Percentage", "Average Latency (min)",
    "Period Date", "Ping Interval (min)", "Attr1 Name", "Attr1 Value", "Attr2 Name",
    "Attr2 Value", "Attr3 Name", "Attr3 Value", "Attr4 Name", "Attr4 Value", "Attr5 Name", "Attr5 Value"
]

def build_ftl_tables(df_raw: pd.DataFrame):
    """Returns (summary_df, detail_df, data_df) where data_df has exact column structure"""
    df = normalize_headers(df_raw).copy()
    for _, col in FTL_RAW_MAP.items():
        if col not in df.columns: df[col] = np.nan

    trk   = df[FTL_RAW_MAP["tracked"]]
    dep   = df[FTL_RAW_MAP["pickup_departure_utc"]]
    arr   = df[FTL_RAW_MAP["dropoff_arrival_utc"]]

    dep_ts = dep.apply(parse_timestamp_utc)
    arr_ts = arr.apply(parse_timestamp_utc)

    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    is_untracked     = trk.apply(is_false_like)
    is_tracked_true  = trk.apply(is_true_like)
    is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
    is_tracked_good  = (~is_untracked) & is_tracked_true &  valid_transit.fillna(False)

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
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        "Time taken from Departure to Arrival": ["", "", "", ""],
    })

    # Build main detail table
    rows = df[is_tracked_good].copy().reset_index(drop=True)
    if len(rows):
        p_city, p_state = zip(*rows[FTL_RAW_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[FTL_RAW_MAP["d_city_state"]].astype(str).map(split_city_state))
    else:
        p_city, p_state, d_city, d_state = ([], [], [], [])

    avg_days_col = (
        pd.to_numeric(tracked_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
        if cnt_tracked > 0 else pd.Series([], dtype="Int64")
    )

    main = pd.DataFrame({
        "Bill of Lading": rows[FTL_RAW_MAP["bol"]].astype(str).str.strip(),
        "Pickup Name": rows[FTL_RAW_MAP["p_name"]].astype(str).str.strip(),
        "Pickup City": list(p_city),
        "Pickup State": list(p_state),
        "Pickup Country": rows[FTL_RAW_MAP["p_country"]].astype(str).str.strip(),
        "Dropoff Name": rows[FTL_RAW_MAP["d_name"]].astype(str).str.strip(),
        "Dropoff City": list(d_city),
        "Dropoff State": list(d_state),
        "Dropoff Country": rows[FTL_RAW_MAP["d_country"]].astype(str).str.strip(),
        "Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
    total_row = {col: "" for col in main.columns}
    total_row["Bill of Lading"] = "Grand Total"
    total_row["Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # DATA sheet: exact column structure
    data_df = pd.DataFrame()
    for col in FTL_DATA_COLUMNS:
        if col == "Transit Time":
            data_df[col] = in_transit_days
        elif col in df.columns:
            data_df[col] = df[col]
        else:
            data_df[col] = np.nan

    return small, main, data_df

# =============================
# LTL
# =============================
LTL_MAP = {
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
}

LTL_DATA_COLUMNS = [
    "Carrier Name", "Tenant Name", "Bill of Lading", "Bill of Lading Source", 
    "Bill of Lading Secondary", "Bill of Lading Secondary Source", "Order Number", 
    "PRO number", "PRO number source", "PRO number secondary", "PRO number secondary source",
    "Purchase Order", "Purchase Order Source", "Purchase Order Secondary Source", 
    "Purchase Order Secondary", "Pickup Number", "Customer Reference", "Tracking Number",
    "Tracked", "Pickup City State", "Pickup Name", "Pickup Region", "Pickup Country",
    "Destination Name", "Dropoff City State", "Dropoff Country Region", "Dropoff Country",
    "Pickup Utc Timestamp Time", "Pickup Utc Retrieval Timestamp Time", 
    "Out for Delivery Utc Timestamp Time", "Out for Delivery Utc Retrieval Timestamp Time",
    "Delivered Utc Timestamp Time", "Delivered Utc Retrieval Timestamp Time", "Transit Time",
    "Has All Milestones (Yes / No)", "Nb Milestones Expected", "Nb Milestones Received",
    "Latency Updates Received", "Latency Updates Passed", "Average Latency (hour)", 
    "Final Status Reason"
]

def build_ltl_tables(df_raw: pd.DataFrame):
    """Returns (summary_df, detail_df, data_df)"""
    df = normalize_headers(df_raw).copy()
    for col in [
        LTL_MAP["pro"], LTL_MAP["tracked"], LTL_MAP["p_name"], LTL_MAP["p_city_state"], LTL_MAP["p_region"],
        LTL_MAP["dest_name"], LTL_MAP["d_city_state"], LTL_MAP["d_region"], LTL_MAP["start_ts"], LTL_MAP["end_ts"]
    ]:
        if col not in df.columns: df[col] = np.nan

    trk = df[LTL_MAP["tracked"]]
    dep_ts = df[LTL_MAP["start_ts"]].apply(parse_timestamp_utc)
    arr_ts = df[LTL_MAP["end_ts"]].apply(parse_timestamp_utc)

    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    valid_transit = delta_days > 0
    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    is_untracked     = trk.apply(is_false_like)
    is_tracked_true  = trk.apply(is_true_like)
    is_missing       = (~is_untracked) & is_tracked_true & (~valid_transit.fillna(False))
    is_tracked_good  = (~is_untracked) & is_tracked_true &  valid_transit.fillna(False)

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
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        "Time taken from Picked up to Delivered": ["", "", "", ""],
    })

    rows = df[is_tracked_good].copy().reset_index(drop=True)
    if len(rows):
        p_city, p_state = zip(*rows[LTL_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[LTL_MAP["d_city_state"]].astype(str).map(split_city_state))
    else:
        p_city, p_state, d_city, d_state = ([], [], [], [])

    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
        if cnt_tracked > 0 else pd.Series([], dtype="Int64")
    )

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
        "Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
    total_row = {col: "" for col in main.columns}
    total_row["Pro Number"] = "Grand Total"
    total_row["Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # DATA sheet: exact column structure
    data_df = pd.DataFrame()
    for col in LTL_DATA_COLUMNS:
        if col == "Transit Time":
            data_df[col] = in_transit_days
        elif col in df.columns:
            data_df[col] = df[col]
        else:
            data_df[col] = np.nan

    return small, main, data_df

# =============================
# PARCEL
# =============================
PARCEL_MAP = {
    "shipment_id": "Shipment ID",
    "carrier_id": "Carrier ID",
    "carrier_name": "Carrier Name",
    "tenant_id": "Tenant ID",
    "tenant_name": "Tenant Name",
    "bol": "Bill of Lading",
    "order_number": "Order Number",
    "tracking_number": "Tracking Number",
    "tracked": "Tracked",
    "pickup_region": "Pickup Region",
    "pickup_country": "Pickup Country",
    "pickup_name": "Pickup Name",
    "dropoff_region": "Dropoff Country Region",
    "dropoff_country": "Dropoff Country",
    "destination_name": "Destination Name",
    "pickup_ts": "Pickup Utc Timestamp Time",
    "departed_ts": "Departed Utc Timestamp Time",
    "delivered_ts": "Delivered Utc Timestamp Time",
    "final_status_reason": "Final Status Reason",
}

PARCEL_DATA_COLUMNS = [
    "Carrier Name", "Tracking Number", "Tracked", "Pickup Region", "Pickup Country",
    "Dropoff Country Region", "Dropoff Country", "Pickup Utc Timestamp Time",
    "Departed Utc Timestamp Time", "Out for Delivery Utc Timestamp Time",
    "Arrived Utc Timestamp Time", "Delivered Utc Timestamp Time", "Transit Time",
    "Final Status Reason"
]

def build_parcel_tables(df_raw: pd.DataFrame):
    """Returns (summary_df, detail_df, data_df)"""
    df = normalize_headers(df_raw).copy()
    for _, col in PARCEL_MAP.items():
        if col not in df.columns: df[col] = np.nan

    trk    = df[PARCEL_MAP["tracked"]]
    dep_ts = df[PARCEL_MAP["departed_ts"]].apply(_parse_ts_zero_ok)
    arr_ts = df[PARCEL_MAP["delivered_ts"]].apply(_parse_ts_zero_ok)

    delta_days = (arr_ts - dep_ts).dt.total_seconds() / (24 * 3600)
    ts_missing = dep_ts.isna() | arr_ts.isna()
    valid_transit = (delta_days > 0) & (~ts_missing)

    is_untracked    = trk.apply(is_false_like)
    is_tracked_true = trk.apply(is_true_like)
    is_missing      = (~is_untracked) & is_tracked_true & (~valid_transit)
    is_tracked_good = (~is_untracked) & is_tracked_true &  valid_transit

    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    cnt_untracked = int(is_untracked.sum())
    cnt_missing   = int(is_missing.sum())
    cnt_tracked   = int(is_tracked_good.sum())
    grand_total   = cnt_untracked + cnt_missing + cnt_tracked

    tracked_days = in_transit_days.where(is_tracked_good)
    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    small = pd.DataFrame({
        "Label": ["Tracked", "Untracked", "Missed Milestone", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_untracked, cnt_missing, grand_total],
        "": ["", "", "", ""],
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        "Time taken from Departure to Delivered": ["", "", "", ""],
    })

    rows = df[is_tracked_good].copy().reset_index(drop=True)
    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
        if cnt_tracked > 0 else pd.Series([], dtype="Int64")
    )

    main = pd.DataFrame({
        "Tracking Number": rows[PARCEL_MAP["tracking_number"]].astype(str).str.strip(),
        "Pickup Region": rows[PARCEL_MAP["pickup_region"]].astype(str).str.strip(),
        "Pickup Country": rows[PARCEL_MAP["pickup_country"]].astype(str).str.strip(),
        "Dropoff Country Region": rows[PARCEL_MAP["dropoff_region"]].astype(str).str.strip(),
        "Dropoff Country": rows[PARCEL_MAP["dropoff_country"]].astype(str).str.strip(),
        "Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
    total_row = {col: "" for col in main.columns}
    total_row["Tracking Number"] = "Grand Total"
    total_row["Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # DATA sheet: exact column structure
    data_df = pd.DataFrame()
    for col in PARCEL_DATA_COLUMNS:
        if col == "Transit Time":
            data_df[col] = in_transit_days
        elif col in df.columns:
            data_df[col] = df[col]
        else:
            data_df[col] = np.nan

    return small, main, data_df

# =============================
# OCEAN
# =============================
OCEAN_MAP = {
    "tenant_name": "Tenant Name",
    "owner_id": "Owner ID",
    "carrier_name": "Carrier Name",
    "shipment_id": "Shipment ID",
    "container_number": "Container Number",
    "request_key": "Request Key",
    "pol": "Pol",
    "pod": "Pod",
    "gate_in": "2-Gate In Timestamp",
    "container_loaded": "3-Container Loaded Timestamp",
    "vessel_depart_carrier": "4-Vessel Depart POL Carrier Timestamp",
    "vessel_depart_p44": "4-Vessel Depart POL p44 Timestamp",
    "vessel_arrive_carrier": "5-Vessel Arrive POD Carrier Timestamp",
    "vessel_arrive_p44": "5-Vessel Arrive POD p44 Timestamp",
    "container_discharge": "6-Container Discharge Timestamp",
    "gate_out": "7-Gate Out Timestamp",
    "lifecycle_status": "Lifecycle Status",
}

OCEAN_DATA_COLUMNS = [
    "Shipper Tenant Name", "Owner ID", "Carrier Name", "Shipment ID", 
    "Tracking Requested Date Date", "Subscription Created Date Date", "Subscription Status",
    "Container Status", "Lifecycle Status", "Container Number", "Request Key", "FFW Name",
    "Request Key Type", "Carrier Connectivity", "Edi Source", "POL", "POD",
    "1-Empty Pickup Timestamp", "2-Gate In Timestamp", "3-Container Loaded Timestamp",
    "4-Vessel Depart POL Carrier Timestamp", "4-Vessel Depart POL p44 Timestamp",
    "5-Vessel Arrive POD Carrier Timestamp", "5-Vessel Arrive POD p44 Timestamp",
    "6-Container Discharge Timestamp", "7-Gate Out Timestamp", "8-Empty Return Timestamp",
    "Transit Time", "Shipment Completed", "1-Empty Pickup Missed", "2-Gate In Missed",
    "3-Container Loaded POL Missed", "4-Vessel Departure POL Missed", 
    "5-Vessel Arrival POD Missed", "6-Container Discharge POD Missed", 
    "7-Gate Out Missed", "8-Empty Return Missed"
]

OCEAN_TS_ALL_FOR_UNTRACKED = [
    "2-Gate In Timestamp",
    "3-Container Loaded Timestamp",
    "4-Vessel Depart POL Carrier Timestamp",
    "4-Vessel Depart POL p44 Timestamp",
    "5-Vessel Arrive POD Carrier Timestamp",
    "5-Vessel Arrive POD p44 Timestamp",
    "6-Container Discharge Timestamp",
    "7-Gate Out Timestamp",
]

INTERMEDIATE_TS_FOR_INTRANSIT = [
    "3-Container Loaded Timestamp",
    "4-Vessel Depart POL Carrier Timestamp",
    "4-Vessel Depart POL p44 Timestamp",
    "5-Vessel Arrive POD Carrier Timestamp",
    "5-Vessel Arrive POD p44 Timestamp",
    "6-Container Discharge Timestamp",
]

def build_ocean_tables(df_raw: pd.DataFrame):
    """Returns (summary_df, lane_detail_df, container_detail_df, data_df)"""
    df = normalize_headers(df_raw).copy()
    for _, col in OCEAN_MAP.items():
        if col not in df.columns: df[col] = np.nan

    gate_in_ts  = df[OCEAN_MAP["gate_in"]].apply(_parse_ts_zero_ok)
    gate_out_ts = df[OCEAN_MAP["gate_out"]].apply(_parse_ts_zero_ok)

    all_empty_mask = pd.Series(True, index=df.index)
    for col in OCEAN_TS_ALL_FOR_UNTRACKED:
        col_ts = df[col].apply(_parse_ts_zero_ok)
        all_empty_mask &= col_ts.isna()

    lifecycle_status = df[OCEAN_MAP["lifecycle_status"]].astype(str).str.strip().str.lower()

    delta_days = (gate_out_ts - gate_in_ts).dt.total_seconds() / (24 * 3600)
    delta_pos  = delta_days > 0
    delta_neg  = delta_days < 0

    gate_in_present   = ~gate_in_ts.isna()
    gate_in_missing   = gate_in_ts.isna()
    gate_out_present  = ~gate_out_ts.isna()
    gate_out_missing  = gate_out_ts.isna()

    any_intermediate_present = pd.Series(False, index=df.index)
    for col in INTERMEDIATE_TS_FOR_INTRANSIT:
        any_intermediate_present |= ~df[col].apply(_parse_ts_zero_ok).isna()

    is_untracked = all_empty_mask
    is_tracked_good = delta_pos & (~is_untracked)

    both_missing = gate_in_missing & gate_out_missing
    lifecycle_active = lifecycle_status.eq("active")
    intransit_case_d1 = both_missing & any_intermediate_present & lifecycle_active
    intransit_case_d2 = gate_in_present & gate_out_missing & any_intermediate_present
    intransit_case_d3 = gate_in_present & gate_out_missing & (~any_intermediate_present) & lifecycle_active
    is_in_transit = (intransit_case_d1 | intransit_case_d2 | intransit_case_d3) & (~is_untracked) & (~is_tracked_good)

    missing_case_b  = delta_neg
    missing_case_c  = gate_out_present & gate_in_missing
    missing_case_d1 = both_missing & any_intermediate_present & (~lifecycle_active)
    missing_case_d2 = gate_in_present & gate_out_missing & (~any_intermediate_present) & (~lifecycle_active)
    is_missing = (missing_case_b | missing_case_c | missing_case_d1 | missing_case_d2)
    is_missing = is_missing & (~is_untracked) & (~is_tracked_good) & (~is_in_transit)

    in_transit_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)

    cnt_untracked   = int(is_untracked.sum())
    cnt_in_transit  = int(is_in_transit.sum())
    cnt_missing     = int(is_missing.sum())
    cnt_tracked     = int(is_tracked_good.sum())
    grand_total     = cnt_untracked + cnt_in_transit + cnt_missing + cnt_tracked

    tracked_days = in_transit_days.where(is_tracked_good)
    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    small = pd.DataFrame({
        "Label": ["Tracked", "Untracked", "Missed Milestone", "In Transit Shipment", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_untracked, cnt_missing, cnt_in_transit, grand_total],
        "": ["", "", "", "", ""],
        "Average of In-Transit Time": ["", "", "", "", avg_tracked],
        "Time taken from Gate In to Gate Out": ["", "", "", "", ""],
    })

    rows_tracked = df[is_tracked_good].copy().reset_index(drop=True)
    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
        if cnt_tracked > 0 else pd.Series([], dtype="Int64")
    )

    # Container-level detail
    main2 = pd.DataFrame({
        "Container Number": rows_tracked[OCEAN_MAP["container_number"]].astype(str).str.strip(),
        "Request Key": rows_tracked[OCEAN_MAP["request_key"]].astype(str).str.strip(),
        "FFW Name": rows_tracked[OCEAN_MAP["carrier_name"]].astype(str).str.strip(),
        "Pol": rows_tracked[OCEAN_MAP["pol"]].astype(str).str.strip(),
        "Pod": rows_tracked[OCEAN_MAP["pod"]].astype(str).str.strip(),
        "Transit Time": avg_days_col,
        "Add 7 Days D2D": 7,
        "D2D Avg Transit Time": (avg_days_col.astype("float") + 7).astype("Int64"),
    })

    # Lane-level aggregation
    if cnt_tracked > 0 and len(rows_tracked) > 0:
        lanes = rows_tracked[[OCEAN_MAP["pol"], OCEAN_MAP["pod"]]].copy()
        lanes.columns = ["Pol", "Pod"]
        lanes["Transit Time"] = avg_days_col.astype("float")
        lane_agg = (
            lanes.groupby(["Pol", "Pod"], dropna=False)["Transit Time"]
                 .mean().round().astype("Int64").reset_index()
        )
    else:
        lane_agg = pd.DataFrame({"Pol": pd.Series(dtype=str),
                                 "Pod": pd.Series(dtype=str),
                                 "Transit Time": pd.Series(dtype="Int64")})

    main1 = lane_agg.copy()
    if len(main1) > 0:
        main1["Add 7 Days D2D"] = 7
        main1["D2D Avg Transit Time"] = (pd.to_numeric(main1["Transit Time"], errors="coerce").astype(float) + 7)\
                                         .round().astype("Int64")
    else:
        main1["Add 7 Days D2D"] = pd.Series(dtype="Int64")
        main1["D2D Avg Transit Time"] = pd.Series(dtype="Int64")

    # Append Grand Total rows
    def _append_total_row(df_in: pd.DataFrame, first_label_col: str):
        if len(df_in) == 0: return df_in
        javg = float(pd.to_numeric(df_in["Transit Time"], errors="coerce").dropna().mean()) \
               if "Transit Time" in df_in.columns else ""
        total = {col: "" for col in df_in.columns}
        total[first_label_col] = "Grand Total"
        total["Transit Time"] = javg
        if "Add 7 Days D2D" in df_in.columns:
            total["Add 7 Days D2D"] = 7
            total["D2D Avg Transit Time"] = (javg + 7) if javg != "" else ""
        return pd.concat([df_in, pd.DataFrame([total])], ignore_index=True)

    main1 = _append_total_row(main1, "Pol")
    main2 = _append_total_row(main2, "Container Number")

    # DATA sheet: exact column structure (with POL/POD mapped correctly)
    data_df = pd.DataFrame()
    for col in OCEAN_DATA_COLUMNS:
        if col == "Transit Time":
            data_df[col] = in_transit_days
        elif col == "POL" and OCEAN_MAP["pol"] in df.columns:
            data_df[col] = df[OCEAN_MAP["pol"]]
        elif col == "POD" and OCEAN_MAP["pod"] in df.columns:
            data_df[col] = df[OCEAN_MAP["pod"]]
        elif col in df.columns:
            data_df[col] = df[col]
        else:
            data_df[col] = np.nan

    return small, main1, main2, data_df

# =============================
# AIR
# =============================
AIR_MAP = {
    "carrier_scac": "Carrier Scac",
    "carrier_name": "Carrier Name",
    "airline_code": "Airline Code",
    "airline_name": "Airline Name",
    "tracking_type": "Tracking Type",
    "shipment_id": "Shipment ID",
    "tenant_id": "Tenant ID",
    "tenant_name": "Tenant Name",
    "awb": "Air Waybill",
    "hawb": "House Air Waybill",
    "pickup_country": "Pickup Country",
    "pickup_city": "Pickup City",
    "dest_country": "Destination Country",
    "dest_city": "Destination City",
    "m3_ready": "M3 Ready for Carriage Utc Dt",
    "m8_rcf": "M8 Received Cargo From Flight Utc Dt",
    "m9_hold": "M9 Import Custom on Hold Utc Dt",
    "m10_clear": "M10 Import Custom Cleared Utc Dt",
    "m11_notified": "M11 Notified Utc Dt",
    "m12_delivered": "M12 Delivered Utc Dt",
}

AIR_DATA_COLUMNS = [
    "Airline Code", "Airline Name", "Tracking Type", "Air Waybill", "FFW Name",
    "Pickup Country", "Pickup City", "Destination Country", "Destination City",
    "Shipment Created Date", "M3 Ready for Carriage Utc Dt",
    "M8 Received Cargo From Flight Utc Dt", "M10 Import Custom Cleared Utc Dt",
    "M11 Notified Utc Dt", "M12 Delivered Utc Dt", "Transit Time", "",
    "M8 Received Cargo From Flight Utc Dt.1", "M10 Import Custom Cleared Utc Dt.1",
    "M11 Notified Utc Dt.1", "M12 Delivered Utc Dt.1", "In-Transit Time"
]

def build_air_tables(df_raw: pd.DataFrame):
    """Returns (summary_df, detail_df, data_df)"""
    df = normalize_headers(df_raw).copy()
    for _, col in AIR_MAP.items():
        if col not in df.columns:
            df[col] = np.nan

    m3  = df[AIR_MAP["m3_ready"]].apply(_parse_ts_zero_ok)
    m8  = df[AIR_MAP["m8_rcf"]].apply(_parse_ts_zero_ok)
    m9  = df[AIR_MAP["m9_hold"]].apply(_parse_ts_zero_ok)
    m10 = df[AIR_MAP["m10_clear"]].apply(_parse_ts_zero_ok)
    m11 = df[AIR_MAP["m11_notified"]].apply(_parse_ts_zero_ok)
    m12 = df[AIR_MAP["m12_delivered"]].apply(_parse_ts_zero_ok)

    all_empty_six = m3.isna() & m8.isna() & m9.isna() & m10.isna() & m11.isna() & m12.isna()
    is_untracked = all_empty_six

    end_ts = (
        m12
        .combine_first(m11)
        .combine_first(m10)
        .combine_first(m9)
        .combine_first(m8)
    )
    end_ts  = pd.to_datetime(end_ts, utc=True, errors="coerce")
    start_ts = pd.to_datetime(m3,   utc=True, errors="coerce")

    delta = end_ts - start_ts
    delta_days = delta.dt.total_seconds() / (24 * 3600)

    delta_pos = delta_days > 0
    delta_neg = delta_days < 0
    start_missing   = start_ts.isna()
    any_end_present = ~(m8.isna() & m9.isna() & m10.isna() & m11.isna() & m12.isna())
    all_ends_missing = ~any_end_present

    is_tracked_good = delta_pos & (~is_untracked)

    is_missing = (
        delta_neg |
        (start_missing & any_end_present) |
        (~start_missing & all_ends_missing)
    )
    is_missing = is_missing & (~is_untracked) & (~is_tracked_good)

    per_row_days = delta_days.apply(lambda x: int(round_half_up_days(x)) if pd.notna(x) else np.nan)
    tracked_days = per_row_days.where(is_tracked_good)

    cnt_untracked = int(is_untracked.sum())
    cnt_missing   = int(is_missing.sum())
    cnt_tracked   = int(is_tracked_good.sum())
    grand_total   = cnt_untracked + cnt_missing + cnt_tracked

    avg_tracked = float(pd.to_numeric(tracked_days, errors="coerce").dropna().mean()) if cnt_tracked > 0 else ""

    small = pd.DataFrame({
        "Label": ["Tracked", "Untracked", "Missed Milestone", "Grand Total"],
        "Shipment Count": [cnt_tracked, cnt_untracked, cnt_missing, grand_total],
        "": ["", "", "", ""],
        "Average of In-Transit Time": ["", "", "", avg_tracked],
        "Time taken from Ready from Carriage to Delivery": ["", "", "", ""],
    })

    rows = df[is_tracked_good].copy().reset_index(drop=True)
    avg_days_col = (
        pd.to_numeric(tracked_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
        if cnt_tracked > 0 else pd.Series([], dtype="Int64")
    )
    add_days = 4
    total_days_col = (avg_days_col.astype("float") + add_days).round().astype("Int64") if len(avg_days_col) else pd.Series([], dtype="Int64")

    main = pd.DataFrame({
        "Air Waybill": rows[AIR_MAP["awb"]].astype(str).str.strip(),
        "Pickup City": rows[AIR_MAP["pickup_city"]].astype(str).str.strip(),
        "Pickup Country": rows[AIR_MAP["pickup_country"]].astype(str).str.strip(),
        "Destination City": rows[AIR_MAP["dest_city"]].astype(str).str.strip(),
        "Destination Country": rows[AIR_MAP["dest_country"]].astype(str).str.strip(),
        "Transit Time": avg_days_col,
        "Additional days": add_days,
        "Total Transit Time": total_days_col,
    })

    if len(main) > 0:
        javg = float(pd.to_numeric(main["Transit Time"], errors="coerce").dropna().mean())
        jtotal = javg + add_days if javg != "" else ""
    else:
        javg = ""
        jtotal = ""
    total_row = {col: "" for col in main.columns}
    total_row["Air Waybill"] = "Grand Total"
    total_row["Transit Time"] = javg
    total_row["Additional days"] = add_days if javg != "" else ""
    total_row["Total Transit Time"] = jtotal
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    # DATA sheet: exact column structure
    # Create "In-Transit Time" - can be text like "Missed Milestones" or numeric days
    in_transit_time_col = pd.Series(index=df.index, dtype=object)
    for idx in df.index:
        if is_tracked_good.iloc[idx]:
            in_transit_time_col.iloc[idx] = per_row_days.iloc[idx]
        elif is_missing.iloc[idx]:
            in_transit_time_col.iloc[idx] = "Missed Milestones"
        else:
            in_transit_time_col.iloc[idx] = np.nan

    data_df = pd.DataFrame()
    for col in AIR_DATA_COLUMNS:
        if col == "Transit Time":
            data_df[col] = per_row_days
        elif col == "In-Transit Time":
            data_df[col] = in_transit_time_col
        elif col == "":
            data_df[col] = ""
        elif col in df.columns:
            data_df[col] = df[col]
        else:
            data_df[col] = np.nan

    return small, main, data_df

# =============================
# UI
# =============================
st.markdown("### Select Modes to Process")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    use_ftl = st.checkbox("FTL/TL", value=False, key="cb_ftl")
with col2:
    use_ltl = st.checkbox("LTL", value=False, key="cb_ltl")
with col3:
    use_parcel = st.checkbox("Parcel", value=False, key="cb_parcel")
with col4:
    use_ocean = st.checkbox("Ocean", value=False, key="cb_ocean")
with col5:
    use_air = st.checkbox("Air", value=False, key="cb_air")

st.markdown("---")

# File uploaders for each checked mode
uploaded_files = {}

if use_ftl:
    uploaded_files["FTL"] = st.file_uploader(
        "📁 Upload FTL/TL Data File",
        type=["csv", "xlsx", "xls"],
        key="uploader_ftl",
        help="Upload the raw data file for FTL/TL mode"
    )

if use_ltl:
    uploaded_files["LTL"] = st.file_uploader(
        "📁 Upload LTL Data File",
        type=["csv", "xlsx", "xls"],
        key="uploader_ltl",
        help="Upload the raw data file for LTL mode"
    )

if use_parcel:
    uploaded_files["Parcel"] = st.file_uploader(
        "📁 Upload Parcel Data File",
        type=["csv", "xlsx", "xls"],
        key="uploader_parcel",
        help="Upload the raw data file for Parcel mode"
    )

if use_ocean:
    uploaded_files["Ocean"] = st.file_uploader(
        "📁 Upload Ocean Data File",
        type=["csv", "xlsx", "xls"],
        key="uploader_ocean",
        help="Upload the raw data file for Ocean mode"
    )

if use_air:
    uploaded_files["Air"] = st.file_uploader(
        "📁 Upload Air Data File",
        type=["csv", "xlsx", "xls"],
        key="uploader_air",
        help="Upload the raw data file for Air mode"
    )

# Check if at least one mode is selected
modes_selected = use_ftl or use_ltl or use_parcel or use_ocean or use_air

if not modes_selected:
    st.info("Please select at least one mode using the checkboxes above.")
else:
    # Check if all selected modes have files uploaded
    missing_files = [mode for mode, file in uploaded_files.items() if file is None]
    
    if missing_files:
        st.warning(f"Please upload files for: {', '.join(missing_files)}")
    else:
        st.markdown("---")
        if st.button("🚀 Generate Reports for All Selected Modes", type="primary", use_container_width=True):
            try:
                with st.spinner("Processing all selected modes..."):
                    engine = pick_xlsx_engine()
                    if not engine:
                        st.error("Cannot export to Excel. Install xlsxwriter or openpyxl.")
                        st.stop()
                    
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine=engine) as writer:
                        
                        # Process FTL
                        if use_ftl and uploaded_files.get("FTL"):
                            with st.status("Processing FTL/TL..."):
                                df_raw = load_table(uploaded_files["FTL"])
                                summary_df, detail_df, data_df = build_ftl_tables(df_raw)
                                
                                # Write Data sheet (all raw columns + Transit Time)
                                data_df.to_excel(writer, sheet_name="TL Data", index=False)
                                
                                # Write Summary sheet (summary table + detail table)
                                summary_df.to_excel(writer, sheet_name="TL Summary", index=False, startrow=0)
                                detail_df.to_excel(writer, sheet_name="TL Summary", index=False, startrow=len(summary_df) + 1)
                                
                                st.success(f"✓ FTL/TL processed: {len(data_df):,} rows")
                        
                        # Process LTL
                        if use_ltl and uploaded_files.get("LTL"):
                            with st.status("Processing LTL..."):
                                df_raw = load_table(uploaded_files["LTL"])
                                summary_df, detail_df, data_df = build_ltl_tables(df_raw)
                                
                                data_df.to_excel(writer, sheet_name="LTL Data", index=False)
                                summary_df.to_excel(writer, sheet_name="LTL Summary", index=False, startrow=0)
                                detail_df.to_excel(writer, sheet_name="LTL Summary", index=False, startrow=len(summary_df) + 1)
                                
                                st.success(f"✓ LTL processed: {len(data_df):,} rows")
                        
                        # Process Parcel
                        if use_parcel and uploaded_files.get("Parcel"):
                            with st.status("Processing Parcel..."):
                                df_raw = load_table(uploaded_files["Parcel"])
                                summary_df, detail_df, data_df = build_parcel_tables(df_raw)
                                
                                data_df.to_excel(writer, sheet_name="Parcel Data", index=False)
                                summary_df.to_excel(writer, sheet_name="Parcel Summary", index=False, startrow=0)
                                detail_df.to_excel(writer, sheet_name="Parcel Summary", index=False, startrow=len(summary_df) + 1)
                                
                                st.success(f"✓ Parcel processed: {len(data_df):,} rows")
                        
                        # Process Ocean
                        if use_ocean and uploaded_files.get("Ocean"):
                            with st.status("Processing Ocean..."):
                                df_raw = load_table(uploaded_files["Ocean"])
                                summary_df, lane_df, container_df, data_df = build_ocean_tables(df_raw)
                                
                                data_df.to_excel(writer, sheet_name="Ocean Data", index=False)
                                summary_df.to_excel(writer, sheet_name="Ocean Summary", index=False, startrow=0)
                                # Lane and Container details side by side
                                lane_df.to_excel(writer, sheet_name="Ocean Summary", index=False, startrow=len(summary_df) + 1, startcol=0)
                                container_df.to_excel(writer, sheet_name="Ocean Summary", index=False, startrow=len(summary_df) + 1, startcol=len(lane_df.columns) + 1)
                                
                                st.success(f"✓ Ocean processed: {len(data_df):,} rows")
                        
                        # Process Air
                        if use_air and uploaded_files.get("Air"):
                            with st.status("Processing Air..."):
                                df_raw = load_table(uploaded_files["Air"])
                                summary_df, detail_df, data_df = build_air_tables(df_raw)
                                
                                data_df.to_excel(writer, sheet_name="Air Data", index=False)
                                summary_df.to_excel(writer, sheet_name="Air Summary", index=False, startrow=0)
                                detail_df.to_excel(writer, sheet_name="Air Summary", index=False, startrow=len(summary_df) + 1)
                                
                                st.success(f"✓ Air processed: {len(data_df):,} rows")
                    
                    out.seek(0)
                    excel_data = out.getvalue()
                    
                    st.success("🎉 All reports generated successfully!")
                    
                    # Generate filename with selected modes
                    selected_modes = []
                    if use_ftl: selected_modes.append("TL")
                    if use_ltl: selected_modes.append("LTL")
                    if use_parcel: selected_modes.append("Parcel")
                    if use_ocean: selected_modes.append("Ocean")
                    if use_air: selected_modes.append("Air")
                    
                    filename = f"P44_All_Data_Modes_{'_'.join(selected_modes)}.xlsx"
                    
                    st.download_button(
                        "⬇️ Download Combined Report (Excel)",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                    
            except Exception as e:
                st.error(f"Error processing files: {e}")
                import traceback
                st.code(traceback.format_exc())
