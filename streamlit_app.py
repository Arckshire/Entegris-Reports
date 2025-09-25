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
    return math.floor(x + 0.5)  # e.g., 3.5->4, 3.4->3

def split_city_state(text: str):
    """Preferred: split on first '-' (city - state). Else try last 2-letter uppercase token as state."""
    if is_missing_like(text): return "", ""
    s = str(text).strip()
    parts = re.split(r"\s*-\s*", s, maxsplit=1)
    if len(parts) == 2: return parts[0].strip(), parts[1].strip()
    m = re.match(r"^(.*?)[\s,]+([A-Z]{2})$", s)
    if m: return m.group(1).strip(), m.group(2).strip()
    return s, ""

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

    # CSV: read as text to avoid weird type coercions
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
# FTL (Original, corrected for alignment)
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

def build_ftl_tables(df_raw: pd.DataFrame):
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

    rows = df[is_tracked_good].copy().reset_index(drop=True)
    if len(rows):
        p_city, p_state = zip(*rows[FTL_RAW_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[FTL_RAW_MAP["d_city_state"]].astype(str).map(split_city_state))
    else:
        p_city, p_state, d_city, d_state = ([], [], [], [])

    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
    ) if cnt_tracked > 0 else pd.Series([], dtype="Int64")

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
        "Average of In-Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
    total_row = {col: "" for col in main.columns}
    total_row["Bill of Lading"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main

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
    "def_col_d": "Average of In-Transit Time",
    "def_col_e": "Time taken from Picked up to Delivered",
}

def build_ltl_tables(df_raw: pd.DataFrame):
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
        LTL_MAP["def_col_d"]: ["", "", "", avg_tracked],
        LTL_MAP["def_col_e"]: ["", "", "", ""],
    })

    rows = df[is_tracked_good].copy().reset_index(drop=True)
    if len(rows):
        p_city, p_state = zip(*rows[LTL_MAP["p_city_state"]].astype(str).map(split_city_state))
        d_city, d_state = zip(*rows[LTL_MAP["d_city_state"]].astype(str).map(split_city_state))
    else:
        p_city, p_state, d_city, d_state = ([], [], [], [])

    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
    ) if cnt_tracked > 0 else pd.Series([], dtype="Int64")

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
        "Average of In-Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
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
    if not engine: return None
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
    buf.write("\n")
    main_df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

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
    "pickup_ret_ts": "Pickup Utc Retrieval Timestamp Time",
    "departed_ts": "Departed Utc Timestamp Time",
    "departed_ret_ts": "Departed Utc Retrieval Timestamp Time",
    "ofd_ts": "Out for Delivery Utc Timestamp Time",
    "ofd_ret_ts": "Out for Delivery Utc Retrieval Timestamp Time",
    "arrived_ts": "Arrived Utc Timestamp Time",
    "arrived_ret_ts": "Arrived Utc Retrieval Timestamp Time",
    "delivered_ts": "Delivered Utc Timestamp Time",
    "delivered_ret_ts": "Delivered Utc Retrieval Timestamp Time",
    "nb_expected": "Nb Milestones Expected",
    "nb_received": "Nb Milestones Received",
    "latency_updates_received": "Latency Updates Received",
    "latency_updates_passed": "Latency Updates Passed",
    "latency_in_hour": "Latency In Hour",
    "final_status_reason": "Final Status Reason",
}

def _parse_ts_zero_ok(x):
    """Treat '0' or 0 as missing; otherwise parse to UTC."""
    if pd.isna(x): return pd.NaT
    if isinstance(x, str) and x.strip() == "0": return pd.NaT
    if isinstance(x, (int, float)) and float(x) == 0.0: return pd.NaT
    return pd.to_datetime(x, utc=True, errors="coerce")

def build_parcel_tables(df_raw: pd.DataFrame):
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
    ) if cnt_tracked > 0 else pd.Series([], dtype="Int64")

    main = pd.DataFrame({
        "Tracking Number": rows[PARCEL_MAP["tracking_number"]].astype(str).str.strip(),
        "Pickup Region": rows[PARCEL_MAP["pickup_region"]].astype(str).str.strip(),
        "Pickup Country": rows[PARCEL_MAP["pickup_country"]].astype(str).str.strip(),
        "Dropoff Country Region": rows[PARCEL_MAP["dropoff_region"]].astype(str).str.strip(),
        "Dropoff Country": rows[PARCEL_MAP["dropoff_country"]].astype(str).str.strip(),
        "Average of In-Transit Time": avg_days_col,
    })

    javg = float(pd.to_numeric(main["Average of In-Transit Time"], errors="coerce").dropna().mean()) if len(main) else ""
    total_row = {col: "" for col in main.columns}
    total_row["Tracking Number"] = "Grand Total"
    total_row["Average of In-Transit Time"] = javg
    main = pd.concat([main, pd.DataFrame([total_row])], ignore_index=True)

    return small, main

# =============================
# OCEAN
# =============================
OCEAN_MAP = {
    "tenant_name": "Tenant Name",
    "owner_id": "Owner ID",
    "carrier_name": "Carrier Name",
    "shipment_id": "Shipment ID",
    "shipment_created": "Shipment Created Date Date",
    "shipment_modified": "Shipment Modified Date Date",
    "subscription_created": "Subscription Created Date Date",
    "subscription_status": "Subscription Status",
    "container_status": "Container Status",
    "lifecycle_status": "Lifecycle Status",
    "container_number": "Container Number",
    "container_type": "Container Type",
    "request_key": "Request Key",
    "request_key_type": "Request Key Type",
    "carrier_connectivity": "Carrier Connectivity",
    "edi_source": "Edi Source",
    "pol": "Pol",
    "pod": "Pod",
    "empty_pickup": "1-Empty Pickup Timestamp",
    "gate_in": "2-Gate In Timestamp",
    "container_loaded": "3-Container Loaded Timestamp",
    "vessel_depart_carrier": "4-Vessel Depart POL Carrier Timestamp",
    "vessel_depart_p44": "4-Vessel Depart POL p44 Timestamp",
    "vessel_arrive_carrier": "5-Vessel Arrive POD Carrier Timestamp",
    "vessel_arrive_p44": "5-Vessel Arrive POD p44 Timestamp",
    "container_discharge": "6-Container Discharge Timestamp",
    "gate_out": "7-Gate Out Timestamp",
    "empty_return": "8-Empty Return Timestamp",
    "origin_pickup_actual": "Origin Pickup Actual Date",
    "delivery_actual": "Delivery Actual Date",
    "master_shipment_id": "Master Shipment ID",
    "duplicate_flag": "TEST-duplicate flag",
    "missed_1": "1-Empty Pickup Missed",
    "missed_2": "2-Gate In Missed",
    "missed_3": "3-Container Loaded POL Missed",
    "missed_4": "4-Vessel Departure POL Missed",
    "missed_5": "5-Vessel Arrival POD Missed",
    "missed_6": "6-Container Discharge POD Missed",
    "missed_7": "7-Gate Out Missed",
    "missed_8": "8-Empty Return Missed",
}

OCEAN_TS_ALL_FOR_UNTRACKED = [
    OCEAN_MAP["gate_in"],
    OCEAN_MAP["container_loaded"],
    OCEAN_MAP["vessel_depart_carrier"],
    OCEAN_MAP["vessel_depart_p44"],
    OCEAN_MAP["vessel_arrive_carrier"],
    OCEAN_MAP["vessel_arrive_p44"],
    OCEAN_MAP["container_discharge"],
    OCEAN_MAP["gate_out"],
]

INTERMEDIATE_TS_FOR_INTRANSIT = [
    OCEAN_MAP["container_loaded"],
    OCEAN_MAP["vessel_depart_carrier"],
    OCEAN_MAP["vessel_depart_p44"],
    OCEAN_MAP["vessel_arrive_carrier"],
    OCEAN_MAP["vessel_arrive_p44"],
    OCEAN_MAP["container_discharge"],
]

def build_ocean_tables(df_raw: pd.DataFrame):
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

    # 1) Untracked
    is_untracked = all_empty_mask

    # 2) Tracked (positive delta)
    is_tracked_good = delta_pos & (~is_untracked)

    # 3) In-Transit:
    both_missing = gate_in_missing & gate_out_missing
    lifecycle_active = lifecycle_status.eq("active")
    intransit_case_d1 = both_missing & any_intermediate_present & lifecycle_active
    intransit_case_d2 = gate_in_present & gate_out_missing & any_intermediate_present
    intransit_case_d3 = gate_in_present & gate_out_missing & (~any_intermediate_present) & lifecycle_active
    is_in_transit = (intransit_case_d1 | intransit_case_d2 | intransit_case_d3) & (~is_untracked) & (~is_tracked_good)

    # 4) Missing:
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
        "Label": [
            "Tracked",
            "Untracked",
            "Missed Milestone",
            "In Transit Shipment",
            "Grand Total",
        ],
        "Shipment Count": [
            cnt_tracked,
            cnt_untracked,
            cnt_missing,
            cnt_in_transit,
            grand_total,
        ],
        "": ["", "", "", "", ""],
        "Average of In-Transit Time": ["", "", "", "", avg_tracked],
        "Time taken from Gate In to Gate Out": ["", "", "", "", ""],
    })

    rows_tracked = df[is_tracked_good].copy().reset_index(drop=True)
    avg_days_col = (
        pd.to_numeric(in_transit_days[is_tracked_good], errors="coerce").astype("Int64").reset_index(drop=True)
    ) if cnt_tracked > 0 else pd.Series([], dtype="Int64")

    # Main Table 2 — Container level (G–N)
    main2 = pd.DataFrame({
        "Container Number": rows_tracked[OCEAN_MAP["container_number"]].astype(str).str.strip(),
        "Request Key": rows_tracked[OCEAN_MAP["request_key"]].astype(str).str.strip(),
        "FFW Name": rows_tracked[OCEAN_MAP["carrier_name"]].astype(str).str.strip(),  # placeholder
        "Pol": rows_tracked[OCEAN_MAP["pol"]].astype(str).str.strip(),
        "Pod": rows_tracked[OCEAN_MAP["pod"]].astype(str).str.strip(),
        "Average of In-Transit Time": avg_days_col,
        "Add 7 Days D2D": 7,
        "D2D Avg Transit Time": (avg_days_col.astype("float") + 7).astype("Int64"),
    })

    # Main Table 1 — Lane level (A–E)
    if cnt_tracked > 0 and len(rows_tracked) > 0:
        lanes = rows_tracked[[OCEAN_MAP["pol"], OCEAN_MAP["pod"]]].copy()
        lanes.columns = ["Pol", "Pod"]
        lanes["Average of In-Transit Time"] = avg_days_col.astype("float")
        lane_agg = (
            lanes.groupby(["Pol", "Pod"], dropna=False)["Average of In-Transit Time"]
                 .mean().round().astype("Int64").reset_index()
        )
    else:
        lane_agg = pd.DataFrame({
            "Pol": pd.Series(dtype=str),
            "Pod": pd.Series(dtype=str),
            "Average of In-Transit Time": pd.Series(dtype="Int64"),
        })

    main1 = lane_agg.copy()
    if len(main1) > 0:
        main1["Add 7 Days D2D"] = 7
        main1["D2D Avg Transit Time"] = (
            pd.to_numeric(main1["Average of In-Transit Time"], errors="coerce").astype(float) + 7
        ).round().astype("Int64")
    else:
        main1["Add 7 Days D2D"] = pd.Series(dtype="Int64")
        main1["D2D Avg Transit Time"] = pd.Series(dtype="Int64")

    # Append Grand Total avg row to both mains
    def _append_total_row(df_in: pd.DataFrame, first_label_col: str):
        if len(df_in) == 0: return df_in
        javg = float(pd.to_numeric(df_in["Average of In-Transit Time"], errors="coerce").dropna().mean()) \
               if "Average of In-Transit Time" in df_in.columns else ""
        total = {col: "" for col in df_in.columns}
        total[first_label_col] = "Grand Total"
        total["Average of In-Transit Time"] = javg
        if "Add 7 Days D2D" in df_in.columns:
            total["Add 7 Days D2D"] = 7
            total["D2D Avg Transit Time"] = (javg + 7) if javg != "" else ""
        return pd.concat([df_in, pd.DataFrame([total])], ignore_index=True)

    main1 = _append_total_row(main1, "Pol")
    main2 = _append_total_row(main2, "Container Number")

    return small, main1, main2

# -----------------------------
# Single, unified UI (no duplicates)
# -----------------------------
mode = st.selectbox(
    "Choose Product",
    options=["FTL", "LTL", "Parcel", "Ocean"],
    index=0,
    help=("FTL uses original working logic; LTL uses Pickup→Delivered; "
          "Parcel uses Departed→Delivered (0 treated as missing); "
          "Ocean uses Gate In→Gate Out with lane- and container-level mains."),
    key="mode_select"
)

uploaded = st.file_uploader(
    "Upload RAW file (CSV or Excel)",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=False,
    key="uploader_main"
)
st.caption(f"Selected: **{mode}**")

if uploaded:
    try:
        df_raw = load_table(uploaded)
        st.write(f"**Rows loaded:** {len(df_raw):,} | **Columns:** {len(df_raw.columns)}")

        if mode == "FTL":
            small_df, main_df = build_ftl_tables(df_raw)
            st.success("Summary built successfully.")
            with st.expander("Preview — Small table (rows 1–5)"):
                st.dataframe(small_df, use_container_width=True)
            with st.expander("Preview — Main table (row 7 onward)"):
                st.dataframe(main_df.head(50), use_container_width=True)

            single_csv_blob = build_summary_single_csv(small_df, main_df)
            st.download_button("⬇️ Download Summary (Single CSV)", data=single_csv_blob,
                               file_name=f"Summary_{mode}.csv", mime="text/csv", use_container_width=True)

            excel_blob = build_summary_excel(small_df, main_df, mode)
            if excel_blob is not None:
                st.download_button("⬇️ Download Summary (Excel)", data=excel_blob,
                                   file_name=f"Summary_{mode}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
            else:
                st.info("Excel engine unavailable; CSV export works. Add `openpyxl` or `xlsxwriter`.")

            st.caption(
                f"Counts — Tracked: {int(small_df.loc[0, 'Shipment Count'])}, "
                f"Missed: {int(small_df.loc[1, 'Shipment Count'])}, "
                f"Untracked: {int(small_df.loc[2, 'Shipment Count'])}, "
                f"Total: {int(small_df.loc[3, 'Shipment Count'])}"
            )

        elif mode == "LTL":
            small_df, main_df = build_ltl_tables(df_raw)
            st.success("Summary built successfully.")
            with st.expander("Preview — Small table (rows 1–5)"):
                st.dataframe(small_df, use_container_width=True)
            with st.expander("Preview — Main table (row 7 onward)"):
                st.dataframe(main_df.head(50), use_container_width=True)

            single_csv_blob = build_summary_single_csv(small_df, main_df)
            st.download_button("⬇️ Download Summary (Single CSV)", data=single_csv_blob,
                               file_name=f"Summary_{mode}.csv", mime="text/csv", use_container_width=True)

            excel_blob = build_summary_excel(small_df, main_df, mode)
            if excel_blob is not None:
                st.download_button("⬇️ Download Summary (Excel)", data=excel_blob,
                                   file_name=f"Summary_{mode}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
            else:
                st.info("Excel engine unavailable; CSV export works. Add `openpyxl` or `xlsxwriter`.")

            st.caption(
                f"Counts — Tracked: {int(small_df.loc[0, 'Shipment Count'])}, "
                f"Missed: {int(small_df.loc[1, 'Shipment Count'])}, "
                f"Untracked: {int(small_df.loc[2, 'Shipment Count'])}, "
                f"Total: {int(small_df.loc[3, 'Shipment Count'])}"
            )

        elif mode == "Parcel":
            small_df, main_df = build_parcel_tables(df_raw)
            st.success("Summary built successfully.")
            with st.expander("Preview — Small table (rows 1–5)"):
                st.dataframe(small_df, use_container_width=True)
            with st.expander("Preview — Main table (row 7 onward)"):
                st.dataframe(main_df.head(50), use_container_width=True)

            single_csv_blob = build_summary_single_csv(small_df, main_df)
            st.download_button("⬇️ Download Summary (Single CSV)", data=single_csv_blob,
                               file_name=f"Summary_{mode}.csv", mime="text/csv", use_container_width=True)

            excel_blob = build_summary_excel(small_df, main_df, mode)
            if excel_blob is not None:
                st.download_button("⬇️ Download Summary (Excel)", data=excel_blob,
                                   file_name=f"Summary_{mode}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
            else:
                st.info("Excel engine unavailable; CSV export works. Add `openpyxl` or `xlsxwriter`.")

            st.caption(
                f"Counts — Tracked: {int(small_df.loc[0, 'Shipment Count'])}, "
                f"Untracked: {int(small_df.loc[1, 'Shipment Count'])}, "
                f"Missed: {int(small_df.loc[2, 'Shipment Count'])}, "
                f"Total: {int(small_df.loc[3, 'Shipment Count'])}"
            )

        else:  # Ocean
            small_df, main1_df, main2_df = build_ocean_tables(df_raw)
            st.success("Summary built successfully.")

            with st.expander("Preview — Small table (rows 1–6)"):
                st.dataframe(small_df, use_container_width=True)
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Main Table 1 — Lane Level (A–E)")
                st.dataframe(main1_df.head(50), use_container_width=True)
            with c2:
                st.subheader("Main Table 2 — Container Level (G–N)")
                st.dataframe(main2_df.head(50), use_container_width=True)

            # Downloads
            engine = pick_xlsx_engine()
            if engine:
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine=engine) as writer:
                    small_df.to_excel(writer, sheet_name="Summary", index=False, startrow=0)
                    main1_df.to_excel(writer, sheet_name="Summary", index=False, startrow=6)
                    startrow2 = 6 + len(main1_df) + 2
                    main2_df.to_excel(writer, sheet_name="Summary", index=False, startrow=startrow2)
                    pd.DataFrame({"Mode":[mode]}).to_excel(writer, sheet_name="Meta", index=False)
                out.seek(0)
                excel_blob = out.getvalue()
            else:
                excel_blob = None

            st.download_button("⬇️ Download Lane-Level (CSV)",
                               data=main1_df.to_csv(index=False).encode("utf-8"),
                               file_name=f"Summary_{mode}_lanes.csv",
                               mime="text/csv", use_container_width=True)
            st.download_button("⬇️ Download Container-Level (CSV)",
                               data=main2_df.to_csv(index=False).encode("utf-8"),
                               file_name=f"Summary_{mode}_containers.csv",
                               mime="text/csv", use_container_width=True)
            if excel_blob is not None:
                st.download_button("⬇️ Download Summary (Excel)", data=excel_blob,
                                   file_name=f"Summary_{mode}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   use_container_width=True)
            else:
                st.info("Excel engine unavailable; CSV exports work. Add `openpyxl` or `xlsxwriter`.")

            st.caption(
                f"Counts — Tracked: {int(small_df.loc[0, 'Shipment Count'])}, "
                f"Untracked: {int(small_df.loc[1, 'Shipment Count'])}, "
                f"Missed: {int(small_df.loc[2, 'Shipment Count'])}, "
                f"In Transit: {int(small_df.loc[3, 'Shipment Count'])}, "
                f"Total: {int(small_df.loc[4, 'Shipment Count'])}"
            )

    except Exception as e:
        st.error(f"Could not process this file. Details: {e}")
else:
    st.info("Upload your CSV/XLSX to generate the Summary.")
