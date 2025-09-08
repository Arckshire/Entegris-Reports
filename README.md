# FTL In-Transit Time Processor

A simple Streamlit app that:
- Adds column **V = In-Transit Time** using your business logic.
- Builds a **Summary** sheet with a small top table (rows 1–5) and a main table starting at row 7.
- Exports **CSV** and styled **Excel** (both **Data** and **Summary**).

## Logic for "In-Transit Time" (column V)

1) **Untracked**
- If `Tracked == FALSE` (accepts `False`, `false`, `no`, `0`) **OR**
- `Nb Milestones Received` is blank/NA/0  
→ Set `V = "Untracked"`.

2) **Calculated days**
- Else compute `Dropoff Arrival Utc Timestamp Raw - Pickup Departure Utc Timestamp Raw` in **days**.  
- If either timestamp is missing, or the result is `<= 0`, set `V = "Missing Milestone"`.  
- Otherwise round-half-up (e.g., `3.4 → 3`, `3.5 → 4`, `3.7 → 4`) and write the integer.

3) **Summary**
- Small table:
    - **A1**=Label, **B1**=Shipment Count, **C1**=*(blank)*, **D1**=Average of In-Transit Time, **E1**=Time taken from Departure to Arrival  
    - **A1**, **B1**, **D1** are light-blue + bold.  
    - **A2**=Tracked, **A3**=Missed Milestone, **A4**=Untracked, **A5**=Grand Total (light-blue + bold).  
    - **B2**=count of numeric values in V, **B3**=count of `"Missing Milestone"`, **B4**=count of `"Untracked"`, **B5**=sum.  
    - **D5** shows the overall average of numeric V (days). *(E5 mirrors D5; adjust as needed.)*
- Main table (row 7 onward, headers A–J in light-blue + bold):
    - **Bill of Lading (bold)**, Pickup Name, Pickup City, Pickup State, Pickup Country,
      Dropoff Name, Dropoff City, Dropoff State, Dropoff Country, **Average of In-Transit Time** (from V).
    - Only rows with numeric V are included.
    - City/State are split on the first hyphen and trimmed.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
