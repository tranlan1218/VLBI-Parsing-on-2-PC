import socket
import sqlite3
import pandas as pd
import re
import os

# ============================================================
# CONFIGURATION
# ============================================================
PC1_IP = "192.168.0.50"
PC1_PORT = 6000
db_path = r"D:\VLBI\PyCharmMiscProject\VLBI.test2.db"
TARGET_THREAD_ID = '12' # Thread ID for Frontend
# ============================================================
# STEP 1: Receive log file from PC1
# ============================================================
print("Connecting to PC1...")

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((PC1_IP, PC1_PORT))

buffer = b""
while True:
    chunk = client.recv(4096)
    if not chunk:
        break
    buffer += chunk

client.close()
print(f"✅ Received {len(buffer)} bytes from PC1")

# Decode buffer (handle Korean encoding)
try:
    text = buffer.decode("CP949", errors="replace")
except:
    text = buffer.decode("EUC-KR", errors="replace")

lines = text.splitlines()

# ============================================================
# STEP 2: Define ROBUST SINGLE-LINE regex pattern
# ============================================================
# This pattern captures the log header AND the data block on the SAME line.
# (P<message>.*?) captures the message non-greedily up to the ':' or '-'
# (P<data>.*) captures EVERYTHING remaining on that line as the data block.
full_entry_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+\[(?P<thread_id>\d+)\]\s+(?P<level>\w+)\s*-+\s*(?P<message>.*?)[:\s-]*(?P<data>.*)',
)
# Frequency Data Pattern remains the same, operating on the 'data' group.
freq_pattern = re.compile(r'(\d+ghz)(.*?)(?=\d+ghz|$)', re.IGNORECASE)

# ============================================================
# STEP 3: Parse log lines into structured entries (Filter applied during match)
# ============================================================
entries = []

for line in lines:
    line = line.strip()
    if not line:
        continue

    m = full_entry_pattern.match(line)
    if m:
        entry = m.groupdict()

        # CRITICAL FILTER STEP: Check if thread_id matches the target
        if entry["thread_id"] == TARGET_THREAD_ID:
            entry["data"] = entry["data"].strip()
            entries.append(entry)

print(f"Total log entries detected and filtered for thread ID {TARGET_THREAD_ID}: **{len(entries)}**")

# ============================================================
# STEP 4: Extract frequency data and organize into tables
# ============================================================

FRONTEND_COLUMNS = [
    "RF_RHCP",
    "RF_LHCP",
    "RF_Low",
    "Cryo_ColdPla",
    "Cryo_ShieldBox",
    "Pressure",
    "NormalTemp_RF",
    "NormalTemp_Noise",
    "NormalTemp_Load",
    "LNA_LHCP_Vg1",
    "LNA_LHCP_Vg2",
    "LNA_LHCP_Vg3",
    "LNA_LHCP_Vg4",
    "LNA_LHCP_Vd1",
    "LNA_LHCP_Vd2",
    "LNA_LHCP_Vd3",
    "LNA_LHCP_Vd4",
    "LNA_LHCP_Id1",
    "LNA_LHCP_Id2",
    "LNA_LHCP_Id3",
    "LNA_LHCP_Id4",
    "NA_RHCP_Vg1",
    "NA_RHCP_Vg2",
    "NA_RHCP_Vg3",
    "NA_RHCP_Vg4",
    "LNA_RHCP_Vd1",
    "LNA_RHCP_Vd2",
    "LNA_RHCP_Vd3",
    "LNA_RHCP_Vd4",
    "LNA_RHCP_Id1",
    "LNA_RHCP_Id2",
    "LNA_RHCP_Id3",
    "LNA_RHCP_Id4",
    "Observation_Mode",
    "PolarizationStatus",
    "Status_NoiseDiode",
    "Status_PLO",
    "Status_PCAL",
    "Status_CalChoppe",
    "Status_FlatMirror"
]

freq_tables = {
    "2ghz": [],
    "8ghz": [],
    "22ghz": [],
    "43ghz": [],
}

for e in entries:
    data_str = e.get("data", "").strip()
    if not data_str:
        continue

    found_freq_blocks = freq_pattern.findall(data_str)

    for freq, values in found_freq_blocks:
        freq = freq.lower()
        if freq not in freq_tables:
            continue

        # Split comma-separated values
        vals = [v.strip() for v in values.strip(" ,").split(",") if v.strip()]

        # Ensure exactly 40 values
        if len(vals) < len(FRONTEND_COLUMNS):
            vals += [None] * (len(FRONTEND_COLUMNS) - len(vals))
        else:
            vals = vals[:len(FRONTEND_COLUMNS)]

        row = {
            "datetime": e["datetime"],
            "code": e["code"],
            "thread_id": e["thread_id"],
            "level": e["level"],
        }

        # Map into named frontend columns
        for col_name, val in zip(FRONTEND_COLUMNS, vals):
            row[col_name] = val

        freq_tables[freq].append(row)

total_rows_inserted = sum(len(rows) for rows in freq_tables.values())
print(f"✅ Total rows prepared for insertion: {total_rows_inserted}")

# ============================================================
# STEP 5 & 6: Connect to existing DB, create tables, and insert data (UPDATED)
# ============================================================

# Check if the directory exists (necessary for connection to succeed)
db_dir = os.path.dirname(db_path)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir)

# Connect to DB
conn = sqlite3.connect(db_path)
print(f"\nConnected to DB: {os.path.abspath(db_path)}")

# Build CREATE TABLE SQL using the 40 fixed FrontEnd columns
value_cols_sql = ", ".join([f"{col} TEXT" for col in FRONTEND_COLUMNS])

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {{table_name}} (
    datetime TEXT,
    code TEXT,
    thread_id TEXT,
    level TEXT,
    {value_cols_sql}
);
"""

for freq, rows in freq_tables.items():
    table_name = f"frontend_{freq}"

    if not rows:
        print(f"Skipping {table_name}: No data found.")
        continue

    # Drop existing table to ensure clean schema
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(create_table_sql.format(table_name=table_name))
    conn.commit()

    # Insert rows using pandas
    df = pd.DataFrame(rows)

    # Ensure DataFrame has all 40 frontend columns
    header_cols = ["datetime", "code", "thread_id", "level"]
    df = df.reindex(columns=header_cols + FRONTEND_COLUMNS)

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df)} rows into {table_name}")

conn.close()
print("✅ All filtered and parsed frequency data saved successfully to the database!")



