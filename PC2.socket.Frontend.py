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

    # Find all frequency blocks in the data string
    found_freq_blocks = freq_pattern.findall(data_str)

    for freq, values in found_freq_blocks:
        freq = freq.lower()
        if freq not in freq_tables:
            continue

        # Split comma-separated values
        vals = [v.strip() for v in values.strip(" ,").split(",") if v.strip()]

        row = {
            "datetime": e["datetime"],
            "code": e["code"],
            "thread_id": e["thread_id"],
            "level": e["level"],
        }

        # Dynamically create column names (v1, v2, v3, ...)
        for j, v in enumerate(vals, start=1):
            row[f"v{j}"] = v

        freq_tables[freq].append(row)

total_rows_inserted = sum(len(rows) for rows in freq_tables.values())
print(f"Total rows prepared for insertion: {total_rows_inserted}")

# ============================================================
# STEP 5 & 6: Connect to existing DB, create tables, and insert data
# ============================================================
# Check if the directory exists (necessary for connection to succeed)
db_dir = os.path.dirname(db_path)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir)

# Connect to the existing/new DB (VLBI.test2.db)
table_name="frontend_{freq}"
conn = sqlite3.connect(db_path)
print(f"\nConnected to DB: {os.path.abspath(db_path)}")

create_table_sql = """
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime TEXT,
    code TEXT,
    thread_id TEXT,
    level TEXT,
    {value_cols}
);
"""
max_cols_per_table = {}

for freq, rows in freq_tables.items():
    if not rows:
        print(f"Skipping frontend_{freq}: No data found.")
        continue

    max_cols = max(len(r) - 4 for r in rows)
    max_cols_per_table[freq] = max_cols

    value_cols = ", ".join([f"v{i} TEXT" for i in range(1, max_cols + 1)])
    table_name = f"frontend_{freq}"

    # Drop table to ensure clean insertion compatible with dynamic columns
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(create_table_sql.format(
        table_name=table_name,
        value_cols=value_cols
    ))
    conn.commit()

    # Insert data
    df = pd.DataFrame(rows)
    for j in range(1, max_cols + 1):
        if f"v{j}" not in df.columns:
            df[f"v{j}"] = None

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into **{table_name}**")

conn.close()

print("All filtered and parsed frequency data saved successfully to the database!")