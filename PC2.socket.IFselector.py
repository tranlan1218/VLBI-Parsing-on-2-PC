import re
import sqlite3
import pandas as pd
import os
import socket

# ============================================================
# CONFIGURATION
# ============================================================
PC1_IP = "192.168.0.50"
PC1_PORT = 6000

db_path = r"D:\VLBI\PyCharmMiscProject\VLBI.test2.db"
TARGET_THREAD_ID = '15' # Thread ID for IF Selector

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
# STEP 2: Define robust SINGLE-LINE regex pattern for [15]
# ============================================================
# Captures the header fields and everything after the message as raw data.
full_entry_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+\[(?P<thread_id>\d+)\]\s+(?P<level>INFO)\s*-+\s*(?P<message>.*?)[:\s-]*(?P<data>.*)',
)

# Pattern to find the key-value sections within the data string:
# Group 1: key  'att', 'out2in', 'level'
# Group 2: values '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0'
# OLD: key_value_pattern = re.compile(r'(att|out2in|level)=(\d+[\d.,-]*)(?:\s*|$)', re.IGNORECASE)
# NEW: Allow an optional hyphen (-) at the start of the value string.
key_value_pattern = re.compile(r'(att|out2in|level)=(-?\d*[\d.,-]*)(?:\s*|$)', re.IGNORECASE)

# Define column names for the 48 data points (16 channels for each of the 3 keys)
IF_SELECTOR_COLUMNS = [f"CH{i}ATT" for i in range(1, 17)] + \
                      [f"CH{i}OUT2IN" for i in range(1, 17)] + \
                      [f"CH{i}LEVEL" for i in range(1, 17)]

# ============================================================
# STEP 3: Parse log lines into structured entries (Filter for [15])
# ============================================================
if_selector_entries = []

for line in lines:
    line = line.strip()
    if not line:
        continue

    m = full_entry_pattern.match(line)
    if m:
        entry = m.groupdict()

        # CRITICAL FILTER: Check if thread_id matches the target
        if entry["thread_id"] == TARGET_THREAD_ID:
            entry["data"] = entry["data"].strip()
            if_selector_entries.append(entry)

print(f"Total log entries detected and filtered for thread ID {TARGET_THREAD_ID}: **{len(if_selector_entries)}**")

# ============================================================
# STEP 4: Extract and structure the 48 columns
# ============================================================
parsed_if_selector_rows = []

for e in if_selector_entries:
    data_str = e.get("data", "").strip()
    if not data_str:
        continue

    # Split comma-separated values
    row = {
        "datetime": e["datetime"],
        "code": e["code"],
        "thread_id": e["thread_id"],
        "level": e["level"],
    }

    # Use a temporary dictionary to store the extracted values (key: [list of values])
    extracted_data = {}

    # Find all key=value blocks using the key_value_pattern
    found_blocks = key_value_pattern.findall(data_str)

    for key, values_str in found_blocks:
        # Split the comma-separated values
        extracted_data[key.lower()] = [v.strip() for v in values_str.split(",") if v.strip()]

    # Map the extracted values to the final 48 columns
    col_index = 0

    # 1. 'att' values (CH1ATT to CH16ATT)
    att_vals = extracted_data.get('att', [])
    for i in range(1, 17):
        col_name = f"CH{i}ATT"
        row[col_name] = att_vals[i - 1] if i - 1 < len(att_vals) else None
        col_index += 1

    # 2. 'out2in' values (CH1OUT2IN to CH16OUT2IN)
    out2in_vals = extracted_data.get('out2in', [])
    for i in range(1, 17):
        col_name = f"CH{i}OUT2IN"
        row[col_name] = out2in_vals[i - 1] if i - 1 < len(out2in_vals) else None
        col_index += 1

    # 3. 'level' values (CH1LEVEL to CH16LEVEL)
    level_vals = extracted_data.get('level', [])
    for i in range(1, 17):
        col_name = f"CH{i}LEVEL"
        row[col_name] = level_vals[i - 1] if i - 1 < len(level_vals) else None
        col_index += 1

    parsed_if_selector_rows.append(row)

print(f"Total IF Selector rows prepared for insertion: {len(parsed_if_selector_rows)}")

# ============================================================
# STEP 5 & 6: Connect to DB, create table, and insert data
# ============================================================
table_name = "IFselector"

# Connect to the DB (VLBI.test1.db)
conn = sqlite3.connect(db_path)
print(f"\nConnected to DB: {os.path.abspath(db_path)}")

# Construct the value columns for the CREATE TABLE statement
value_cols_sql = ", ".join([f"{col} TEXT" for col in IF_SELECTOR_COLUMNS])

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime TEXT,
    code TEXT,
    thread_id TEXT,
    level TEXT,
    {value_cols_sql}
);
"""

# Drop the table to ensure a clean insert
conn.execute(f"DROP TABLE IF EXISTS {table_name}")
conn.execute(create_table_sql)
conn.commit()

# Insert data using pandas
if parsed_if_selector_rows:
    df = pd.DataFrame(parsed_if_selector_rows)
    # Ensure DataFrame columns match the desired order and include the fixed header columns
    header_cols = ["datetime", "code", "thread_id", "level"]
    all_cols = header_cols + IF_SELECTOR_COLUMNS

    # Reindex the DataFrame to ensure all columns are present and in order
    df = df.reindex(columns=all_cols)

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into **{table_name}** (Thread ID {TARGET_THREAD_ID})")
else:
    print(f"No data found for Thread ID {TARGET_THREAD_ID} to insert.")

conn.close()

print("IF Selector data extraction and insertion complete!")