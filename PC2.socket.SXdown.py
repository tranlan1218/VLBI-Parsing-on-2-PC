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
TARGET_THREAD_ID = '13' # Thread ID for SX Downconverter

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
# STEP 2: Define robust SINGLE-LINE regex pattern for [13]
# ============================================================
# Captures the header fields and everything after the message as raw data.
full_entry_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+\[(?P<thread_id>\d+)\]\s+(?P<level>INFO)\s*-+\s*(?P<message>.*?)[:\s-]*(?P<data>.*)',
)

# Pattern to find the key-value sections within the data string:
# Group 1: key  'att', 'lock', 'level'
# Group 2: values '0,0,0,0
# Allow an optional hyphen (-) at the start of the value string.
key_value_pattern = re.compile(
    r'(att|level|lock)=([\+\-\d\.,a-zA-Z]*)',
    re.IGNORECASE
)
# Define column names for the 9 data points (3 channels for each of the 3 keys)
SXDOWN_COLUMNS = ["SATT", "X1ATT", "X2ATT",
                  "SLEVEL", "X1LEVEL", "X2LEVEL",
                  "SLOCK", "X1LOCK", "X2LOCK"]

# ============================================================
# STEP 3: Parse lines for only Thread ID = 13
# ============================================================
sxdown_entries = []

for line in lines:
    line = line.strip()
    m = full_entry_pattern.match(line)
    if m:
        e = m.groupdict()
        if e["thread_id"] == TARGET_THREAD_ID:
            e["data"] = e["data"].replace("，", ",").strip()
            sxdown_entries.append(e)

# ============================================================
# STEP 4: Extract and structure the 48 columns
# ============================================================
parsed_rows = []

for e in sxdown_entries:
    data_str = e["data"]
    extracted_data = {}
    found_blocks = key_value_pattern.findall(data_str)

    for key, values_str in found_blocks:
        key = key.lower()
        values_str = values_str.replace("，", ",")
        if key == "lock":
            vals = re.findall(r'(lck|lc)', values_str, re.IGNORECASE)
        else:
            vals = [v.strip() for v in values_str.split(",") if v.strip()]
        extracted_data[key] = vals

        # Map into the row
        row = {
            "datetime": e["datetime"],
            "code": e["code"],
            "thread_id": e["thread_id"],
            "level": e["level"],
        }

        # ========================================================
        # Mapping 3 values → S, X1, X2
        # ========================================================
        mapping = [
            ("att", "ATT"),
            ("level", "LEVEL"),
            ("lock", "LOCK"),
        ]

        for key, suffix in mapping:
            vals = extracted_data.get(key, [])
            # Ensure always 3 slots
            vals = vals + [None] * (3 - len(vals))

            row[f"S{suffix}"] = vals[0]
            row[f"X1{suffix}"] = vals[1]
            row[f"X2{suffix}"] = vals[2]

        parsed_rows.append(row)

# ============================================================
# STEP 5 & 6: Connect to DB, create table, and insert data
# ============================================================
table_name = "SXDown"

# Connect to the DB (VLBI.test1.db)
conn = sqlite3.connect(db_path)
print(f"\nConnected to DB: {os.path.abspath(db_path)}")

# Construct the value columns for the CREATE TABLE statement
value_cols_sql = ", ".join([f"{col} TEXT" for col in SXDOWN_COLUMNS])

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
if parsed_rows:
    df = pd.DataFrame(parsed_rows)
    # Ensure DataFrame columns match the desired order and include the fixed header columns
    header_cols = ["datetime", "code", "thread_id", "level"]
    all_cols = header_cols + SXDOWN_COLUMNS

    # Reindex the DataFrame to ensure all columns are present and in order
    df = df.reindex(columns=all_cols)

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into **{table_name}** (Thread ID {TARGET_THREAD_ID})")
else:
    print(f"No data found for Thread ID {TARGET_THREAD_ID} to insert.")

conn.close()

print("SX Downconverter data extraction and insertion complete!")