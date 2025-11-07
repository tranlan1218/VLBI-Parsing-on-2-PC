import re
import sqlite3
import pandas as pd
import socket

# ============================================================
# CONFIGURATION
# ============================================================
PC1_IP = "192.168.0.50"
PC1_PORT = 6000

db_path = r"D:\VLBI\PyCharmMiscProject\VLBI.test2.db"
TARGET_THREAD_ID = '11' # Thread ID for Q Downconverter

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
# STEP 2: Define robust SINGLE-LINE regex pattern for [11]
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
# Define column names for the 12 data points (4 channels for each of the 3 keys)
KDOWN_COLUMNS = [f"K{i}ATT" for i in range(1, 5)] + \
                [f"K{i}LEVEL" for i in range(1, 5)] + \
                [f"K{i}LOCK" for i in range(1, 5)]

# ============================================================
# STEP 3: Parse lines for only Thread ID = 11
# ============================================================
kdown_entries = []

for line in lines:
    line = line.strip()
    m = full_entry_pattern.match(line)
    if m:
        e = m.groupdict()
        if e["thread_id"] == TARGET_THREAD_ID:
            e["data"] = e["data"].strip()
            kdown_entries.append(e)

print(f"✅ Parsed entries for Thread {TARGET_THREAD_ID}: {len(kdown_entries)}")

# ============================================================
# STEP 4: Extract and structure the 48 columns
# ============================================================
parsed_rows = []

for e in kdown_entries:
    data_str = e.get("data", "").strip()
    data_str = e["data"].replace("，", ",")  # Normalize commas

    extracted_data = {}
    found_blocks = key_value_pattern.findall(data_str)

    for key, values_str in found_blocks:
        key = key.lower()
        values_str = values_str.replace("，", ",")  # normalize again

        if key == 'lock':
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

        start_ch = 1
        end_ch = 5  # 4 channels

        mapping = [
            ("att", "ATT"),
            ("level", "LEVEL"),
            ("lock", "LOCK"),
        ]

        for key, suffix in mapping:
            vals = extracted_data.get(key, [])
            for i in range(start_ch, end_ch):
                col = f"K{i}{suffix}"
                idx = i - start_ch
                row[col] = vals[idx] if idx < len(vals) else None

        parsed_rows.append(row)

# ============================================================
# STEP 5 & 6: Connect to DB, create table, and insert data
# ============================================================
# Connect to the DB (VLBI.test1.db)
conn = sqlite3.connect(db_path)

table_name = "KDown"

conn.execute(f"DROP TABLE IF EXISTS {table_name}")

#Construct the value columns for the CREATE TABLE statement
value_cols_sql = ", ".join([f"{col} TEXT" for col in KDOWN_COLUMNS])


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
conn.execute(create_table_sql)
conn.commit()

# Insert data using pandas
if parsed_rows:
    df = pd.DataFrame(parsed_rows)
    # Ensure DataFrame columns match the desired order and include the fixed header columns
    header_cols = ["datetime", "code", "thread_id", "level"]
    all_cols = header_cols + KDOWN_COLUMNS

    # Reindex the DataFrame to ensure all columns are present and in order
    df = df.reindex(columns=all_cols)

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into **{table_name}** (Thread ID {TARGET_THREAD_ID})")
else:
    print(f"No data found for Thread ID {TARGET_THREAD_ID} to insert.")

conn.close()

print("K Downconverter data extraction and insertion complete!")