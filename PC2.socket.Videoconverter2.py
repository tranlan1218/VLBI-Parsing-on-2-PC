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
TARGET_THREAD_ID = '4' # Thread ID for IF Selector

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
# STEP 2: Regex — Single line parser
# ============================================================
full_entry_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+\[(?P<thread_id>\d+)]\s+(?P<level>INFO)\s*-+\s*(?P<message>.*?)[:\s-]*(?P<data>.*)',
)

# ✅ FIXED: fully capture data sequences after "="
key_value_pattern = re.compile(
    r'(att|frqall|levell|levelu|lock)=([\+\-\d\.,a-zA-Z]*)',
    re.IGNORECASE
)

# ============================================================
# CHANNEL COLUMN NAMES (8 channels)
# ============================================================
VIDEOCONVERTER2_COLUMNS = [f"CH{i}ATT" for i in range(9, 17)] + \
                          [f"CH{i}FRQ" for i in range(9, 17)] + \
                          [f"CH{i}LEVELL" for i in range(9, 17)] + \
                          [f"CH{i}LEVELU" for i in range(9, 17)] + \
                          [f"CH{i}LOCK" for i in range(9, 17)]

# ============================================================
# STEP 3: Parse lines for only Thread ID = 4
# ============================================================
vc2_entries = []

for line in lines:
    line = line.strip()
    m = full_entry_pattern.match(line)
    if m:
        e = m.groupdict()
        if e["thread_id"] == TARGET_THREAD_ID:
            e["data"] = e["data"].strip()
            vc2_entries.append(e)

print(f"✅ Parsed entries for Thread {TARGET_THREAD_ID}: {len(vc2_entries)}")

# ============================================================
# STEP 4: Extract key-value groups into structured columns
# ============================================================
parsed_rows = []

for e in vc2_entries:
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

    start_ch = 9
    end_ch = 17  # 9–16 inclusive = 8 channels

    mapping = [
        ("att", "ATT"),
        ("frqall", "FRQ"),
        ("levell", "LEVELL"),
        ("levelu", "LEVELU"),
        ("lock", "LOCK"),
    ]

    for key, suffix in mapping:
        vals = extracted_data.get(key, [])
        for i in range(start_ch, end_ch):
            col = f"CH{i}{suffix}"
            idx = i - start_ch
            row[col] = vals[idx] if idx < len(vals) else None

    parsed_rows.append(row)

print(f"Total rows prepared: {len(parsed_rows)}")

# ============================================================
# STEP 5–6: SQLite Insert with Dynamic Table Creation
# ============================================================
table_name = "VideoConverter2"
conn = sqlite3.connect(db_path)
print(f"Connected DB: {db_path}")

conn.execute(f"DROP TABLE IF EXISTS {table_name}")

value_cols_sql = ", ".join([f"{c} TEXT" for c in VIDEOCONVERTER2_COLUMNS])

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime TEXT,
    code TEXT,
    thread_id TEXT,
    level TEXT,
    {value_cols_sql}
);
"""
conn.execute(create_table_sql)
conn.commit()

if parsed_rows:
    df = pd.DataFrame(parsed_rows)
    df = df.reindex(columns=["datetime", "code", "thread_id", "level"] + VIDEOCONVERTER2_COLUMNS)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into {table_name}")
else:
    print("⚠ No rows to insert!")

conn.close()
print("DONE — All values successfully inserted!")



