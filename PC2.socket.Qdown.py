import re
import sqlite3
import pandas as pd
import socket
import os

# ============================================================
# CONFIGURATION
# ============================================================
PC1_IP = "192.168.0.50"
PC1_PORT = 6000

db_path = r"D:\VLBI\PyCharmMiscProject\VLBI.test2.db"
TARGET_THREAD_ID = '14' # Thread ID for Q Downconverter

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
# STEP 2: Define robust SINGLE-LINE regex pattern for [14]
# ============================================================

full_entry_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+\[(?P<thread_id>\d+)\]\s+(?P<level>INFO)\s*-+\s*(?P<message>.*?)[:\s-]*(?P<data>.*)',
)

key_value_pattern = re.compile(
    r'(att|level|lock)=([\+\-\d\.,a-zA-Z]*)',
    re.IGNORECASE
)

QDOWN_COLUMNS = [f"Q{i}ATT" for i in range(1, 5)] + \
                [f"Q{i}LEVEL" for i in range(1, 5)] + \
                [f"Q{i}LOCK" for i in range(1, 5)]

# ============================================================
# STEP 3: Parse only Thread 14
# ============================================================
qdown_entries = []

for line in lines:
    m = full_entry_pattern.match(line.strip())
    if m:
        e = m.groupdict()
        if e["thread_id"] == TARGET_THREAD_ID:
            e["data"] = e["data"].strip()
            qdown_entries.append(e)

print(f"✅ Parsed entries for Thread 14: {len(qdown_entries)}")

# ============================================================
# STEP 4: Extract and structure 48 columns
# ============================================================

parsed_rows = []

for e in qdown_entries:
    data_str = e.get("data", "").replace("，", ",")
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

    row = {
        "datetime": e["datetime"],
        "code": e["code"],
        "thread_id": e["thread_id"],
        "level": e["level"],
    }

    for key, suffix in [("att", "ATT"), ("level", "LEVEL"), ("lock", "LOCK")]:
        vals = extracted_data.get(key, [])
        for i in range(1, 5):
            col = f"Q{i}{suffix}"
            row[col] = vals[i - 1] if i <= len(vals) else None

    parsed_rows.append(row)

# ============================================================
# STEP 5: INSERT INTO SQLITE
# ============================================================
conn = sqlite3.connect(db_path)

table_name = "QDown"

conn.execute(f"DROP TABLE IF EXISTS {table_name}")

value_cols_sql = ", ".join([f"{col} TEXT" for col in QDOWN_COLUMNS])

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
    header_cols = ["datetime", "code", "thread_id", "level"]
    all_cols = header_cols + QDOWN_COLUMNS
    df = df.reindex(columns=all_cols)

    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df)} rows into {table_name}")

conn.close()
print("✅ QDown parsing & DB insertion complete")
