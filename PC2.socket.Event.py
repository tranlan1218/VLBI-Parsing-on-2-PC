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


# ============================
# STEP 2: Regex to capture ANY log entry (WARN, DEBUG, ERROR)
# ============================
event_pattern = re.compile(
    r'^(?P<datetime>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}),(?P<code>\d{3})\s+'
    r'\[(?P<thread_id>\d+)\]\s+(?P<level>WARN|DEBUG|ERROR)\s*-+\s*(?P<message>.*)',
    re.IGNORECASE
)

# ============================
# STEP 3: Extract events
# ============================
event_rows = []

for line in lines:
    line = line.strip()
    m = event_pattern.match(line)
    if m:
        event_rows.append(m.groupdict())

# ============================
# STEP 4: Insert into SQLite
# ============================
table_name = "Event"

conn = sqlite3.connect(db_path)
print(f"Connected to DB: {db_path}")

# Create Event table
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    datetime TEXT,
    code TEXT,
    thread_id TEXT,
    level TEXT,
    message TEXT
);
"""

conn.execute(f"DROP TABLE IF EXISTS {table_name}")
conn.execute(create_table_sql)
conn.commit()

# Insert data
if event_rows:
    df = pd.DataFrame(event_rows)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"✅ Inserted {len(df)} event rows into {table_name}")
else:
    print("⚠ No WARN/DEBUG/ERROR rows found.")

conn.close()
print("🎉 Event table extraction complete!")
