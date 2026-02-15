import requests, csv, io

SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
GID = "1151460858"
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
r = requests.get(url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Rows: {len(rows)}, Cols: {len(rows[0]) if rows else 0}")

print(f"\n=== ALL HEADERS ===")
for i, h in enumerate(rows[0]):
    if h.strip():
        print(f"  col {i}: '{h.strip()}'")

print(f"\n=== FIRST 5 DATA ROWS ===")
for ri in range(1, min(6, len(rows))):
    vals = {i: rows[ri][i].strip() for i in range(len(rows[ri])) if rows[ri][i].strip()}
    print(f"  Row {ri}: {vals}")

# Find a 1991 row
print(f"\n=== SAMPLE 1991 ROW ===")
for ri in range(1, len(rows)):
    row = rows[ri]
    for ci in range(len(row)):
        if row[ci].strip() == "1991":
            vals = {i: row[i].strip() for i in range(len(row)) if row[i].strip()}
            print(f"  Row {ri}: {vals}")
            break
    else:
        continue
    break

# Find a recent row (2025)
print(f"\n=== SAMPLE 2025 ROW ===")
for ri in range(1, len(rows)):
    row = rows[ri]
    for ci in range(len(row)):
        if row[ci].strip() == "2025":
            vals = {i: row[i].strip() for i in range(len(row)) if row[i].strip()}
            print(f"  Row {ri}: {vals}")
            break
    else:
        continue
    break
