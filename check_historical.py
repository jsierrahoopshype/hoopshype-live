import requests, csv, io

SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"

# Try default gid=0
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
r = requests.get(url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Rows: {len(rows)}, Cols: {len(rows[0]) if rows else 0}")
print(f"\n=== HEADERS (row 0) ===")
for i, h in enumerate(rows[0][:20]):
    if h.strip():
        print(f"  col {i}: '{h.strip()}'")

print(f"\n=== FIRST 5 DATA ROWS ===")
for ri in range(1, min(6, len(rows))):
    data = {i: rows[ri][i].strip() for i in range(min(20, len(rows[ri]))) if rows[ri][i].strip()}
    print(f"  Row {ri}: {data}")

print(f"\n=== LAST 3 ROWS ===")
for ri in range(max(1, len(rows)-3), len(rows)):
    data = {i: rows[ri][i].strip() for i in range(min(20, len(rows[ri]))) if rows[ri][i].strip()}
    print(f"  Row {ri}: {data}")

# Check year range
years = set()
for ri in range(1, len(rows)):
    for ci in range(min(20, len(rows[ri]))):
        val = rows[ri][ci].strip()
        if val.isdigit() and 1990 <= int(val) <= 2030:
            years.add(int(val))
if years:
    print(f"\n=== YEAR RANGE: {min(years)} - {max(years)} ===")

# Sample some salary values
print(f"\n=== SAMPLE VALUES (checking for $ signs or large numbers) ===")
for ri in [1, 2, 100, 500, 1000]:
    if ri < len(rows):
        vals = [rows[ri][i].strip() for i in range(min(15, len(rows[ri]))) if rows[ri][i].strip()]
        print(f"  Row {ri}: {vals}")
