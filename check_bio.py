import requests, csv, io

SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
GID = "1488063724"
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
r = requests.get(url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Rows: {len(rows)}, Cols: {len(rows[0])}")
print(f"\n=== ALL HEADERS ===")
for i, h in enumerate(rows[0]):
    if h.strip():
        print(f"  col {i}: '{h.strip()}'")

print(f"\n=== FIRST 5 DATA ROWS ===")
for ri in range(1, min(6, len(rows))):
    print(f"\nRow {ri}:")
    for ci in range(len(rows[ri])):
        val = rows[ri][ci].strip()
        if val:
            hdr = rows[0][ci].strip() if ci < len(rows[0]) and rows[0][ci].strip() else f"col{ci}"
            print(f"  {ci} ({hdr}): {val}")
