import requests, csv, io

SHEET_ID = "14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY"
GID = "2081598055"
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
r = requests.get(url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Rows: {len(rows)}, Cols: {len(rows[0]) if rows else 0}")
print(f"\n=== ALL HEADERS ===")
for i, h in enumerate(rows[0]):
    if h.strip():
        print(f"  col {i}: '{h.strip()}'")

print(f"\n=== FIRST 15 DATA ROWS ===")
for ri in range(1, min(16, len(rows))):
    print(f"\nRow {ri}:")
    for ci in range(len(rows[ri])):
        val = rows[ri][ci].strip()
        if val:
            hdr = rows[0][ci].strip() if ci < len(rows[0]) and rows[0][ci].strip() else f"col{ci}"
            print(f"  {ci} ({hdr}): {val}")
