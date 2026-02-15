import requests, csv, io

SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid=0"
r = requests.get(url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"=== ALL {len(rows[0])} HEADERS ===")
for i, h in enumerate(rows[0]):
    print(f"  col {i}: '{h.strip()}'")

# Find 2025 rows
print(f"\n=== FIRST 3 ROWS WITH YEAR=2025 (all cols) ===")
count = 0
for ri in range(1, len(rows)):
    if len(rows[ri]) > 4 and rows[ri][4].strip() == "2025":
        print(f"\nRow {ri}:")
        for ci in range(len(rows[ri])):
            val = rows[ri][ci].strip()
            if val:
                hdr = rows[0][ci].strip() if ci < len(rows[0]) else f"col{ci}"
                print(f"  {ci} ({hdr}): {val}")
        count += 1
        if count >= 3:
            break

print(f"\n=== COUNT OF 2025 ROWS ===")
count_2025 = sum(1 for r in rows[1:] if len(r) > 4 and r[4].strip() == "2025")
print(f"  {count_2025} players in 2025")
