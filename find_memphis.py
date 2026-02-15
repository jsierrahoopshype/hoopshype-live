import requests, csv, io

dep_url = "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201"
r = requests.get(dep_url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

pos_set = {"PG", "SG", "SF", "PF", "C"}

# Find exact matches
exact = []
almost = []
for ri, row in enumerate(rows):
    if len(row) < 5: continue
    cols = [row[i].strip() for i in range(5)]
    match = sum(1 for c in cols if c in pos_set)
    if match == 5:
        exact.append(ri)
    elif match >= 3:
        almost.append((ri, cols, match))

print(f"Exact 5/5 matches: {len(exact)} rows")
for r in exact:
    print(f"  Row {r}")

print(f"\nAlmost matches (3-4): {len(almost)} rows")
for ri, cols, mc in almost:
    print(f"  Row {ri}: {cols} ({mc}/5)")
    # Show repr of each cell for hidden chars
    raw = [repr(rows[ri][i]) for i in range(5)]
    print(f"    raw: {raw}")

# Check rows between LA Lakers (block 13, ~row 286) and Miami (block 14, ~row 330)
print(f"\n=== Rows 300-315 (where Memphis should be) ===")
for ri in range(300, min(316, len(rows))):
    row = rows[ri]
    cols = [row[i].strip() if i < len(row) else "" for i in range(5)]
    if any(cols):
        raw = [repr(row[i]) if i < len(row) else "" for i in range(5)]
        print(f"  Row {ri}: {cols}  raw={raw}")
