import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/15sz5Quun4k86N-XEXvbXU9D5BrLg_26z7PtuH-T5bP8/export?format=csv&gid=1342397740",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Total rows: {len(rows)}, cols: {len(rows[0])}")

# Block 2 (Season) starts at col 14
# Check headers around col 14
print("\n=== Header row cols 14-24 ===")
h = rows[0]
for i in range(14, min(25, len(h))):
    if h[i].strip():
        print(f"  col {i}: '{h[i].strip()}'")

# Check row 1 cols 14-24
print("\n=== Row 1 cols 14-24 ===")
r1 = rows[1]
for i in range(14, min(25, len(r1))):
    if r1[i].strip():
        print(f"  col {i}: '{r1[i].strip()}'")

# Count non-empty rows in Season block (col 15 = player name)
count = 0
for ri in range(1, len(rows)):
    if len(rows[ri]) > 15 and rows[ri][15].strip():
        count += 1
    else:
        if count > 0:
            break
print(f"\n=== Season block: {count} players ===")

# Show first 5 rows with all cols 14-24
print("\n=== First 5 Season rows ===")
for ri in range(1, 6):
    row = rows[ri]
    data = {i: row[i].strip() for i in range(14, min(25, len(row))) if row[i].strip()}
    print(f"  Row {ri}: {data}")

# Check if there's a team column anywhere
# Look at all headers
print("\n=== All non-empty headers ===")
for i, val in enumerate(h):
    v = val.strip().upper()
    if 'TEAM' in v or 'TM' == v:
        print(f"  col {i}: '{val.strip()}'")
