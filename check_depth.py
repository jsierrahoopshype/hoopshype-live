import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Total cols: {len(rows[0])}, Total rows: {len(rows)}")
print()

# Print header with column indices
h = rows[0]
print("=== HEADER (non-empty cols) ===")
for i, val in enumerate(h):
    if val.strip():
        print(f"  col {i}: {val}")

print()
print("=== First 10 data rows, all non-empty cols ===")
for ri in range(1, min(11, len(rows))):
    row = rows[ri]
    chunk = [f"{i}:{row[i]}" for i in range(min(len(row), 40)) if i < len(row) and row[i].strip()]
    print(f"  Row {ri}: {chunk}")
