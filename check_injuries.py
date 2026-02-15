import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=306285159",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Total cols: {len(rows[0])}, Total rows: {len(rows)}")
print()

# Print header with column indices
h = rows[0]
for i, val in enumerate(h):
    if val.strip():
        print(f"  col {i}: {val}")

print()
print("--- First 5 data rows, cols 17-25 (Atlanta section?) ---")
for ri in range(1, 6):
    row = rows[ri]
    chunk = [f"{i}:{row[i]}" for i in range(17, min(26, len(row))) if row[i].strip()]
    print(f"  Row {ri}: {chunk}")

print()
print("--- First 5 data rows, cols 25-40 ---")
for ri in range(1, 6):
    row = rows[ri]
    chunk = [f"{i}:{row[i]}" for i in range(25, min(42, len(row))) if row[i].strip()]
    print(f"  Row {ri}: {chunk}")

print()
print("--- First 5 data rows, cols 40-55 ---")
for ri in range(0, 6):
    row = rows[ri]
    chunk = [f"{i}:{row[i]}" for i in range(40, min(56, len(row))) if row[i].strip()]
    print(f"  Row {ri}: {chunk}")
