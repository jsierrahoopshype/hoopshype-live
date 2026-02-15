import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

print(f"Total rows: {len(rows)}")
print()

# Find team names from col 19 (HTML tags with team names)
import re
teams_found = []
for ri, row in enumerate(rows):
    if len(row) > 19 and row[19]:
        match = re.search(r'<strong>(.*?)</strong>', row[19])
        if match:
            teams_found.append((ri, match.group(1)))

print(f"Teams found: {len(teams_found)}")
for ri, name in teams_found[:5]:
    print(f"  Row {ri}: {name}")
print("  ...")
for ri, name in teams_found[-3:]:
    print(f"  Row {ri}: {name}")

print()
print("=== First team (Atlanta) detail, rows around team marker ===")
if teams_found:
    start = teams_found[0][0]
    # Show cols 0-5 for rows around the team
    for ri in range(max(0, start-2), min(len(rows), start+20)):
        row = rows[ri]
        c05 = [row[i].strip() if i < len(row) else '' for i in range(6)]
        # Skip if all empty
        if any(c05[:5]):
            print(f"  Row {ri}: {c05[:5]}")

print()
print("=== Rows between team 1 and team 2 ===")
if len(teams_found) >= 2:
    r1, r2 = teams_found[0][0], teams_found[1][0]
    print(f"  Team 1 at row {r1}, Team 2 at row {r2}, gap = {r2-r1} rows")
    # Show all non-empty rows in cols 0-5
    for ri in range(r1-1, r2+2):
        row = rows[ri]
        c05 = [row[i].strip() if i < len(row) else '' for i in range(6)]
        if any(c05[:5]):
            is_salary = c05[0].startswith('$')
            print(f"  Row {ri}: {'[$]' if is_salary else '[N]'} {c05[:5]}")
