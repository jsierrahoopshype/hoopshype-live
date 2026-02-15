import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

# Find ALL rows where cols 0-4 look like position headers
pos_set = {"PG", "SG", "SF", "PF", "C"}
headers = []
for ri, row in enumerate(rows):
    if len(row) < 5:
        continue
    cols = [row[i].strip() for i in range(5)]
    match_count = sum(1 for c in cols if c in pos_set)
    if match_count >= 3:
        headers.append((ri, cols, match_count))

print(f"Total header-like rows: {len(headers)}")
for i, (ri, cols, mc) in enumerate(headers[:5]):
    print(f"  [{i}] Row {ri}: {cols}  (matched {mc}/5)")
print("  ...")
for i, (ri, cols, mc) in enumerate(headers[-3:], len(headers)-3):
    print(f"  [{i}] Row {ri}: {cols}  (matched {mc}/5)")

# Show the FIRST data row after each header (should be starters)
import re
TEAMS = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
    "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "LA Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
    "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards",
]

# Also find team names from col 19
team_markers = {}
for ri, row in enumerate(rows):
    if len(row) > 19 and row[19]:
        m = re.search(r'<strong>(.*?)</strong>', row[19])
        if m:
            name = m.group(1).strip()
            if name == name.upper() and len(name) > 5 and " " in name:
                team_markers[ri] = name

print(f"\nTeam markers from col 19: {len(team_markers)}")
for ri, name in sorted(team_markers.items())[:5]:
    print(f"  Row {ri}: {name}")

# Now check: for each header row, what's the closest team marker?
print("\n=== Header → nearest team marker ===")
for i, (ri, cols, mc) in enumerate(headers[:35]):
    # Find nearest team marker at or after this header
    nearest = None
    for mr in sorted(team_markers.keys()):
        if mr >= ri:
            nearest = (mr, team_markers[mr])
            break
    # First data row
    first_data = ""
    for dr in range(ri+1, min(ri+3, len(rows))):
        if len(rows[dr]) >= 5:
            d = [rows[dr][j].strip() for j in range(5)]
            if any(d) and not any(c.startswith('$') for c in d if c):
                first_data = d
                break
    
    assigned = TEAMS[i-1] if i > 0 and i-1 < len(TEAMS) else "N/A (table header)"
    print(f"  [{i}] Row {ri}: starters={first_data}  |  col19={nearest}  |  assigned={assigned}")
