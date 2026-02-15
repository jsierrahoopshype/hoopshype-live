import requests, csv, io

# Load salary data for team lookup
sal_url = "https://docs.google.com/spreadsheets/d/11llk0icQqoi0JwJXat5KO8y2RQeBMN5rS9FhAY56Idc/export?format=csv&gid=0"
r = requests.get(sal_url, timeout=30)
r.encoding = "utf-8"
sal_lookup = {}  # player -> team
for row in csv.reader(io.StringIO(r.text)):
    if len(row) >= 3 and row[0].strip() and row[1].strip():
        sal_lookup[row[0].strip()] = row[1].strip()

# Load depth chart
dep_url = "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201"
r = requests.get(dep_url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

# Also build abbreviated name map from salary
name_map = {}
for player in sal_lookup:
    parts = player.split(" ", 1)
    if len(parts) == 2:
        abbr = f"{parts[0][0]}. {parts[1]}"
        name_map[abbr] = player

pos_set = {"PG", "SG", "SF", "PF", "C"}
headers = []
for ri, row in enumerate(rows):
    if len(row) >= 5:
        cols = [row[i].strip() for i in range(5)]
        if all(c in pos_set for c in cols):
            headers.append(ri)

print(f"Found {len(headers)} header rows")

for hi, hrow in enumerate(headers):
    end = headers[hi+1] if hi+1 < len(headers) else len(rows)
    # Get first name row (starters)
    starters = []
    for ri in range(hrow+1, min(hrow+3, len(rows))):
        row = rows[ri]
        if len(row) >= 5:
            cols = [row[i].strip() for i in range(5)]
            if any(cols) and not any(c.startswith("$") for c in cols if c):
                starters = cols
                break
    
    # Look up teams
    votes = {}
    for ri in range(hrow+1, end):
        row = rows[ri]
        if len(row) < 5: continue
        cols = [row[i].strip() for i in range(5)]
        if any(c.startswith("$") for c in cols if c): continue
        if all(c in pos_set for c in cols if c): continue
        for c in cols:
            if not c: continue
            full = name_map.get(c, c)
            t = sal_lookup.get(full, sal_lookup.get(c, ""))
            if t:
                votes[t] = votes.get(t, 0) + 1
    
    winner = max(votes, key=votes.get) if votes else "UNKNOWN"
    print(f"  [{hi:2d}] Row {hrow:4d}: {winner:30s}  starters={starters[:3]}")

# Check for Memphis
memphis_players = [p for p, t in sal_lookup.items() if "Memphis" in t]
print(f"\nMemphis players in salary data: {len(memphis_players)}")
print(f"  First 5: {memphis_players[:5]}")
