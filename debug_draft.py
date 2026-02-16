import requests, csv, io

# 1. Check bio sheet for LeBron, CP3, Lowry
BIO_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
BIO_GID = "1488063724"
bio_url = f"https://docs.google.com/spreadsheets/d/{BIO_ID}/export?format=csv&gid={BIO_GID}"
r = requests.get(bio_url, timeout=30)
r.encoding = "utf-8"
bio_rows = list(csv.reader(io.StringIO(r.text)))

targets = ["LeBron James", "Chris Paul", "Kyle Lowry", "Dwyane Wade", "Carmelo Anthony"]
print("=== BIO SHEET MATCHES ===")
for row in bio_rows[1:]:
    name = row[0].strip()
    draft = row[9].strip() if len(row) > 9 else ""
    if any(t.lower() in name.lower() for t in targets):
        print(f"  '{name}' → draft={draft}")

# 2. Check ratings sheet for these players
RAT_ID = "15sz5Quun4k86N-XEXvbXU9D5BrLg_26z7PtuH-T5bP8"
RAT_GID = "1342397740"
rat_url = f"https://docs.google.com/spreadsheets/d/{RAT_ID}/export?format=csv&gid={RAT_GID}"
r2 = requests.get(rat_url, timeout=30)
r2.encoding = "utf-8"
rat_rows = list(csv.reader(io.StringIO(r2.text)))

print(f"\n=== RATINGS SHEET: {len(rat_rows)} rows, {len(rat_rows[0])} cols ===")
# Check all 7 blocks
blocks = [3, 14, 25, 36, 47, 58, 80]
all_rated = set()
for sc in blocks:
    count = 0
    for row in rat_rows[1:]:
        if sc < len(row) and row[sc].strip():
            all_rated.add(row[sc].strip())
            count += 1
    print(f"  Block col {sc}: {count} players")

print(f"\n  Total unique rated players: {len(all_rated)}")

print("\n=== TARGET PLAYERS IN RATINGS? ===")
for t in targets:
    found = t in all_rated
    # Also check fuzzy
    partial = [n for n in all_rated if t.lower() in n.lower()]
    print(f"  '{t}': exact={found}, partial={partial}")

# 3. Check draft years that ARE matched
player_draft = {}
for row in bio_rows[1:]:
    name = row[0].strip()
    draft = row[9].strip()
    if name and draft.isdigit():
        player_draft[name] = int(draft)

matched = 0
unmatched_names = []
for name in all_rated:
    if name in player_draft:
        matched += 1
    else:
        unmatched_names.append(name)

print(f"\n=== MATCH STATS ===")
print(f"  Rated players: {len(all_rated)}")
print(f"  Matched to bio: {matched}")
print(f"  Unmatched: {len(unmatched_names)}")
if unmatched_names:
    print(f"  Unmatched names: {unmatched_names[:30]}")
