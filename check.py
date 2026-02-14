import requests, csv, io

r = requests.get(
    "https://docs.google.com/spreadsheets/d/15sz5Quun4k86N-XEXvbXU9D5BrLg_26z7PtuH-T5bP8/export?format=csv&gid=1342397740",
    timeout=30
)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))
h = rows[0]
d = rows[1]

print(f"Total cols: {len(h)}, Total rows: {len(rows)}")
print()

b = 1
for i in range(len(h)):
    if h[i] == "PLAYER":
        cols = h[i:i+8]
        top_player = d[i] if i < len(d) else ""
        top_rat = d[i+1] if i+1 < len(d) else ""
        print(f"Block {b} (col {i}): {cols}")
        print(f"  #1: {top_player} (RAT: {top_rat})")
        print()
        b += 1
