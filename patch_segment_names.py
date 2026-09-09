#!/usr/bin/env python3
"""Assign segment_name to all existing segments (one-off patch)."""

import mysql.connector

MUNI_SHORT = {
    "002001": "Makkah",   "003001": "Madinah",  "004001": "Riyadh",
    "005001": "Jeddah",   "006001": "Eastern",  "007001": "Asir",
    "008001": "Qassim",   "009001": "Jazan",    "010001": "Jawf",
    "011001": "Tabuk",    "012001": "Hail",     "013001": "Northern",
    "014001": "Baha",     "015001": "Najran",   "016001": "Taif",
    "017001": "Ahsa",     "018001": "Hafar",
}

conn = mysql.connector.connect(
    host="127.0.0.1", port=3306,
    user="pci_user", password="pci_pass", database="pci",
)
cur = conn.cursor(dictionary=True)

cur.execute(
    "SELECT segment_id, municipality, submunicipality, frame_count "
    "FROM segments ORDER BY frame_count DESC"
)
rows = cur.fetchall()

name_counters = {}
updates = []
for row in rows:
    muni = (row["municipality"]    or "").strip()
    sub  = (row["submunicipality"] or "").strip()
    city = MUNI_SHORT.get(muni, muni or "Unknown")
    key  = (muni, sub)
    n    = name_counters.get(key, 0) + 1
    name_counters[key] = n
    sub_part = sub if sub else muni
    name = f"{city}_{sub_part}_seg_{n}"
    updates.append((name, row["segment_id"]))

cur.executemany(
    "UPDATE segments SET segment_name = %s WHERE segment_id = %s", updates
)
conn.commit()
print(f"Named {len(updates)} segments across {len(name_counters)} groups.")
cur.close()
conn.close()
