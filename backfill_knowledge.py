#!/usr/bin/env python3
"""
Insert metadata-only project_knowledge rows for any project not yet enriched.
No network calls — just reads from the local DB.
Run once to unblock classify_combined.py fast-path for all remaining projects.
"""
import json, sqlite3, time
from pathlib import Path

DB = "23692652-sq26.db"

conn = sqlite3.connect(DB)

rows = conn.execute("""
    SELECT p.id, p.title, p.description,
           GROUP_CONCAT(DISTINCT k.keyword) AS keywords
    FROM projects p
    LEFT JOIN keywords k ON k.project_id = p.id
    WHERE p.id NOT IN (SELECT project_id FROM project_knowledge)
    GROUP BY p.id
""").fetchall()

print(f"Inserting metadata-only rows for {len(rows):,} projects…")
t0 = time.time()

conn.executemany("""
    INSERT OR IGNORE INTO project_knowledge
        (project_id, title, description, keywords,
         file_names, file_snippets, source_description,
         source_subjects, enriched_text, file_source)
    VALUES (?, ?, ?, ?, '[]', '{}', '', '[]', '', NULL)
""", [(r[0], r[1], r[2], r[3]) for r in rows])

conn.commit()
conn.close()
print(f"Done. {len(rows):,} rows inserted in {time.time()-t0:.1f}s")
