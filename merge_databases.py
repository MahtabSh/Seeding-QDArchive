"""
Merge prev-schema/metadata.sqlite into metadata.sqlite (new DB).

Rules:
- If a project_url already exists in new DB → keep new DB row (it may have gdrive data).
- If a project_url only exists in prev DB → insert it into new DB, along with its
  related keywords, person_role, licenses, and project_files rows.
"""

import sqlite3
import shutil
import os

NEW_DB  = "metadata.sqlite"
PREV_DB = "prev-schema/metadata.sqlite"
OUT_DB  = "23692652-sq26.db"

# ── 1. Copy new DB as the output ─────────────────────────────────────────────
print(f"Copying {NEW_DB} → {OUT_DB} …")
shutil.copy2(NEW_DB, OUT_DB)

conn = sqlite3.connect(OUT_DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ── 2. Attach prev DB ─────────────────────────────────────────────────────────
cur.execute(f"ATTACH DATABASE '{PREV_DB}' AS prev")

# ── 3. Find projects in prev that are NOT in new (by project_url) ─────────────
cur.execute("""
    SELECT p.*
    FROM   prev.projects p
    WHERE  NOT EXISTS (
        SELECT 1 FROM main.projects n WHERE n.project_url = p.project_url
    )
""")
prev_only = cur.fetchall()
print(f"Projects only in prev DB: {len(prev_only):,}")

# Build a mapping old_id → new_id for inserted projects
old_to_new_id = {}

for row in prev_only:
    old_id = row["id"]
    cur.execute("""
        INSERT INTO main.projects (
            query_string, repository_id, repository_url, project_url,
            version, title, description, language, doi,
            upload_date, download_date,
            download_repository_folder, download_project_folder, download_version_folder,
            download_method,
            gdrive_folder_id, gdrive_folder_url
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)
    """, (
        row["query_string"], row["repository_id"], row["repository_url"],
        row["project_url"], row["version"], row["title"], row["description"],
        row["language"], row["doi"], row["upload_date"], row["download_date"],
        row["download_repository_folder"], row["download_project_folder"],
        row["download_version_folder"], row["download_method"],
    ))
    new_id = cur.lastrowid
    old_to_new_id[old_id] = new_id

print(f"Inserted {len(old_to_new_id):,} projects from prev DB.")

# ── 4. Copy related rows ───────────────────────────────────────────────────────
if old_to_new_id:
    old_ids = list(old_to_new_id.keys())

    # --- keywords ---
    placeholders = ",".join("?" * len(old_ids))
    cur.execute(f"SELECT * FROM prev.keywords WHERE project_id IN ({placeholders})", old_ids)
    kw_rows = cur.fetchall()
    kw_inserted = 0
    for r in kw_rows:
        new_pid = old_to_new_id.get(r["project_id"])
        if new_pid:
            cur.execute("INSERT INTO main.keywords (project_id, keyword) VALUES (?,?)",
                        (new_pid, r["keyword"]))
            kw_inserted += 1
    print(f"Inserted {kw_inserted:,} keywords.")

    # --- person_role ---
    cur.execute(f"SELECT * FROM prev.person_role WHERE project_id IN ({placeholders})", old_ids)
    pr_rows = cur.fetchall()
    pr_inserted = 0
    for r in pr_rows:
        new_pid = old_to_new_id.get(r["project_id"])
        if new_pid:
            cur.execute("INSERT INTO main.person_role (project_id, name, role) VALUES (?,?,?)",
                        (new_pid, r["name"], r["role"]))
            pr_inserted += 1
    print(f"Inserted {pr_inserted:,} person_role rows.")

    # --- licenses ---
    cur.execute(f"SELECT * FROM prev.licenses WHERE project_id IN ({placeholders})", old_ids)
    lic_rows = cur.fetchall()
    lic_inserted = 0
    for r in lic_rows:
        new_pid = old_to_new_id.get(r["project_id"])
        if new_pid:
            cur.execute("INSERT INTO main.licenses (project_id, license) VALUES (?,?)",
                        (new_pid, r["license"]))
            lic_inserted += 1
    print(f"Inserted {lic_inserted:,} license rows.")

    # --- project_files ---
    # prev schema does NOT have gdrive_file_id / gdrive_url — insert with NULLs
    cur.execute(f"SELECT * FROM prev.project_files WHERE project_id IN ({placeholders})", old_ids)
    pf_rows = cur.fetchall()
    pf_inserted = 0
    for r in pf_rows:
        new_pid = old_to_new_id.get(r["project_id"])
        if new_pid:
            # prev schema has no file_size_bytes column
            file_size = dict(r).get("file_size_bytes", None)
            cur.execute("""
                INSERT INTO main.project_files
                    (project_id, file_name, file_type, file_size_bytes, status,
                     gdrive_file_id, gdrive_url)
                VALUES (?,?,?,?,?,NULL,NULL)
            """, (new_pid, r["file_name"], r["file_type"], file_size, r["status"]))
            pf_inserted += 1
    print(f"Inserted {pf_inserted:,} project_files rows.")

# ── 5. Commit and report ──────────────────────────────────────────────────────
conn.commit()

# Final counts
for table in ("projects", "keywords", "person_role", "licenses", "project_files"):
    cur.execute(f"SELECT COUNT(*) FROM main.{table}")
    n = cur.fetchone()[0]
    print(f"  {table}: {n:,} rows")

conn.close()
print(f"\nDone. Merged database written to: {OUT_DB}")
