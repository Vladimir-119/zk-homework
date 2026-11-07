import argparse
import collections
import csv
import hashlib
import io
import itertools
import json
from pathlib import Path
import re
import zipfile


SOURCE_URL = "https://www.kaggle.com/datasets/ilyazawilsiv/it-vacancies-from-headhunter-website"
DOWNLOAD_URL = "https://www.kaggle.com/api/v1/datasets/download/ilyazawilsiv/it-vacancies-from-headhunter-website"
FIELDS = [
    "id", "area_id", "area_name", "name", "salary_from", "salary_to",
    "salary_currency", "salary_gross", "experience", "schedule",
    "employment", "published_at", "professional_roles_id",
    "professional_roles_name", "key_skills", "count_key_skills",
]

CPP = re.compile(r"(?i)(?<!\w)[cс]\+\+(?!\+)")
ONE_C = re.compile(r"(?i)(?<!\w)1[\s-]*[cс](?!\w)")
OBJECTIVE_C = re.compile(r"(?i)objective[\s-]*[cс]")
# An uppercase single C/С; exclude initials, C#, and C++.
BARE_C = re.compile(r"(?<![\w.])[CС](?![\w.#+]|\s*#)")
SI = re.compile(r"(?i)(?<!\w)си(?!\w)")
LOWER_C_DEVELOPER = re.compile(r"(?<!\w)c\s+(?:developer|programmer)(?!\w)")


def is_variant(row):
    """Conservative title-based C/C++ selection within developer role 96.

    Do not select generic developer titles just because they list C++ skills.
    This is a reproducible operational definition, not an official segment tag.
    """
    if row["professional_roles_id"] != "96":
        return False
    title = row["name"]
    if CPP.search(title):
        return True
    title = OBJECTIVE_C.sub("", ONE_C.sub("", title))
    return bool(BARE_C.search(title) or SI.search(title) or LOWER_C_DEVELOPER.search(title))


def parse_skills(value):
    """Parse the PostgreSQL-style array used by the source CSV."""
    if not value or value == "{}":
        return []
    if not value.startswith("{") or not value.endswith("}"):
        raise ValueError(f"Unexpected skill array: {value!r}")
    return next(csv.reader([value[1:-1]], delimiter=",", quotechar='"', escapechar="\\"))


def audit(rows):
    selected = [row for row in rows if is_variant(row)]
    by_region = collections.defaultdict(collections.Counter)
    paid_sets = {2: collections.Counter(), 3: collections.Counter()}
    bad_skill_counts = []
    for row in rows:
        skills = parse_skills(row["key_skills"])
        if len(skills) != int(row["count_key_skills"]):
            bad_skill_counts.append(row["id"])
    if bad_skill_counts:
        raise ValueError(f"Skill count mismatch: {bad_skill_counts[:10]}")
    for row in selected:
        counts = by_region[row["area_name"]]
        counts["vacancies"] += 1
        skills = sorted(set(s.strip().casefold() for s in parse_skills(row["key_skills"]) if s.strip()))
        if skills:
            counts["with_skills"] += 1
        if row["salary_currency"] == "RUR" and (row["salary_from"] or row["salary_to"]):
            counts["with_rur_salary"] += 1
            counts[f"rur_gross_{row['salary_gross'] or 'unknown'}"] += 1
            if skills:
                counts["with_skills_and_rur_salary"] += 1
            for size in (2, 3):
                paid_sets[size].update(itertools.combinations(skills, size))
    return {
        "total_rows": len(rows),
        "unique_ids": len({row["id"] for row in rows}),
        "published_from": min(row["published_at"] for row in rows),
        "published_to": max(row["published_at"] for row in rows),
        "variant_rows": len(selected),
        "variant_with_skills": sum(c["with_skills"] for c in by_region.values()),
        "variant_with_rur_salary": sum(c["with_rur_salary"] for c in by_region.values()),
        "variant_with_skills_and_rur_salary": sum(c["with_skills_and_rur_salary"] for c in by_region.values()),
        "regions": dict(sorted(by_region.items(), key=lambda x: (-x[1]["vacancies"], x[0]))),
        "salary_support_at_least_10": {
            str(size): sum(n >= 10 for n in counter.values()) for size, counter in paid_sets.items()
        },
        "skill_count_mismatches": len(bad_skill_counts),
        "note": "Source audit only. Salary counts include both gross and net; not a salary comparison. No S3 benchmark has been run.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_zip", type=Path)
    parser.add_argument("--output", type=Path, default=Path("."))
    args = parser.parse_args()
    csv.field_size_limit(10_000_000)
    args.output.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.source_zip) as source, source.open("IT_vacancies.csv") as raw:
        rows = list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")))
    result = audit(rows)
    # All 68,770 source records are retained; values of these fields are unchanged.
    target = args.output / "hh_it_vacancies.csv"
    with target.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    with args.source_zip.open("rb") as source_file, target.open("rb") as target_file:
        source_hash = hashlib.file_digest(source_file, "sha256").hexdigest()
        projection_hash = hashlib.file_digest(target_file, "sha256").hexdigest()
    provenance = {
        "source_page": SOURCE_URL,
        "download_url": DOWNLOAD_URL,
        "source_file": "IT_vacancies.csv",
        "license_from_kaggle_metadata": "Apache 2.0",
        "source_zip_sha256": source_hash,
        "projection_sha256": projection_hash,
        "projection_fields": FIELDS,
        "transformation": "Column projection only; all source rows and original field values retained. No invented or extracted skills; no imputed salaries.",
    }
    (args.output / "provenance.json").write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "source_audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("projection_bytes:", target.stat().st_size)


if __name__ == "__main__":
    main()
