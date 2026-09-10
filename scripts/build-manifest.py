# -*- coding: utf-8 -*-
"""색인 매니페스트(`index-manifest.json`)를 만든다 — 색인을 새로 만들었을 때만 돌린다.

  python -X utf8 scripts/build-manifest.py <색인 폴더> [--release index-2026-05-18]

매니페스트에 드는 것 둘.
  · `files` — 내려받을 파일의 이름·크기·sha256. 플러그인이 이것으로 받고 대조한다.
  · `consts` — 덤프에서 한 번 재는 상수 넷(`id_since` 와 등급별 게시물 수 셋). 색인 판과 짝이 맞아야
    하므로 여기 함께 적는다. 덤프가 없으면 기존 매니페스트의 값을 그대로 물려받는다.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
OUT = HERE / "index-manifest.json"
SINCE = "2016-01-01"

# 색인을 돌리는 데 필요한 파일 전부 (원본 표 `_tags_long.parquet` 와 덤프는 뺀다 — 색인을 만들 때만 쓴다)
FILES = [
    "_tag_dict.parquet",
    "_roll_since2016.duckdb",
    "_roll_since2016_tids.npy",
    "_roll_since2016_offs.npy",
    "_roll_since2016_upid.npy",
    "_roll_since2016_rating.npy",
    "_roll_since2016_genre_v2.npy",
    "_roll_since2016_costume.npy",
    "_roll_since2016_char.npy",
    "_roll_pairs_solo.npz",
    "_tag_counts_since2016_all.parquet",
    "_tag_counts_since2016_gs.parquet",
    "_tag_counts_since2016_e.parquet",
]


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def consts_from_dump(d: Path) -> dict | None:
    db = d / "danbooru.duckdb"
    if not db.is_file():
        return None
    import duckdb

    c = duckdb.connect(str(db), read_only=True)
    ids = int(c.execute(f"SELECT min(id) FROM post WHERE created_at >= '{SINCE}'").fetchone()[0])
    rf = {"all": "1=1", "gs": "rating IN ('g','s')", "e": "rating = 'e'"}
    n = {k: int(c.execute(f"SELECT count(*) FROM post WHERE {v} AND id >= {ids}").fetchone()[0]) for k, v in rf.items()}
    return {"id_since": ids, "nposts": n}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", help="색인 파일들이 있는 폴더")
    ap.add_argument("--release", default="", help="색인을 올릴 릴리즈 태그 (기본: 기존 값 유지)")
    a = ap.parse_args()
    d = Path(a.dir)
    old = json.loads(OUT.read_text(encoding="utf-8")) if OUT.is_file() else {}

    missing = [f for f in FILES if not (d / f).is_file()]
    if missing:
        print("색인 파일이 없습니다: " + ", ".join(missing), file=sys.stderr)
        return 1

    files, total = [], 0
    for name in FILES:
        p = d / name
        print(f"  {name} … ", end="", flush=True)
        files.append({"name": name, "size": p.stat().st_size, "sha256": sha256(p)})
        total += p.stat().st_size
        print(f"{p.stat().st_size / 1e6:,.1f}MB")

    consts = consts_from_dump(d) or old.get("consts")
    if not consts:
        print("상수를 구할 덤프(danbooru.duckdb)도, 물려받을 기존 매니페스트도 없습니다", file=sys.stderr)
        return 1

    out = {
        "index_version": a.release or old.get("index_version") or "",
        "release": a.release or old.get("release") or "",
        "repo": old.get("repo") or "mrm987/peropix-plugin-tag-roll",
        "total": total,
        "consts": consts,
        "files": files,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT.name}: 파일 {len(files)}개 · 합계 {total / 1e6:,.0f}MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
