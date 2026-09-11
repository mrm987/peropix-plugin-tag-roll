# -*- coding: utf-8 -*-
"""색인 내려받기 점검 — 받기 · 이어받기(Range) · sha256 대조 · 멈추기 · 지우기.

  python -X utf8 test_download.py <색인이 있는 폴더>

색인 폴더를 Range 지원 파일 서버로 내보내고 **작은 파일 몇 개만** 받아 본다 (960MB 를 다 받지 않는다).
GitHub 릴리즈 자산은 Range 를 지원하므로 이어받기 경로가 실제와 같다 (stdlib 의 SimpleHTTPRequestHandler 는
Range 를 무시하므로 여기서 206 을 내주는 핸들러를 따로 둔다).
"""
import asyncio
import hashlib
import os
import shutil
import sys
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEST = HERE / "_index-test"
PORT = 8799
SMALL = ["_tag_dict.parquet", "_roll_pairs_solo.npz", "_tag_counts_since2016_e.parquet"]
BIG = "_roll_since2016_offs.npy"   # 멈추기 시험용 (50MB)
ok = fail = 0


def chk(name, cond, extra=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK   {name}")
    else:
        fail += 1
        print(f"  FAIL {name} {extra}")


class Range(SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send_head(self):
        rng = self.headers.get("Range")
        if not rng or not rng.startswith("bytes="):
            return super().send_head()
        p = Path(self.translate_path(self.path))
        if not p.is_file():
            self.send_error(404)
            return None
        start, size = int(rng.split("=")[1].split("-")[0]), p.stat().st_size
        f = p.open("rb")
        f.seek(start)
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size - start))
        self.send_header("Content-Range", f"bytes {start}-{size - 1}/{size}")
        self.end_headers()
        return f


def sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


async def wait_idle(idx, limit=180.0):
    t = 0.0
    while idx._task and not idx._task.done() and t < limit:
        await asyncio.sleep(0.1)
        t += 0.1
    if idx._task:
        await asyncio.gather(idx._task, return_exceptions=True)


async def main(src: Path):
    shutil.rmtree(DEST, ignore_errors=True)
    DEST.mkdir(parents=True)
    os.environ["TAG_ROLL_INDEX"] = str(DEST)
    os.environ["TAG_ROLL_INDEX_URL"] = f"http://127.0.0.1:{PORT}/"
    sys.path.insert(0, str(HERE))
    import index as idx

    want = {f["name"]: f for f in idx.FILES}
    for n in SMALL + [BIG]:
        if not (src / n).is_file():
            print(f"색인 파일이 없습니다: {src / n}", file=sys.stderr)
            return 1
    idx.FILES = [want[n] for n in SMALL]
    idx.TOTAL = sum(f["size"] for f in idx.FILES)

    srv = ThreadingHTTPServer(("127.0.0.1", PORT), partial(Range, directory=str(src)))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        s = idx.status()
        chk("색인이 없으면 ready 가 아니다", not s["ready"] and len(s["missing"]) == len(SMALL), s)
        chk("자리를 알려 준다", s["dir"] == str(DEST), s["dir"])

        idx.start()
        await wait_idle(idx)
        s = idx.status()
        chk("다 받으면 ready", s["ready"] and not s["missing"], s)
        chk("받은 바이트 = 총 바이트", s["have"] == idx.TOTAL, (s["have"], idx.TOTAL))
        chk("오류 없음", not s.get("error"), s.get("error"))
        bad = [f["name"] for f in idx.FILES if sha(DEST / f["name"]) != f["sha256"]]
        chk("sha256 이 전부 맞다", not bad, bad)
        chk(".part 가 남지 않는다", not list(DEST.glob("*.part")))

        # 이어받기 — 반만 받은 조각을 두고 다시 받는다 (Range 206)
        f0 = idx.FILES[0]
        (DEST / f0["name"]).unlink()
        half = f0["size"] // 2
        part = DEST / (f0["name"] + ".part")
        part.write_bytes((src / f0["name"]).read_bytes()[:half])
        s = idx.status()
        chk("받다 만 조각을 진행률에 센다", s["have"] == idx.TOTAL - f0["size"] + half, (s["have"], half))
        idx.start()
        await wait_idle(idx)
        chk("이어받아 완성된다", (DEST / f0["name"]).is_file() and sha(DEST / f0["name"]) == f0["sha256"])

        # 깨진 조각 — 크기는 맞고 내용이 다르면 버린다
        (DEST / f0["name"]).unlink()
        part.write_bytes(b"\0" * (f0["size"] - 10))
        idx.start()
        await wait_idle(idx)
        s = idx.status()
        chk("해시가 다르면 알린다", "sha256" in (s.get("error") or ""), s.get("error"))
        chk("깨진 파일을 두지 않는다", not (DEST / f0["name"]).is_file())

        # 멈추기 — 큰 파일을 받다가 멈춘다
        idx.FILES = [want[BIG]] + idx.FILES
        idx.TOTAL = sum(f["size"] for f in idx.FILES)
        (DEST / f0["name"]).unlink(missing_ok=True)
        part.unlink(missing_ok=True)
        idx.start()
        await asyncio.sleep(0.25)
        idx.cancel()
        await wait_idle(idx)
        s = idx.status()
        chk("멈추면 그렇다고 알린다", s.get("cancelled") is True, s)
        chk("멈춘 파일은 완성하지 않는다", not (DEST / BIG).is_file())
        chk("받은 데까지 조각으로 둔다", (DEST / (BIG + ".part")).is_file())

        # 없는 주소 — 릴리즈에 아직 없을 때의 문구
        os.environ["TAG_ROLL_INDEX_URL"] = f"http://127.0.0.1:{PORT}/없는자리/"
        idx.FILES = [want[SMALL[1]]]
        idx.TOTAL = idx.FILES[0]["size"]
        (DEST / SMALL[1]).unlink(missing_ok=True)
        idx.start()
        await wait_idle(idx)
        err = idx.status().get("error") or ""
        chk("자산이 없으면 릴리즈를 가리켜 알린다", "404" in err and "릴리즈" in err and "RuntimeError" not in err, err[:120])

        os.environ["TAG_ROLL_INDEX_URL"] = f"http://127.0.0.1:{PORT}/"
        idx.FILES = [want[n] for n in SMALL]
        idx.TOTAL = sum(f["size"] for f in idx.FILES)
        idx.start()
        await wait_idle(idx)
        chk("다시 받으면 ready", idx.status()["ready"])
    finally:
        srv.shutdown()
        shutil.rmtree(DEST, ignore_errors=True)
    print(f"\n통과 {ok} · 실패 {fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(asyncio.run(main(Path(sys.argv[1]))))
