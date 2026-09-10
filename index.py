# -*- coding: utf-8 -*-
"""색인(약 960MB)의 자리와 내려받기.

★★색인은 **플러그인 폴더 밖**에 둔다 — `<앱 뿌리>/data/plugins/tag-roll/`.
  플러그인을 업데이트하면 앱이 옛 폴더를 `_old-tag-roll-<시각>` 으로 물리고 새 것을 놓으므로
  (`backend/plugins.py` `install`), 색인을 플러그인 폴더 안에 두면 판을 올릴 때마다 960MB 를
  다시 받게 된다. 삭제도 같다.
★내려받는 자리는 제작자 저장소의 릴리즈다 (`index-manifest.json` 의 `repo`·`release`).
  GitHub 릴리즈 자산은 파일당 2GiB 미만·1,000개까지이고 총량·전송량 제한이 없다.
★파일마다 크기·sha256 을 대조한다. 받다 끊기면 `.part` 를 남기고 다음에 Range 로 이어 받는다.
★★엔진은 import 하는 순간 색인을 열고 데운다(10초 안팎). 그래서 이 모듈이 **색인이 준비된 뒤에
  스레드에서** import 한다 — 백엔드 기동과 이벤트 루프를 막지 않는다.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from pathlib import Path

HERE = Path(__file__).parent
MANIFEST = json.loads((HERE / "index-manifest.json").read_text(encoding="utf-8"))
FILES: list[dict] = MANIFEST["files"]
TOTAL: int = int(MANIFEST.get("total") or sum(f["size"] for f in FILES))

_engine = None          # import 한 엔진 모듈
_task = None            # 내려받는 중인 작업
_cancel = False
_prog: dict = {}        # {done, total, name, started, error, cancelled}


def dirpath() -> Path:
    """색인 폴더. 환경 변수 `TAG_ROLL_INDEX` 가 있으면 그것을 쓴다 (개발 중 이미 만들어 둔 색인을 가리킬 때)."""
    env = os.environ.get("TAG_ROLL_INDEX")
    if env:
        return Path(env)
    try:
        from plugins import host   # 앱 안: 앱 뿌리를 host 가 준다

        if host.app_dir:
            return Path(host.app_dir) / "data" / "plugins" / "tag-roll"
    except Exception:  # noqa: BLE001 — 앱 밖에서 홀로 부를 때
        pass
    return HERE / "_index"


def _have(d: Path, f: dict) -> bool:
    p = d / f["name"]
    return p.is_file() and p.stat().st_size == f["size"]


def missing(d: Path | None = None) -> list[dict]:
    d = d or dirpath()
    return [f for f in FILES if not _have(d, f)]


def status() -> dict:
    """화면이 500ms 마다 부르는 상태. 준비됐으면 `ready`, 아니면 무엇이 없는지·받는 중인지."""
    d = dirpath()
    miss = missing(d)
    # ★받은 바이트는 **한 곳에서만** 센다 — 다 받은 파일 + 받다 만 `.part`. 화면은 이 값 하나로 진행률을 그린다
    #   (내려받기 작업이 따로 세는 값을 함께 쓰면 이번 회에 끝난 파일이 두 번 세어진다).
    have_bytes = sum(f["size"] for f in FILES if _have(d, f))
    for f in miss:
        part = d / (f["name"] + ".part")
        if part.is_file():
            have_bytes += min(part.stat().st_size, f["size"])
    out = {
        "dir": str(d),
        "ready": not miss,
        "loaded": _engine is not None,
        "total": TOTAL,
        "have": have_bytes,
        "missing": [f["name"] for f in miss],
        "version": MANIFEST.get("index_version") or "",
        "url": _base_url(),
    }
    if _task and not _task.done():
        el = max(0.001, time.time() - float(_prog.get("started") or time.time()))
        out["downloading"] = {"name": _prog.get("name") or "", "bps": int((_prog.get("done") or 0) / el)}
    if _prog.get("error"):
        out["error"] = _prog["error"]
    if _prog.get("cancelled"):
        out["cancelled"] = True
    return out


def _base_url() -> str:
    repo = MANIFEST.get("repo") or ""
    rel = MANIFEST.get("release") or ""
    if os.environ.get("TAG_ROLL_INDEX_URL"):        # 점검용: 로컬 파일 서버를 가리킬 때
        return os.environ["TAG_ROLL_INDEX_URL"].rstrip("/") + "/"
    if not repo or not rel:
        return ""
    return f"https://github.com/{repo}/releases/download/{rel}/"


async def _fetch(client, url: str, dest: Path, f: dict) -> None:
    """한 파일 받기 — `.part` 에 이어 쓰고, 다 받으면 sha256 을 대조한 뒤 제자리로 옮긴다."""
    part = dest.with_suffix(dest.suffix + ".part")
    h = hashlib.sha256()
    start = 0
    if part.is_file() and part.stat().st_size < f["size"]:
        with part.open("rb") as fh:                  # 이어 받으려면 먼저 있는 만큼을 해시에 넣는다
            for chunk in iter(lambda: fh.read(1 << 22), b""):
                h.update(chunk)
        start = part.stat().st_size
    elif part.is_file():
        part.unlink()
    headers = {"Range": f"bytes={start}-"} if start else {}
    _prog["done"] = int(_prog.get("done") or 0) + start
    async with client.stream("GET", url, headers=headers) as r:
        if start and r.status_code == 200:           # 서버가 Range 를 무시했다 — 처음부터 다시
            _prog["done"] = int(_prog.get("done") or 0) - start
            h, start = hashlib.sha256(), 0
            part.unlink(missing_ok=True)
        elif r.status_code not in (200, 206):
            raise RuntimeError(f"{dest.name}: HTTP {r.status_code}")
        with part.open("ab" if start else "wb") as fh:
            async for chunk in r.aiter_bytes(1 << 20):
                if _cancel:
                    raise asyncio.CancelledError
                fh.write(chunk)
                h.update(chunk)
                _prog["done"] = int(_prog.get("done") or 0) + len(chunk)
    if part.stat().st_size != f["size"]:
        raise RuntimeError(f"{dest.name}: 크기가 다릅니다 ({part.stat().st_size:,} ≠ {f['size']:,})")
    if h.hexdigest() != f["sha256"]:
        part.unlink(missing_ok=True)
        raise RuntimeError(f"{dest.name}: sha256 이 다릅니다 (받은 파일을 버렸습니다)")
    dest.unlink(missing_ok=True)
    part.rename(dest)


async def _run() -> None:
    global _cancel
    import httpx

    d = dirpath()
    d.mkdir(parents=True, exist_ok=True)
    base = _base_url()
    if not base:
        _prog["error"] = "색인을 받을 주소가 매니페스트에 없습니다"
        return
    miss = missing(d)
    _prog.update({"done": 0, "total": sum(f["size"] for f in miss), "started": time.time(), "error": "", "cancelled": False})
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=300.0), follow_redirects=True) as c:
            for f in miss:
                _prog["name"] = f["name"]
                await _fetch(c, base + f["name"], d / f["name"], f)
    except asyncio.CancelledError:
        _prog["cancelled"] = True
    except Exception as e:  # noqa: BLE001 — 못 받은 까닭을 화면에 그대로 보낸다
        _prog["error"] = f"{type(e).__name__}: {e}"
    finally:
        _cancel = False
        _prog["name"] = ""


def start() -> dict:
    """내려받기 시작 (이미 돌고 있으면 그대로 둔다)."""
    global _task, _cancel
    if _task and not _task.done():
        return {"ok": True, "already": True}
    if not missing():
        return {"ok": True, "ready": True}
    _cancel = False
    _task = asyncio.create_task(_run())
    return {"ok": True}


def cancel() -> dict:
    global _cancel
    if _task and not _task.done():
        _cancel = True
        return {"ok": True}
    return {"ok": True, "idle": True}


def remove() -> dict:
    """색인 지우기 — **매니페스트에 있는 파일만** 지운다 (폴더를 통째로 지우지 않는다).

    ★엔진이 이미 색인을 열었으면 파일이 메모리 맵으로 잡혀 있어 윈도우에서 지워지지 않는다.
      그때는 무엇이 남았는지 알리고, 앱을 다시 켠 뒤에 지우게 한다."""
    d = dirpath()
    gone, kept = [], []
    for f in FILES:
        p = d / f["name"]
        for q in (p, p.with_suffix(p.suffix + ".part")):
            if not q.is_file():
                continue
            try:
                q.unlink()
                gone.append(q.name)
            except OSError as e:
                kept.append(f"{q.name} ({e.strerror or e})")
    if kept:
        return {"ok": False, "removed": gone, "kept": kept,
                "error": "쓰고 있는 파일이 있어 일부를 못 지웠습니다. 앱을 다시 켠 뒤에 지우십시오."}
    return {"ok": True, "removed": gone}


def _import_engine():
    global _engine
    if _engine is None:
        os.environ["TAG_ROLL_INDEX"] = str(dirpath())   # 엔진이 import 될 때 읽는다
        if __package__:
            from . import engine as mod                 # 앱이 플러그인 폴더를 패키지로 읽는다
        else:
            import engine as mod                        # 점검 스크립트가 홀로 부를 때
        _engine = mod
    return _engine


async def engine():
    """엔진 모듈 (첫 호출에 색인을 연다 — 10초 안팎이라 스레드에서 한다)."""
    if _engine is not None:
        return _engine
    if missing():
        raise FileNotFoundError("색인이 아직 없습니다")
    return await asyncio.to_thread(_import_engine)
