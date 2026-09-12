# -*- coding: utf-8 -*-
"""창구 — 원본 `roll_server.py` 의 `ThreadingHTTPServer`(8766 고정) 를 대신한다.

앱이 이 파일의 `router` 를 `/plug/tag-roll/` 에 붙인다. 화면(`web/index.html`)은 같은 자리에서
서빙되므로 상대 주소(`./api/roll`)로 부른다.

★★엔진을 **여기서 import 하지 않는다.** 엔진은 import 하는 순간 색인(약 960MB)을 열고 데우므로,
  백엔드가 켜질 때 그것이 돌면 앱 기동이 그만큼 늦어진다. 창구를 처음 부를 때 `index.engine()` 이
  스레드에서 import 한다.
★굴리기 한 번은 0.1~0.5초 걸리는 계산이다 (표본이 크면 그 이상). 그래서 라우트는 `to_thread` 로
  돌린다 — 이벤트 루프를 잡고 있으면 그 사이 앱의 다른 요청(생성·저장)이 멈춘다.
★예외를 삼키지 말고 JSON 으로 돌려준다 (원본 `do_GET` 의 ★주와 같다) — 응답이 없으면 화면의
  모듈이 「굴리는 중…」에 머문다.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
import traceback
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import Response

from . import index as idx
from .msgs import M, set_lang

router = APIRouter()


def _json(d) -> Response:
    return Response(json.dumps(d, ensure_ascii=False), media_type="application/json; charset=utf-8")


# ── 색인 ────────────────────────────────────────────────────────────
# ★언어는 화면이 `lang=` 으로 알려 준다 (앱 설정을 따라간다). 창구가 돌려주는 문구가 그 언어로 나간다.
def _lang(request: Request) -> None:
    set_lang(parse_qs(request.url.query).get("lang", ["ko"])[0])


@router.get("/api/index")
async def api_index_status(request: Request):
    _lang(request)
    return _json(idx.status())


@router.post("/api/index/download")
async def api_index_download(request: Request):
    _lang(request)
    return _json(idx.start())


@router.post("/api/index/cancel")
async def api_index_cancel(request: Request):
    _lang(request)
    return _json(idx.cancel())


# ── 굴리기 ──────────────────────────────────────────────────────────
async def _call(request: Request, fn):
    """엔진을 준비하고(첫 회 10초 안팎) 쿼리를 넘긴다. 색인이 없으면 그 사실을 답으로 돌려준다."""
    _lang(request)
    try:
        E = await idx.engine()
    except FileNotFoundError:
        return _json({"error": M("색인이 없습니다"), "need_index": True, "index": idx.status()})
    except Exception as e:  # noqa: BLE001 — 색인이 깨졌거나 못 읽는 경우
        traceback.print_exc()
        return _json({"error": M("색인을 열지 못했습니다: {detail}", detail=f"{type(e).__name__}: {e}")})
    q = parse_qs(request.url.query)
    try:
        return _json(await asyncio.to_thread(fn, E, q))
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        return _json({"error": f"{type(e).__name__}: {e}"})


def _slots(E, q):
    frame_pool = next(p for k, _, _, p in E.SSLOTS if k == "frame")
    return {
        "slots": [{"key": k, "label": l} for k, l, _ in E.SLOTS],
        "wearable": sorted(E.WEARABLE),
        "default_off": sorted(E.DEFAULT_OFF),
        "body_zones": E.BODY_ZONES,
        "wear_zones": E.WEAR_ZONES,
        "nude_off": sorted(E.NUDE_OFF),
        "bare_off": sorted(E.BARE_OFF),
        "modules": [{"key": "expr", "label": "표정", "slots": [{"key": k, "label": l} for k, l, _, _, _ in E.XSLOTS]},
                    {"key": "pose", "label": "포즈", "slots": [{"key": k, "label": l} for k, l, _, _ in E.PSLOTS]},
                    {"key": "scene", "label": "씬", "slots": [{"key": k, "label": l} for k, l, _, _ in E.SSLOTS]},
                    {"key": "nsfw", "label": "NSFW", "slots": [{"key": k, "label": l} for k, l, _ in E.NSLOTS]}],
        "genres": [{"key": k, "label": l} for k, l, _ in E.GENRES],
        "costumes": [{"key": k, "label": l} for k, l, _ in E.COSTUMES],
        # ★원본은 화면 HTML 에 박아 넣던 것이다 (`<!--FRAMES-->`). 화면이 정적 파일이 되었으니 여기서 준다.
        "frames": list(frame_pool),
    }


def _tags(E, q):
    s = q.get("q", [""])[0].strip().lower().replace(" ", "_")
    hits = [t for t in E.TOPTAGS if s in t][:30]
    hits.sort(key=lambda t: (not t.startswith(s), len(t)))
    return hits


def _roll(E, q):
    anchors = [t.strip().lower().replace(" ", "_") for t in q.get("anchor", [""])[0].split(",") if t.strip()]
    bad = [t for t in anchors if t not in E.TID]
    if bad:
        return {"error": M("사전에 없는 태그: {tags}", tags=bad)}
    seed = int(q.get("seed", ["0"])[0] or 0)
    random_pick = not anchors   # 빈 칸이면 시드마다 새 랜덤 앵커. 입력칸에는 안 써 넣고 결과에만 표시한다 (사용자 지시 2026-09-06)
    if not anchors:
        with E.LOCK:
            anchors = [E.random_anchor(random.Random(seed), q.get("rating", ["all"])[0], q.get("seg", ["all"])[0], q.get("kemono", ["1"])[0] == "1")]
    anchor = anchors
    temp = max(0.1, float(q.get("temp", ["1.0"])[0])); rating = q.get("rating", ["all"])[0]
    fixed = json.loads(q.get("fixed", ["{}"])[0]); mode = q.get("mode", ["dressed"])[0]; seg = q.get("seg", ["all"])[0]
    on = json.loads(q.get("on", ["null"])[0]); kemono = q.get("kemono", ["1"])[0] == "1"; exclude = E._excl(q)
    beta = E.FOCUS_BETA if q.get("focus", ["0"])[0] == "1" else None   # 「앵커 특화」 체크
    with E.LOCK:
        t0 = time.time()
        d = E.roll(anchor, temp, rating, fixed, seed, mode, seg, on, kemono, beta, exclude, E._charcap(q), E._must(q), E._cos(q))
        d["ms"] = int((time.time() - t0) * 1000); d["anchor_random"] = random_pick
    return d


def _scene(E, q):
    anchor = tuple(t for t in q.get("anchor", [""])[0].split(",") if t); rating = q.get("rating", ["all"])[0]; act = q.get("act", [""])[0]
    temp = max(0.1, float(q.get("temp", ["1.0"])[0])); seed = int(q.get("seed", ["0"])[0] or 0); fixed = json.loads(q.get("fixed", ["{}"])[0])
    bg = q.get("bg", [""])[0] or ("real" if q.get("fill", [""])[0] == "1" else "auto"); seg = q.get("seg", ["all"])[0]; frame = q.get("frame", ["auto"])[0]   # fill= 은 옛 호출 호환
    if not anchor or any(a not in E.TID for a in anchor) or (act and act not in E.TID):
        return {"error": M("사전에 없는 태그")}
    with E.LOCK:
        t0 = time.time()
        d = E.roll_scene(anchor, rating, act, temp, fixed, seed, bg, seg, frame,
                         E._excl(q) | (E.MULTI_ONLY if E._people(q) == 1 else set()), E._charcap(q), E._design(q),
                         q.get("nowhite", ["0"])[0] == "1", E._must(q), E._on(q))
        d["ms"] = int((time.time() - t0) * 1000)
    return d


def _expr(E, q):
    anchor = tuple(t for t in q.get("anchor", [""])[0].split(",") if t); rating = q.get("rating", ["all"])[0]; act = q.get("act", [""])[0]
    temp = max(0.1, float(q.get("temp", ["1.0"])[0])); seed = int(q.get("seed", ["0"])[0] or 0); fixed = json.loads(q.get("fixed", ["{}"])[0])
    seg = q.get("seg", ["all"])[0]
    exclude = [t for t in q.get("exclude", [""])[0].split(",") if t] + (sorted(E.MULTI_ONLY) if E._people(q) == 1 else [])
    if not anchor or any(a not in E.TID for a in anchor) or (act and act not in E.TID):
        return {"error": M("사전에 없는 태그")}
    with E.LOCK:
        t0 = time.time()
        d = E.roll_expr(anchor, rating, act, temp, fixed, seed, seg, exclude, E._charcap(q), E._design(q), E._cos(q), E._must(q), E._on(q))
        d["ms"] = int((time.time() - t0) * 1000)
    return d


def _pose(E, q):
    anchor = tuple(t for t in q.get("anchor", [""])[0].split(",") if t); rating = q.get("rating", ["all"])[0]; act = q.get("act", [""])[0]
    temp = max(0.1, float(q.get("temp", ["1.0"])[0])); seed = int(q.get("seed", ["0"])[0] or 0); fixed = json.loads(q.get("fixed", ["{}"])[0])
    seg = q.get("seg", ["all"])[0]
    exclude = [t for t in q.get("exclude", [""])[0].split(",") if t] + (sorted(E.MULTI_ONLY) if E._people(q) == 1 else [])
    if not anchor or any(a not in E.TID for a in anchor) or (act and act not in E.TID):
        return {"error": M("사전에 없는 태그")}
    with E.LOCK:
        t0 = time.time()
        d = E.roll_pose(anchor, rating, act, temp, fixed, seed, seg, exclude, E._charcap(q), E._design(q), E._cos(q), E._must(q), E._on(q))
        d["ms"] = int((time.time() - t0) * 1000)
    return d


def _nsfw(E, q):
    temp = max(0.1, float(q.get("temp", ["1.0"])[0])); seed = int(q.get("seed", ["0"])[0] or 0)
    fixed = json.loads(q.get("fixed", ["{}"])[0]); body = q.get("body", [""])[0]; design = json.loads(q.get("design", ["[]"])[0])
    act = fixed.pop("act", "") or ""
    level = int(q.get("level", ["2"])[0]); nude = q.get("bare", ["0"])[0] == "1"; nude_tag = q.get("nudetag", [""])[0]
    if act and act not in E.TID:
        return {"error": M("사전에 없는 태그: {tags}", tags=act)}
    with E.LOCK:
        t0 = time.time()
        d = E.roll_nsfw(act, temp, fixed, seed, body, design, level, nude, nude_tag, E._excl(q), E._must(q), E._on(q))
        d["ms"] = int((time.time() - t0) * 1000)
    return d


@router.get("/api/slots")
async def api_slots(request: Request):
    return await _call(request, _slots)


@router.get("/api/tags")
async def api_tags(request: Request):
    return await _call(request, _tags)


@router.get("/api/roll")
async def api_roll(request: Request):
    return await _call(request, _roll)


@router.get("/api/scene")
async def api_scene(request: Request):
    return await _call(request, _scene)


@router.get("/api/expr")
async def api_expr(request: Request):
    return await _call(request, _expr)


@router.get("/api/pose")
async def api_pose(request: Request):
    return await _call(request, _pose)


@router.get("/api/nsfw")
async def api_nsfw(request: Request):
    return await _call(request, _nsfw)
