# -*- coding: utf-8 -*-
"""굴리기 핵심(급 사다리·인물 가중·「없음」)의 결정적 회귀 테스트 (2026-09-07, 검증 의견 반영).

  python -X utf8 test_roll_regress.py           → 골든 파일(_golden_roll.json)과 비교. 없으면 만든다.
  python -X utf8 test_roll_regress.py --update  → 골든을 새로 쓴다 (동작을 일부러 바꿨을 때만).

전역 색인을 쓰지 않고 200장짜리 합성 표본(Sample.from_arrays)으로 검사한다:
- 같은 입력 두 번, 캐시를 비운 뒤, 항목 순서를 바꿔서 — 급·확률·「없음」이 바이트 단위로 같아야 한다 (재현성).
- 급 안 인물당 상한이 실제로 걸리는지 (한 인물 60장이 상한 아래로).
- 「없음」 질의가 이웃을 옮기는지 (원피스 없음 뒤 상의 「없음」이 내려가는지).
engine 을 import 하면 색인이 로드된다(첫 실행 10초 안팎). 서버는 켜지 않는다."""
import sys, json, argparse
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).parent)); import engine as R

GOLD = Path(__file__).parent / "_golden_roll.json"

def synth():
    """합성 표본 200장: 태그는 실제 사전의 tid 를 쓴다(이름 → 슬롯 표가 그대로 먹게)."""
    T = lambda name: R.TID[name]
    rng = np.random.default_rng(7)
    rows, stid, char = [], [], []
    for i in range(200):
        tags = ["1girl", "long_hair" if rng.random() < 0.6 else "short_hair", rng.choice(["black_hair", "brown_hair", "blonde_hair"], p=[0.5, 0.3, 0.2])]
        if rng.random() < 0.85: tags.append(rng.choice(["blue_eyes", "red_eyes", "green_eyes"]))
        if i < 60: c = 1; tags += ["school_uniform", "serafuku", "pleated_skirt", "thighhighs"]      # 인물 1: 60장, 교복
        elif i < 90: c = 2; tags += ["dress", "white_dress", "hair_ribbon"]                            # 인물 2: 30장, 드레스
        else:
            c = 0                                                                                     # 오리지널 110장
            if rng.random() < 0.5: tags += ["dress"] if rng.random() < 0.5 else ["swimsuit"]
            else: tags += ["shirt", "skirt"] + (["thighhighs"] if rng.random() < 0.4 else [])
        tags += [f"tag_pad_{k}" for k in range(0)]   # 자리
        # 태그 수를 30개 이상으로 채워 「태그 넉넉한 그림」이 되게 (결측 처리 ② 가 작동하도록)
        filler = ["smile", "looking_at_viewer", "solo", "blush", "open_mouth", "closed_mouth", "simple_background", "white_background", "upper_body", "cowboy_shot",
                  "standing", "sitting", "bangs", "sidelocks", "collarbone", "bare_shoulders", "hair_between_eyes", "very_long_hair", "medium_breasts", "ahoge",
                  "hair_ornament", "ribbon", "bow", "jewelry", "earrings", "necklace", "choker", "sleeveless", "detached_sleeves", "frills"]
        tags += list(rng.choice(filler, size=28, replace=False))
        tids = sorted({T(t) for t in tags if t in R.TID})
        rows += [i] * len(tids); stid += tids; char.append(c)
    return R.Sample.from_arrays(np.array(rows), np.array(stid), np.array(char))

def snapshot(sub, items, pool_slot):
    pool = R.SLOT_POOL_ARR[pool_slot]
    order, bounds, vals, _st = R.query_tiers(sub, items)   # 급 = 점수 내림차순 정렬의 접두사. bounds[j] = 급 j 의 크기
    n, k, p = R.ladder(sub, items, pool, R.M_PRIOR)
    base, N = R.base_for("gs")
    _, cands = R.slot_cands(sub, items, pool, base, N, R.M_PRIOR, set(), None, lift_min=0)
    none_p = R.none_prob(sub, items, cands, R.M_PRIOR, None, pool_slot, pool_tids=pool)
    return {"tiers": [int(b) for b in bounds], "n": round(float(n), 6), "top": [(c[0], round(c[1], 6)) for c in cands[:6]], "none_p": round(float(none_p), 6)}

def run():
    sub = synth()
    out = {}
    out["A_no_query_upper"] = snapshot(sub, [], "upper")
    out["B_serafuku_upper"] = snapshot(sub, R.q_items(["serafuku"]), "lower")            # 인물 1 이 지배 → 상한이 걸려야
    out["C_none_onepiece_upper"] = snapshot(sub, R.q_items([], ["onepiece"]), "upper")   # 원피스 없음 → 상의 「없음」이 A 보다 내려가야
    out["D_two_tags"] = snapshot(sub, R.q_items(["black_hair", "blue_eyes"]), "onepiece")
    # 재현성: 항목 순서 바꿔도, 캐시 비워도 같아야
    s1 = snapshot(sub, R.q_items(["blue_eyes", "black_hair"]), "onepiece")
    sub2 = synth(); s2 = snapshot(sub2, R.q_items(["black_hair", "blue_eyes"]), "onepiece")
    assert s1 == out["D_two_tags"] == s2, "재현성 실패: 항목 순서 또는 캐시에 따라 결과가 다르다"
    # 인물 상한: serafuku 질의의 최상급은 인물 1 의 60장인데, 가중 장수는 상한(max(6, 2%·60)=6) 아래여야
    assert out["B_serafuku_upper"]["n"] <= 6.0 + 1e-6, f"급 안 인물 상한이 안 걸림: n={out['B_serafuku_upper']['n']}"
    # 「없음」 질의 효과
    assert out["C_none_onepiece_upper"]["none_p"] < out["A_no_query_upper"]["none_p"], "원피스 「없음」 질의가 상의 「없음」을 낮추지 못함"
    return out

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--update", action="store_true"); a = ap.parse_args()
    out = json.loads(json.dumps(run(), ensure_ascii=False))   # JSON 왕복으로 정규화(튜플 → 리스트) 후 비교
    if a.update or not GOLD.exists():
        GOLD.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8"); print("골든 기록:", GOLD.name)
    else:
        gold = json.loads(GOLD.read_text(encoding="utf-8"))
        bad = [k for k in out if out[k] != gold.get(k)]
        if bad:
            for k in bad: print("DIFF", k, "\n  now :", out[k], "\n  gold:", gold.get(k))
            sys.exit(1)
        print("회귀 테스트 통과:", ", ".join(out))
