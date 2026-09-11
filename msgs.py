# -*- coding: utf-8 -*-
"""창구가 사용자에게 돌려주는 문구 (한국어·영어·일본어).

★★**한국어 원문이 키다** — 화면의 `web/i18n.js` 와 같은 방식이다. 표에 없으면 한국어 그대로 나간다.
★언어는 화면이 매 요청에 `lang=` 으로 알려 준다 (앱 설정을 따라간다). 받은 값을 모듈 전역에 둔다 —
  이 플러그인은 한 사람의 앱 안에서만 도는 창구라 요청마다 갈아 끼워도 엉킬 일이 없고,
  내려받기처럼 요청이 끝난 뒤에도 도는 일은 시작할 때의 언어를 그대로 쓴다.
★`{이름}` 은 자리표시자다. `M("조건 {n}개", n=3)`.
"""
from __future__ import annotations

LANG = "ko"

_TBL: dict[str, dict[str, str]] = {
    "en": {
        "색인이 없습니다": "There is no index",
        "색인이 아직 없습니다": "There is no index yet",
        "색인을 열지 못했습니다: {detail}": "Could not open the index: {detail}",
        "사전에 없는 태그": "That tag is not in the dictionary",
        "사전에 없는 태그: {tags}": "Tags that are not in the dictionary: {tags}",
        "색인을 릴리즈에서 찾지 못했습니다 (HTTP 404). 색인 판 「{rel}」 이 아직 올라가 있지 않을 수 있습니다:\n{url}":
            "The index was not found in the release (HTTP 404). Index version “{rel}” may not be uploaded yet:\n{url}",
        "{name} 를 받지 못했습니다 (HTTP {code})": "Could not download {name} (HTTP {code})",
        "{name} 의 크기가 다릅니다 ({got} ≠ {want})": "{name} has the wrong size ({got} ≠ {want})",
        "{name} 의 sha256 이 다릅니다 (받은 파일을 버렸습니다)": "The sha256 of {name} does not match (the downloaded file was discarded)",
        "색인을 받을 주소가 매니페스트에 없습니다": "The manifest has no address to download the index from",
    },
    "ja": {
        "색인이 없습니다": "インデックスがありません",
        "색인이 아직 없습니다": "インデックスはまだありません",
        "색인을 열지 못했습니다: {detail}": "インデックスを開けませんでした: {detail}",
        "사전에 없는 태그": "辞書にないタグです",
        "사전에 없는 태그: {tags}": "辞書にないタグ: {tags}",
        "색인을 릴리즈에서 찾지 못했습니다 (HTTP 404). 색인 판 「{rel}」 이 아직 올라가 있지 않을 수 있습니다:\n{url}":
            "リリースにインデックスが見つかりません（HTTP 404）。インデックス版「{rel}」がまだアップロードされていない可能性があります:\n{url}",
        "{name} 를 받지 못했습니다 (HTTP {code})": "{name} を取得できませんでした（HTTP {code}）",
        "{name} 의 크기가 다릅니다 ({got} ≠ {want})": "{name} のサイズが違います（{got} ≠ {want}）",
        "{name} 의 sha256 이 다릅니다 (받은 파일을 버렸습니다)": "{name} の sha256 が一致しません（取得したファイルは破棄しました）",
        "색인을 받을 주소가 매니페스트에 없습니다": "インデックスの取得先がマニフェストにありません",
    },
}


def set_lang(lang: str) -> None:
    global LANG
    LANG = lang if lang in ("ko", "en", "ja") else "ko"


def M(ko: str, **kw) -> str:
    s = _TBL.get(LANG, {}).get(ko, ko)
    return s.format(**kw) if kw else s
