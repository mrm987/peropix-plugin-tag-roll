# 태그 굴리기 (PeroPix 공식 플러그인)

태그 하나에서 출발해 부위별로 캐릭터 디자인·표정·포즈·씬·NSFW 를 danbooru 통계에서 뽑습니다.
「지금 씬에 넣기」가 굴린 태그를 캐릭터 카드와 스타일 카드에 넣고, 「굴리기+넣기」는 그 둘을 한 번에 합니다.

화면은 **한국어·영어·일본어**로 나옵니다 — 앱 설정의 언어를 따라가고, 바꾸면 그 자리에서 바뀝니다
(문구는 `web/i18n.js` 와 `msgs.py` 에 있습니다).

### 넣은 블록의 이름표는 `굴리기_` 로 시작합니다

`굴리기_디자인`·`굴리기_표정`·`굴리기_포즈`·`굴리기_NSFW`·`굴리기_씬` 다섯입니다. 넣기를 누르면
**이 접두어가 붙은 블록을 먼저 전부 지우고** 이번에 굴린 것만 넣으므로, 이번에 안 뽑힌 모듈의 지난 태그가
남아 섞이지 않습니다. 접두어가 없는 블록(직접 만드신 것)은 건드리지 않으니, 굴린 결과를 남겨 두고 싶으시면
블록 이름을 바꿔 두시면 됩니다.

**설치** — PeroPix 의 플러그인 모드 → 관리 → 플러그인 목록에서 「태그 굴리기」를 설치하십시오.
앱은 [목록 저장소](https://github.com/mrm987/peropix-plugins) 를 통해 이 저장소의 태그 압축본을 받습니다.

## 색인을 한 번 내려받아야 합니다 (약 960MB)

굴리기는 danbooru 덤프(2026-05-18)에서 만든 색인을 읽습니다. 그 색인은 꾸러미에 담기지 않고
**이 저장소의 릴리즈**에 자산으로 올라가 있습니다. 플러그인을 처음 열면 화면이 안내하고,
「색인 내려받기」를 누르면 받습니다. 받다 끊기면 다음에 이어 받고, 파일마다 sha256 을 대조합니다.

| | |
|---|---|
| 무엇 | 2016년 이후 1girl 그림 636만 장 × 상위 12,000 태그의 동시출현 색인 (13개 파일) |
| 자리 | `<앱 뿌리>\plugins\tag-roll\_data\` |
| 지우기 | 창 아래 「색인 관리」 (플러그인을 지우면 색인도 함께 휴지통으로 갑니다) |

★`_data/` 는 앱의 규격입니다 — **업데이트해도 남는 자리**입니다. 앱이 판을 갈아 끼울 때 옛 사본의 `_data/` 만
새 사본으로 옮겨 주므로, 판을 올려도 960MB 를 다시 받지 않습니다. 자세한 것은
[목록 저장소 README](https://github.com/mrm987/peropix-plugins) 의 「업데이트해도 남는 자리」를 보십시오.

## 담고 있는 것

- `server.py` — 창구(FastAPI 라우터). 앱 백엔드에 `/plug/tag-roll/` 로 붙습니다.
- `engine.py` — 굴리기 본체. 원본은 `danbooru-dump/_scripts/roll_server.py` 이고, 셈법은 옮기면서 한 줄도 바꾸지 않았습니다
  (`test_roll_regress.py` + `_golden_roll.json` 이 원본과 같은 값을 내는지 대조합니다).
- `index.py` — 색인의 자리·내려받기·검증·지우기.
- `index-manifest.json` — 색인 파일 목록(이름·크기·sha256)과 색인 판에 딸린 상수(모집단 시작 id·등급별 게시물 수).
- `web/index.html` — 화면 하나. 앱이 주는 공통 자산(`/plug/_app/base.css`·`peropix.js`)을 씁니다.
- `requirements.txt` — duckdb (numpy 는 앱 백엔드에 이미 있습니다).

## 판 올리기

`plugin.json` 의 `version` 을 올리고 같은 번호로 태그(`v1.0.0`)를 답니다. 앱은 목록 저장소의 `index.json` 에 적힌
태그의 압축본을 받으므로, 태그를 달아야 사용자에게 갑니다.

## 색인을 새로 만들었을 때

덤프에서 색인을 다시 만들었으면 매니페스트를 새로 쓰고 릴리즈에 올립니다.

```
python -X utf8 scripts/build-manifest.py <색인 폴더> --release index-<덤프 날짜>
gh release create index-<덤프 날짜> <색인 파일들> --title "색인 <덤프 날짜>"
```

색인 파일 자체는 이 저장소에 커밋하지 않습니다 (git 은 파일당 100MB 를 넘기지 못하고, 릴리즈 자산은 2GiB 까지 받습니다).

---

A PeroPix plugin: roll a character design (and expression, pose, scene) from danbooru co-occurrence statistics,
starting from a single tag. Install it from the plugin list inside the app; it downloads its ~960MB index once.
