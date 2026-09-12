# -*- coding: utf-8 -*-
"""태그 하나 → 부위별 자동 선택 (로컬 웹 UI). 사용자 지시 2026-09-03: 「태그 아무거나 하나 선택하면 자동선택되는걸로」.

  python roll_server.py            → http://127.0.0.1:8766 를 연다

원리: 덤프 1,136만 장의 순수 동시출현(tagassoc.py 와 같은 표 _tags_long.parquet). 앵커 태그가 있는 그림(최대 5만 장, 시드로 추출)의 태그를 그림별 색인에서 모으고,
부위를 순서대로 돌며 「지금까지 고른 것 전부가 있는 그림」에서 그 부위 태그의 비율 P(B|A) 을 재서 확률로 뽑는다 (온도 = 비율^(1/온도)).
칩을 누르면 그 부위를 그것으로 고정하고 뒤 부위만 다시 굴린다. 시드가 같으면 같은 결과.
★조건은 「완전 일치」가 아니라 「질의 점수 급」이다 (2026-09-08, 설계문서 §8): 질의 항목(태그·믿을 만한 슬롯의 「없음」)마다 정수 idf 를 두고 그림 점수 S = Σ idf,
점수 급을 위에서부터 접두사로 밟아 Dirichlet 사다리(m=300)로 잇는다. 급 안 인물당 상한(max(6, 2%)). 다섯 모듈 공통. README 「굴리기」 절 머리의 ★항목 참조.

NSFW 모듈 (사용자 지시 2026-09-03 「랜덤 체위를 고르고 그에 맞는 조합」): 체위·행위 풀에서 e 등급 빈도로 하나를 뽑고, 그 태그가 있는 e 등급 여성 그림에서
삽입·상대·옷 상태·시점·반응·절정·장소·접촉을 같은 방식으로 채운다. 디자인에서 고른 가슴 크기를 조건에 넣는다 (조사 결과 행위와 닿는 유일한 훅).
옷 상태가 완전 나체면 디자인의 옷 태그를 걷어 내고 살아남는 요소(머리·눈·체형·다리·손·머리 장식·목·종족)만 남긴다.

씬 모듈 (사용자 지시 2026-09-04 「배경이나 조명이나 구도 같은 씬 모듈」): 장소 → 시간·하늘 → 조명 → 날씨·효과 → 프레이밍 → 카메라 각도. (시선은 2026-09-07 표정 모듈로.)
NSFW 가 켜져 있으면 체위의 e 등급 표본에서(장면이 행위와 맞게: 침대·교실·야외 비율이 체위마다 다르다), 아니면 디자인 앵커의 표본에서 같은 방식으로 뽑는다.
NSFW 의 장소·시점 슬롯은 이 모듈로 옮겼다 (하나의 정보에 하나의 창구).

표정 모듈 (사용자 지시 2026-09-07 「표정 모듈을 따로」): 감정(필수) → 입 → 눈 → 눈썹 → 볼·상태(둘까지) → 시선. 표본은 포즈·씬과 같다.
포즈 모듈의 표정 슬롯 하나에 감정·입 상태·눈 상태가 한 풀로 섞여 있고 후보 상위 12개 컷까지 받아서, 실측(현대·전연령·standing 조건)으로 smile 44~54%
+ open_mouth·closed_mouth·parted_lips 가 추첨의 43~46% 를 차지하고 angry·serious·smug 는 후보에도 못 들었다. 표정 모듈은 슬롯을 갈라 조건부로 뽑고(감정 embarrassed 뒤에 blush 확률이 오른다),
후보 컷을 풀 전체로 푼다. 시선(looking_at_viewer 등)은 씬 모듈에서 여기로 옮겼다(얼굴에 관한 것은 한 창구). blush 는 볼·상태 슬롯에 넣었다(감정과 상호 보완이라 감정 뒤에 조건부로 딸려오게).
온도는 전역 슬라이더 하나가 기본이고, 하위 드롭다운에서 모듈(디자인·NSFW·표정·포즈·씬)마다 따로 정할 수 있다 (사용자 결정 2026-09-07). 굴리기 사슬은 디자인 → NSFW → 표정 → 포즈 → 씬."""
import sys, os, json, re, random, time, math, threading
import traceback
from pathlib import Path
import duckdb
import numpy as np
# ★★플러그인으로 옮기면서 바꾼 것 (2026-09-10). 굴리기 셈법은 한 줄도 손대지 않았다 —
#   회귀 테스트(`test_roll_regress.py` + `_golden_roll.json`)가 원본과 같은 값을 내는지로 확인한다.
#   1. 색인 폴더를 환경 변수 `TAG_ROLL_INDEX` 로 받는다 (원본은 스크립트 옆 폴더 고정).
#   2. 덤프(`danbooru.duckdb`, 1.5GB)를 떼어냈다 — 거기서 구하던 것은 상수 넷(`ID_SINCE` 와 등급별
#      게시물 수 셋)뿐이라 `index-manifest.json` 에 적어 둔다. 색인을 **만들** 때만 덤프가 필요하다.
#   3. `assoc.py`(다른 폴더를 절대 경로로 읽던 것)에서 실제로 쓰던 두 조각(부위 표·머리 형태)을 아래로 옮겼다.
#   ★이 모듈은 **import 하는 순간** 색인(약 1GB)을 열고 데운다 — 원본과 같다. 그래서 `server.py` 가
#     import 를 첫 호출까지 미룬다. 백엔드가 켜질 때 도는 일이 없어야 한다.
HERE = Path(__file__).parent
DIR = Path(os.environ.get("TAG_ROLL_INDEX") or HERE)   # 색인 폴더 (`index.py` 가 정해 넣는다)
LONG = (DIR / "_tags_long.parquet").as_posix()          # 원본 표 — 색인을 만들 때와 상위 12,000 밖 태그에만 쓴다
DICT = (DIR / "_tag_dict.parquet").as_posix()
DB = str(DIR / "danbooru.duckdb")                       # 덤프 — 색인을 만들 때만
# 색인과 짝이 맞아야 하는 상수: 덤프에서 한 번 재어 적어 둔 값이다 (`scripts/build-manifest.py` 가 쓴다)
CONSTS = json.loads((HERE / "index-manifest.json").read_text(encoding="utf-8"))["consts"]
# 굴리기 전용 축소본: 1girl 그림 × 상위 12,000 태그(569장 이상)만, 정수 컬럼. parquet 원본을 훑으면 앵커 표본 하나에 2.3초가 드는데
# 이 표에서는 0.5초다 (2026-09-04 실측: 첫 굴리기가 6초 걸리던 원인). 컷 밖 태그가 앵커로 오면 원본 parquet 로 돌아간다.
# ---- 모집단: 2016-01-01 이후 게시물 (사용자 지시 2026-09-05 「최근 10년으로」) ----
# 덤프(2026-05-18)의 1girl 그림 790만 장 중 2016년 이후가 81%(636만). 옛 유행이 같은 무게로 섞이는 것을 막는다.
# id 는 업로드 순으로 발급되므로 「created_at ≥ 2016-01-01 인 최소 id」 하나로 자른다. 파생 파일(_roll_since2016_*)은 이 모집단으로 만든다.
SINCE = "2016-01-01"
ID_SINCE = int(CONSTS["id_since"])
ROLL_DB = DIR / "_roll_since2016.duckdb"
TID_CUT = 12000


def need_dump(what):
    """색인을 만들려면 덤프가 있어야 한다. 없으면 무엇이 없는지 그대로 알린다 (창구가 그 문구를 화면에 보낸다)."""
    raise FileNotFoundError(f"{what} 이(가) 없고, 만들 원본({Path(DB).name}·{Path(LONG).name})도 색인 폴더에 없습니다: {DIR}")

# ---- 부위 표 (원본은 `assoc.py` 에서 읽었다: `SLOTS`·`HAIR_STYLE`·`slot()`) ----
# ★이 표가 「이 태그는 어느 부위인가」를 정한다. 마지막 낱말로 판정하므로 white_thighhighs 도 다리로 간다.
ASSOC_SLOTS = {
    "onepiece": "dress leotard bodysuit kimono robe tunic gown sundress swimsuit overalls jumpsuit",
    "lower": "skirt shorts pants jeans trousers hakama loincloth fundoshi bloomers",
    "upper": "shirt blouse vest sweater hoodie top tank_top crop_top corset bra bikini_top camisole tabard surcoat turtleneck",
    "outer": "cape capelet cloak mantle poncho shawl haori jacket coat cardigan blazer",   # jacket·coat·cardigan·blazer 는 2026-09-04 상의에서 옮김 — 현대 옷의 겉옷이 망토류뿐이라 늘 비었고, 상의에서 open_jacket 이 뽑히면 밑에 입은 셔츠가 안 뽑혔다
    "legs": "thighhighs pantyhose socks kneehighs leg_warmers garter_straps legwear stockings",
    "feet": "boots sandals shoes footwear barefoot heels loafers geta slippers",
    "hands": "gloves gauntlets bracer vambraces bracers armlet wristband",
    "head": "hat crown tiara hairband headband circlet helmet veil headdress beret hood bandana headpiece hair_ornament hairclip hairpin hair_ribbon hair_bow hair_flower hair_bell hair_stick hair_tubes hair_scrunchie headscarf goggles_on_head eyepatch glasses monocle",
    "neck": "choker scarf necklace collar cravat necktie ascot neckerchief bowtie pendant amulet gorget neck_ribbon neck_bell",
    "wrist": "bracelet wristband bangle armlet arm_wraps bandaged_arm wrist_cuffs bracer armband",
    "waist": "belt sash belt_pouch pouch waist_apron apron corset obi rope_belt chain",
    "face": "scar facial_mark freckles mole tattoo face_paint eyepatch fang heterochromia bandaid",
    "ears": "earrings earring",
    "armor": "armor breastplate pauldrons shoulder_armor greaves chainmail",
    "race": "pointy_ears animal_ears horns tail wings cat_ears fox_ears rabbit_ears wolf_ears dog_ears mouse_ears horn demon_horns",
    "weapon": "sword staff bow_(weapon) polearm spear axe shield dagger gun whip scythe wand",
}
SLOT_OF = {w: k for k, ws in ASSOC_SLOTS.items() for w in ws.split()}
HAIR_STYLE = set("twintails ponytail braid twin_braids single_braid side_ponytail hair_bun ahoge hair_over_one_eye blunt_bangs drill_hair low_twintails two_side_up hair_intakes messy_hair wavy_hair curly_hair spiky_hair short_hair long_hair very_long_hair medium_hair bob_cut".split())


def slots_of(t):
    """태그 → 부위 (원본 `assoc.slot`)."""
    if t in HAIR_STYLE: return {"hair_style"}
    if re.fullmatch(r"[a-z_-]+_hair", t): return {"hair_color"}
    if re.fullmatch(r"[a-z_-]+_eyes", t): return {"eye_color"}
    if t in SLOT_OF: return {SLOT_OF[t]}
    last = t.split("_")[-1]
    if last in SLOT_OF: return {SLOT_OF[last]}
    for w, k in SLOT_OF.items():
        if t.endswith("_" + w): return {k}
    return set()

COLORS = "black white red blue green yellow purple pink brown grey orange aqua silver blonde light_brown dark_blue light_blue dark_green light_green lavender violet multicolored gradient two-tone streaked colored_inner".split()
HAIR_COLOR = {f"{c}_hair" for c in COLORS}
EYE_COLOR = {f"{c}_eyes" for c in COLORS} | {"heterochromia"}
BODY = {"flat_chest", "small_breasts", "medium_breasts", "large_breasts", "huge_breasts"}
# 부위 순서. required=False 면 「없음」이 후보에 든다 (확률 = 1 − 후보 비율 합, 최소 5%)
HAIR_LEN = {"very_long_hair", "long_hair", "medium_hair", "short_hair", "very_short_hair", "absurdly_long_hair"}
# 부위 표(assoc.py SLOTS)의 범주 전부를 슬롯으로 (사용자 지시 2026-09-04). 순서 = 조건이 쌓이는 순서
# ★디자인 슬롯은 전부 선택(required=False)이다 (사용자 결정 2026-09-07 「체크 = 통계대로 「없음」 포함, 해제 = 무조건 없음」).
#  전에는 머리색·머리 길이·눈색·가슴·상의·하의가 필수였다. 가슴이 필수라서 생긴 편향: 가슴 크기 태그는 몸이 보이는 그림에만 달리는데(가슴 태그 없는 그림 수영복 3.4%·있는 그림 17.9%,
#  전체 57% 가 태그 없음) 늘 크기 하나를 뽑아 뒤 슬롯의 조건으로 쓰니 옷 슬롯이 노출 옷으로 쏠렸다 (brown_hair 앵커 원피스 슬롯 swimsuit 0.096 → large_breasts 조건 뒤 0.206, 2026-09-07 실측).
SLOTS = [("hair_color", "머리색", False), ("hair_len", "머리 길이", False), ("hair_style", "머리 형태", False), ("eye_color", "눈색", False),
         ("face", "얼굴", False), ("ears", "귀", False), ("body", "가슴", False),
         ("onepiece", "원피스", False), ("upper", "상의", False), ("outer", "겉옷", False), ("lower", "하의", False), ("waist", "허리띠", False),
         ("legs", "다리옷", False), ("feet", "신발", False), ("hands", "장갑", False), ("wrist", "팔 장식", False),
         ("head", "머리 장식", False), ("neck", "목장식", False), ("armor", "갑주", False), ("weapon", "무기", False), ("race", "종족 파츠", False)]
WEARABLE = {"onepiece", "upper", "outer", "lower", "waist", "legs", "feet", "hands", "wrist", "head", "neck", "armor", "weapon"}   # 착의 모드가 켜고 끄는 부위
DEFAULT_OFF = {"armor", "weapon"}   # 처음에는 꺼 둔다 (판타지용)
# 화면의 부위 목록 (구역 이름, 슬롯들).
# ★★**신체와 복장을 가른다** (사용자 지시 2026-09-12). 가르는 선은 `WEARABLE` 이 이미 갖고 있던 것이고,
#   착의 모드(누드·완전 누드)가 끄는 것도 전부 복장 쪽이라 무엇이 꺼졌는지 한눈에 보인다.
#   ★복장 쪽 이름 다섯은 신체 부위 이름을 쓰고 있어 헷갈렸다 — 목→목장식 · 손→장갑 · 손목→팔 장식 ·
#     다리→다리옷 · 허리→허리띠 로 고쳤다 (같은 날 지시).
BODY_ZONES = [("머리", ["hair_color", "hair_len", "hair_style"]), ("얼굴", ["eye_color", "face", "ears"]),
              ("몸", ["body", "race"])]
WEAR_ZONES = [("머리", ["head"]), ("상체", ["neck", "onepiece", "upper", "outer", "waist", "armor"]),
              ("팔", ["wrist", "hands", "weapon"]), ("하체", ["lower", "legs", "feet"])]
# ---- NSFW 모듈 ----
ACTS = "cowgirl_position reverse_cowgirl_position squatting_cowgirl_position doggystyle sex_from_behind prone_bone missionary mating_press standing_sex suspended_congress reverse_suspended_congress full_nelson spooning amazon_position leg_lock piledriver_(sex) fellatio irrumatio paizuri handjob footjob cunnilingus breast_sucking masturbation fingering".split()
NSLOTS = [  # (key, 라벨, 후보 풀). 후보는 사전에 있는 것만 쓴다. 전부 「없음」 가능
 ("insert", "삽입", "vaginal anal double_penetration".split()),
 ("partner", "상대", "1boy faceless_male dark-skinned_male 2boys multiple_boys tentacles futanari yuri old_man fat_man interracial".split()),
 ("clothes", "옷 상태", "completely_nude clothed_sex bottomless clothes_lift shirt_lift skirt_lift panties_aside clothing_aside torn_clothes open_clothes breasts_out no_panties underwear_only wet_clothes topless_female".split()),
 ("react", "반응", "ahegao heart-shaped_pupils rolling_eyes tongue_out torogao fucked_silly tears trembling drooling heavy_breathing moaning empty_eyes crying closed_eyes".split()),
 ("motion", "격함", "motion_lines bouncing_breasts sweat steaming_body saliva saliva_trail pussy_juice wet spoken_heart emphasis_lines speed_lines afterimage motion_blur x-ray cross-section internal_cumshot cum_string breath clenched_teeth stomach_bulge deep_penetration screaming excessive_cum".split()),
 ("climax", "절정", "cum cum_in_pussy cum_on_body cum_on_breasts facial cum_in_mouth ejaculation cumdrip cum_overflow cum_in_ass orgasm female_ejaculation".split()),
 ("touch", "접촉", "grabbing_another's_breast grabbing_another's_ass leg_grab arm_grab hair_grab torso_grab hand_on_another's_head holding_another's_wrist arms_around_neck hand_on_another's_ass ass_grab".split()),
]
# 강도(사용자 지시 2026-09-04 「반응이나 motion lines 같은 태그를 누적시켜 더 격하게」): 슬롯당 뽑는 개수. 없는 슬롯은 1. 0 이면 그 슬롯을 건너뛴다
LEVEL_K = {1: {"react": 1, "motion": 0, "climax": 1, "touch": 1}, 2: {"react": 2, "motion": 1, "climax": 2, "touch": 1}, 3: {"react": 3, "motion": 3, "climax": 3, "touch": 2}}
STACK = ("react", "motion", "climax")   # 중·강에서는 이 슬롯에 「없음」이 없다
PENETRATIVE = set(ACTS[:16])   # 체위 16종은 삽입 슬롯이 필수 (vaginal 을 태거가 절반만 달아서 「없음」이 반이나 뽑히던 것을 막는다, 2026-09-03 실측)
# ---- 포즈 모듈 (사용자 지시 2026-09-06) ---- (key, 라벨, 필수, 후보 풀). 사전에 없는 것은 자동으로 빠진다.
# 자세(필수) → 팔 → 손짓 → 다리 → 몸통 → 소지. 표본은 씬과 같다(NSFW 가 있으면 체위의 e 등급 표본, 없으면 디자인 앵커의 표본) —
# 체위 표본에서 뽑으면 on_back·spread_legs 처럼 그 체위에 맞는 자세가 나온다. NSFW 모듈이 이미 고른 태그(반응·자세)는 후보에서 뺀다(중복 방지).
# 표정·시선은 표정 모듈(XSLOTS)로 옮겼다 (2026-09-07). 여기 있던 표정 슬롯은 감정·입·눈이 한 풀이라 smile 로 수렴했다.
PSLOTS = [
 ("posture", "자세", False, "standing sitting lying kneeling squatting walking running jumping all_fours wariza seiza indian_style knees_up hugging_own_legs bent_over on_back on_side on_stomach leaning_forward leaning_back floating reclining fetal_position yokozuwari straddling top-down_bottom-up".split()),
 ("arms", "팔", False, "arms_up arm_up arms_behind_back arms_behind_head arms_at_sides crossed_arms hand_on_own_hip hands_on_own_hips hand_up hands_up outstretched_arm outstretched_arms arm_support spread_arms arm_behind_back arm_behind_head arm_at_side own_hands_together own_hands_clasped interlocked_fingers hands_in_pockets hand_in_pocket arm_under_breasts arms_under_breasts between_legs hands_on_own_knees hand_on_own_knee hand_on_own_thigh hand_on_own_chest hands_on_own_chest arm_across_chest hand_on_own_stomach hands_on_own_thighs".split()),
 ("hand", "손짓", False, "hand_on_own_face hand_on_own_cheek hand_to_own_mouth hand_in_own_hair hands_on_own_face finger_to_mouth finger_to_cheek v double_v salute waving pointing pointing_at_viewer clenched_hand clenched_hands index_finger_raised thumbs_up hand_on_own_head hands_on_own_head hand_on_headwear adjusting_hair adjusting_clothes adjusting_eyewear hair_flip clothes_lift shirt_lift skirt_lift dress_lift shirt_tug clothes_pull heart_hands paw_pose shushing hand_over_own_mouth covering_own_mouth fox_shadow_puppet w".split()),
 ("legs", "다리", False, "crossed_legs legs_apart spread_legs legs_together knees_together_feet_apart knee_up leg_up legs_up feet_up leg_lift standing_on_one_leg crossed_ankles legs_folded tiptoes on_one_knee pigeon-toed soles".split()),
 ("torso", "몸통", False, "arched_back twisted_torso leaning_to_the_side head_tilt head_rest head_down stretching contrapposto hunched_over".split()),
 ("props", "소지·동작", False, "holding_weapon holding_sword holding_gun holding_umbrella holding_phone holding_cup holding_food holding_bag holding_flower holding_book holding_staff holding_fan holding_bouquet holding_stuffed_toy holding_own_hair holding_can holding_bottle holding_knife holding_polearm holding_bow_(weapon) holding_shield holding_pillow holding_microphone holding_instrument holding_cigarette holding_camera holding_controller eating drinking smoking playing_instrument holding_leash".split()),
]
# ---- 씬 모듈 ---- (key, 라벨, 필수, 후보 풀). 사전에 없는 것은 자동으로 빠진다
# 배경 유형(2026-09-08, 사용자 제안 「simple / detailed / all 옵션」): 표본의 56~62% 가 단색·패턴 배경인데 어느 풀에도 없어 뽑힐 길이 없었고, 「배경 채우기」가
#  장소 태그 있는 4~7% 그림에서만 억지로 뽑아 on_bed·desk·ocean·beach 넷이 70% 였다. 풀은 구체 배경 태그만 — 포괄 태그 simple_background 는 풀에서 빼고, 단색 배경이 뽑히면 함께 붙인다(SOLID_BG).
#  배경 유형이 뽑히면 장소·시간·조명·효과를 건너뛰고(표본에서 둘이 겹치는 그림 1%), 「없음」이면 실경이라 장소·시간·조명을 반드시 채운다.
SOLID_BG = set("white_background grey_background blue_background pink_background yellow_background brown_background red_background green_background purple_background orange_background black_background".split())
WHITE_BG = {"white_background", "transparent_background", "grey_background"}   # 「흰·회색 배경 제외」 체크가 후보에서 빼는 것 (사용자 지시 2026-09-08, 회색은 같은 날 추가). 유채색·그라데이션·무늬는 남는다
SSLOTS = [
 ("bg", "배경 유형", False, "white_background grey_background blue_background pink_background yellow_background brown_background red_background green_background purple_background orange_background black_background gradient_background two-tone_background polka_dot_background striped_background checkered_background floral_background heart_background sparkle_background halftone_background argyle_background plaid_background starry_background abstract_background transparent_background".split()),
 ("place", "장소", False, "bedroom classroom bathroom kitchen office library street city cityscape rooftop beach ocean forest field meadow park garden shrine temple alley train_interior car_interior bus_interior hotel_room onsen pool locker_room school hallway stairs cafe restaurant convenience_store shop ruins castle cave dungeon throne_room church balcony veranda living_room bed on_bed toilet_stall changing_room infirmary railing bridge road sidewalk tatami futon desk on_desk against_wall".split()),
 ("sky", "시간·하늘", False, "day night sunset sunrise evening dusk cloudy_sky blue_sky starry_sky moon full_moon crescent_moon overcast twilight".split()),
 ("light", "조명", False, "backlighting sunlight dappled_sunlight light_rays lens_flare sidelighting dim_lighting neon_lights glowing candlelight spotlight shade shadow light_particles sparkle moonlight sunbeam fluorescent_lamp lamp lantern".split()),
 ("fx", "날씨·효과", False, "rain snow wind petals cherry_blossoms falling_leaves falling_petals fog steam bokeh depth_of_field blurry_background motion_blur chromatic_aberration film_grain bubble fireflies glitter lightning splashing water_drop".split()),
 ("frame", "프레이밍", False, "full_body cowboy_shot upper_body portrait close-up wide_shot feet_out_of_frame head_out_of_frame lower_body".split()),
 ("angle", "카메라 각도", False, "from_above from_below from_side from_behind dutch_angle straight-on pov fisheye profile pov_crotch".split()),
]
# ---- 표정 모듈 (사용자 지시 2026-09-07) ---- (key, 라벨, 필수, 뽑는 개수, 후보 풀). 사전에 없는 것은 자동으로 빠진다. 후보 컷 없음(풀 전체가 후보).
# 감정(필수) → 입 → 눈 → 눈썹 → 볼·상태(둘까지: blush + sweat 같은 짝) → 시선. 조건이 앞 슬롯부터 쌓이므로 embarrassed 를 뽑으면 blush 의 조건부 확률이 오른다.
# 형태 태그(thick_eyebrows·lips·eyes_visible_through_hair·tsurime)는 디자인이라 뺐다. 시선은 씬 모듈에서 옮겨 왔다.
XSLOTS = [
 ("emotion", "감정", False, 1, "smile light_smile grin frown expressionless embarrassed smug angry serious pout nervous sleepy surprised scared crying laughing evil_smile seductive_smile naughty_face sweatdrop shy annoyed sad happy worried confused disappointed bored excited determined".split()),
 ("mouth", "입", False, 1, "open_mouth closed_mouth parted_lips :d ;d :o :3 :p ;p :t :< tongue_out wavy_mouth drooling clenched_teeth biting_own_lip teeth".split()),
 ("eyes", "눈", False, 1, "one_eye_closed closed_eyes half-closed_eyes ^_^ >_< wide-eyed jitome rolling_eyes squinting glaring narrowed_eyes empty_eyes sparkling_eyes wince".split()),
 ("brow", "눈썹", False, 1, "furrowed_brow raised_eyebrows v-shaped_eyebrows".split()),
 ("cheek", "볼·상태", False, 2, "blush light_blush full-face_blush nose_blush blush_stickers sweat tears tearing_up crying_with_eyes_open shaded_face heavy_breathing ahegao torogao".split()),
 ("gaze", "시선", False, 1, "looking_at_viewer looking_away looking_back looking_to_the_side looking_up looking_down looking_at_another eye_contact facing_viewer facing_away looking_afar looking_ahead sideways_glance".split()),
]
# 옷 상태 → 디자인에서 걷어 낼 부위
STRIP = {"completely_nude": {"onepiece", "upper", "lower", "outer", "feet"}, "topless_female": {"upper", "onepiece", "outer"},
         "bottomless": {"lower", "onepiece"}, "underwear_only": {"onepiece", "upper", "lower", "outer", "feet"}}

DESIGN_EXCL = {"bra", "no_bra", "strapless_bra", "underwear", "panties", "no_panties", "underwear_only", "lingerie", "bra_lift", "bra_pull", "panty_pull", "nude", "topless_female", "bottomless"}   # 속옷·탈의 상태는 디자인이 아니다 (전 등급 표본에서 상의로 bra 가 뽑히던 것, 2026-09-04)
OUTFIT = {"school_uniform", "gym_uniform", "maid", "military_uniform", "nurse", "waitress", "track_suit", "pajamas", "bunny_costume", "santa_costume", "cheerleader", "office_lady"}   # 상하의를 한 번에 정하는 차림 → 원피스 자리 (상의·하의 슬롯을 건너뛴다)
UPPER_EXTRA = {"serafuku", "sailor_shirt", "gym_shirt", "sports_bra", "sweater_vest", "bikini_top"}   # 부위 표가 상의로 안 잡는 상의 (serafuku 가 「추가 태그」로 빠져 shirt 가 겹쳐 뽑히던 것, 2026-09-04)
def in_slot(tag, slot):
    if slot == "onepiece" and tag in OUTFIT: return True
    if slot == "upper" and tag in UPPER_EXTRA: return True
    if tag in DESIGN_EXCL or (any(tag.endswith("_" + x) for x in DESIGN_EXCL) and tag != "sports_bra"): return False   # white_bra·lace_panties 같은 색·재질 접두 변형도 뺀다
    if slot == "hair_color": return tag in HAIR_COLOR
    if slot == "eye_color": return tag in EYE_COLOR
    if slot == "body": return tag in BODY
    if slot == "hair_len": return tag in HAIR_LEN
    if slot == "hair_style": return tag not in HAIR_LEN and (tag in HAIR_STYLE or (tag.endswith("_hair") and tag not in HAIR_COLOR and tag not in ("floating_hair", "shiny_hair", "wet_hair", "facial_hair", "armpit_hair", "pubic_hair", "chest_hair", "leg_hair", "female_pubic_hair", "male_pubic_hair", "body_hair", "hair_between_eyes")))
    return slot in slots_of(tag) and tag not in HAIR_COLOR and tag not in EYE_COLOR
def slot_of(tag):
    for s, _, _ in SLOTS:
        if in_slot(tag, s): return s
    return None

con = duckdb.connect()
if not ROLL_DB.exists():
    if not Path(LONG).exists(): need_dump(ROLL_DB.name)
    print("굴리기용 축소 DB 를 만드는 중 (첫 1회, 약 30초)…", file=sys.stderr)
    tmp = ROLL_DB.with_suffix(".part")
    tmp.unlink(missing_ok=True)
    con.execute(f"ATTACH '{tmp.as_posix()}' AS build")
    g1 = con.execute(f"SELECT tid FROM '{DICT}' WHERE tag = '1girl'").fetchone()[0]
    con.execute(f"""CREATE TABLE build.tags AS SELECT pid::INTEGER pid, tid::SMALLINT tid, rating
                    FROM '{LONG}' WHERE tid <= {TID_CUT} AND pid >= {ID_SINCE} AND pid IN (SELECT pid FROM '{LONG}' WHERE tid = {g1}) ORDER BY tid, pid""")
    con.execute("DETACH build"); tmp.rename(ROLL_DB)
con.execute(f"ATTACH '{ROLL_DB.as_posix()}' AS roll (READ_ONLY)")
# ---- 그림별 태그 색인 (CSR, 사용자 승인 2026-09-04) ----
# roll.tags 는 태그 순 정렬이라 「태그 → 그림」은 빠르지만 「그림 → 태그」는 2.3억 행을 훑어야 했다(앵커마다 0.4~1초).
# 같은 행을 그림 순으로 놓고 오프셋을 두면 그림의 태그를 바로 찾는다. 파일은 메모리 맵으로 열고 기동 때 순차로 한 번 읽어 OS 캐시를 데운다.
# 실측: 앵커 표본 0.1~0.15초, 부위 질의 45회 0.1초 → 굴리기 한 번 0.3~0.5초 (표본 상한 5만 장).
CSR = {k: DIR / f"_roll_since2016_{k}.npy" for k in ("tids", "offs", "upid", "rating")}
if not all(f.exists() for f in CSR.values()):
    print("그림별 태그 색인을 만드는 중 (첫 1회, 약 30초)…", file=sys.stderr)   # ROLL_DB 만 있으면 만들 수 있다 (덤프가 필요 없다)
    r = con.execute("SELECT pid, tid FROM roll.tags ORDER BY pid").fetchnumpy()
    _pid = r["pid"].astype(np.int32); _tid = r["tid"].astype(np.int16); del r
    _upid, _start = np.unique(_pid, return_index=True)
    _offs = np.append(_start, len(_pid)).astype(np.int64); del _pid, _start
    rr = con.execute("SELECT DISTINCT pid, rating FROM roll.tags").fetchnumpy()
    _rmap = {"g": 0, "s": 1, "q": 2, "e": 3}
    _rat = np.zeros(len(_upid), np.int8)
    _rat[np.searchsorted(_upid, rr["pid"].astype(np.int32))] = np.array([_rmap[x] for x in rr["rating"]], np.int8); del rr
    for k, arr in (("tids", _tid), ("offs", _offs), ("upid", _upid), ("rating", _rat)):
        tmp = CSR[k].with_suffix(".part.npy"); np.save(tmp, arr); tmp.rename(CSR[k])
    del _tid, _offs, _upid, _rat
TIDS = np.load(CSR["tids"], mmap_mode="r"); OFFS = np.load(CSR["offs"], mmap_mode="r")
UPID = np.load(CSR["upid"], mmap_mode="r"); RATING = np.load(CSR["rating"], mmap_mode="r")
# ---- 장르 구획 (사용자 지시 2026-09-04 「판타지만 뽑을지 다 섞을지」 → 2026-09-05 「함선 의인화만 빼고 다」) ----
# 그림마다 장르 비트를 매긴다(uint32, 장르 하나 = 비트 하나). 한 그림이 여러 장르에 들 수 있다 (VTuber 이면서 마법소녀) — 코드 하나로 매기던 구조를 버린 이유.
# 정의는 둘 중 하나다: 작품 목록(copyright 부모 태그 — danbooru 는 하위 작품에 부모 태그 fate_(series) 를 같이 단다) 또는 복식 태그 조합(general 태그).
# ★아카데미·헌터는 작품이 아니라 태그 조합으로 정의한다 (사용자 승인 2026-09-05 「1안으로」). danbooru 에 그 장르의 작품이 거의 없어서다 —
#   마법학원 라노벨 13작품(마법과고교·낙제기사·로쿠데나시…) 합쳐 1girl 5,522장, 한국 헌터물 12작품(나혼렙·전독시·클로저스·카운터사이드…) 8,499장. 앵커 하나만 걸어도 300장 아래.
#   아카데미 = 교복 ∧ 판타지 요소 → 42,376장 (halo 는 뺐다: 블루아카 후광이 34만 장을 끌고 들어온다). 헌터 = 정장 ∧ 무기 ∧ ¬교복에서 다른 장르 작품을 뺀 것 ∪ 한국 작품 (아래 _HUNTER_* 주석).
# 목록의 정본: 판타지 = 판타지 색인 README (45개에서 취향 제외 3개를 뺀 42개) · 현대 = 현대 구획 build_from_dump.py (80개). 나머지 장르는 이 파일이 정본이다 (README 「장르 구획」).
FANTASY_WORKS = """atelier_(series) black_clover bravely_default_(series) breath_of_fire brown_dust_(series) chrono_trigger chrono_cross
dungeon_ni_deai_wo_motomeru_no_wa_machigatteiru_darou_ka dragon's_crown dragon_quest dungeon_meshi epic_seven fairy_tail fate_(series) final_fantasy genshin_impact
goblin_slayer! granblue_fantasy honzuki_no_gekokujou kono_subarashii_sekai_ni_shukufuku_wo! record_of_lodoss_war made_in_abyss magi_the_labyrinth_of_magic mahou_tsukai_no_yome
maplestory mushoku_tensei nanatsu_no_taizai octopath_traveler odin_sphere overlord_(maruyama) princess_connect! re:zero_kara_hajimeru_isekai_seikatsu seiken_densetsu shadowverse
slayers sousou_no_frieren spice_and_wolf sword_art_online tales_of_(series) tate_no_yuusha_no_nariagari tensei_shitara_slime_datta_ken unicorn_overlord the_legend_of_zelda""".split()
MODERN_WORKS = """idolmaster gakuen_idolmaster blue_archive love_live! love_live!_sunshine!! girls_und_panzer danganronpa_(series) bang_dream! persona project_sekai 22/7 zombie_land_saga
chainsaw_man boku_no_hero_academia mahou_shoujo_madoka_magica jujutsu_kaisen dandadan toaru_majutsu_no_index toaru_kagaku_no_railgun neon_genesis_evangelion
k-on! bocchi_the_rock! onii-chan_wa_oshimai! go-toubun_no_hanayome lucky_star kaguya-sama_wa_kokurasetai_~tensai-tachi_no_renai_zunousen~ oshi_no_ko
hibike!_euphonium saenai_heroine_no_sodatekata yahari_ore_no_seishun_lovecome_wa_machigatteiru. spy_x_family sono_bisque_doll_wa_koi_wo_suru toradora!
ijiranaide_nagatoro-san uzaki-chan_wa_asobitai! komi-san_wa_komyushou_desu tokidoki_bosotto_roshia-go_de_dereru_tonari_no_alya-san kanojo_okarishimasu
gabriel_dropout senpai_ga_uzai_kouhai_no_hanashi boku_no_kokoro_no_yabai_yatsu yofukashi_no_uta
monogatari_(series) amagami yuruyuri little_busters! summer_pockets suzumiya_haruhi_no_yuuutsu clannad gochuumon_wa_usagi_desu_ka? higurashi_no_naku_koro_ni
yurucamp angel_beats! watashi_ga_motenai_no_wa_dou_kangaetemo_omaera_ga_warui! make_heroine_ga_oo_sugiru! watashi_ni_tenshi_ga_maiorita! doki_doki_literature_club
ore_no_imouto_ga_konna_ni_kawaii_wake_ga_nai nisekoi machikado_mazoku nichijou sayonara_zetsubou_sensei maria-sama_ga_miteru kagerou_project chuunibyou_demo_koi_ga_shitai!
boku_wa_tomodachi_ga_sukunai kill_me_baby ryuuou_no_oshigoto! yuyushiki saki_(manga) gakkou_gurashi! non_non_biyori yama_no_susume hayate_no_gotoku! to_love-ru
kanon sanoba_witch riddle_joker 9-nine- cafe_stella_to_shinigami_no_chou senren_banka love_plus""".split()
def _lst(ws): return "[" + ",".join("'" + w.replace("'", "''") + "'" for w in ws) + "]"
def _works(ws): return f"list_has_any(copyright_tags, {_lst(ws.split())})"
def _tags(ws): return f"list_has_any(general_tags, {_lst(ws.split())})"
_UNI = "school_uniform serafuku blazer"
_FANT = "cape cloak sword staff wand witch_hat armor gauntlets pauldrons magic magic_circle pointy_ears elf wings horns"
_SUIT = "formal suit necktie black_jacket business_suit black_suit pencil_skirt office_lady"
_WEAP = "weapon sword gun dagger knife holding_weapon polearm axe"
# 어반 판타지 = 현대 복식에 미래 요소를 가미한 중국 서브컬처풍. 니케·라스트 오리진은 한국 제작이지만 같은 계열, 리버스 1999 도 이쪽 (사용자 판정 2026-09-05).
_URBAN_WORKS = "arknights arknights:_endfield girls'_frontline girls'_frontline_2:_exilium girls'_frontline_neural_cloud zenless_zone_zero punishing:_gray_raven path_to_nowhere snowbreak:_containment_zone honkai_impact_3rd reverse:1999 goddess_of_victory:_nikke last_origin"
# 헌터 = 한국 웹툰풍(현대 복식 + 수트 + 무기). 정장∧무기 조합만으로는 아크나이츠·소전·젠존제(어반 판타지)·페이트(현대 기반 서브컬처 판타지)·원신(중국풍 판타지)이 상위를 차지해 헌터물이 아니었다
# (사용자 지적 2026-09-05). 그래서 이미 다른 장르에 속한 작품(판타지·어반 판타지·중화풍·VTuber·동방·블루아카·칸코레·아주르)을 빼고, 한국 헌터물 작품은 조합과 무관하게 넣는다.
# 재정의 후 26,572장, 상위 = original·클로저스·카운터사이드·프로젝트 문·소울워커·전독시·케데헌·체인소맨 (실측 2026-09-05).
_HUNTER_KR = "solo_leveling solo_leveling:_arise omniscient_reader's_viewpoint tower_of_god closers counter:side soulworker sweet_home return_of_the_mount_hua_sect the_beginning_after_the_end lookism kpop_demon_hunters"
_CHINA_WORKS = "honkai:_star_rail wuthering_waves onmyoji shin_sangoku_musou koihime_musou douluo_dalu blade_&_soul kusuriya_no_hitorigoto touqi_guaitan"
_VTUBER_WORKS = "hololive hololive_english nijisanji indie_virtual_youtuber vspo! nijisanji_en hololive_indonesia stellive phase_connect nanashi_inc. vshojo"
_HUNTER_EXCL = " ".join(FANTASY_WORKS) + " " + _URBAN_WORKS + " " + _CHINA_WORKS + " " + _VTUBER_WORKS + " touhou blue_archive kantai_collection azur_lane"
# ★장르와 복식은 서로 다른 축이다 (사용자 지시 2026-09-08). 장르 = 세계관, 복식 = 그 안에 나오는 인물의 의상이라 복식이 더 좁다.
# 전에는 한 목록에 섞여 있었고 작품 기반 장르가 다수라, 장르를 고르는 순간 오리지널 그림이 사라졌다 —
# 실측: 「현대」 101만 장 중 오리지널 1,824장(0.18%), 오리지널 94만 장의 76%는 어느 장르 비트도 안 켜졌다.
# 그래서 작품으로만 정의되는 것을 장르에, 태그로 정의되는 것을 복식에 나눴다. 이름은 겹치지 않게 붙인다(장르 「판타지」 대 복식 「판타지복」).
GENRES = [   # (키, 표시명, SQL 조건) — 작품(copyright_tags)으로만 판정한다. 순서가 비트 번호이자 UI 순서다. 중간에 끼우면 비트 파일을 다시 만들어야 한다
    ("fantasy", "판타지", _works(" ".join(FANTASY_WORKS))),
    ("school", "일상·학원물", _works(" ".join(MODERN_WORKS))),
    ("hunter", "헌터 (정장+무기, 한국 웹툰풍)", f"({_tags(_SUIT)} AND {_tags(_WEAP)} AND NOT {_tags(_UNI)} AND NOT {_works(_HUNTER_EXCL)}) OR {_works(_HUNTER_KR)}"),
    ("urban", "어반 판타지 (중국 서브컬처풍)", _works(_URBAN_WORKS)),
    ("vtuber", "VTuber", _works(_VTUBER_WORKS)),
    ("idol", "아이돌", _works("idolmaster love_live! bang_dream! project_sekai aikatsu!_(series) pripara pretty_series 22/7 zombie_land_saga tokyo_7th_sisters re:stage! shoujo_kageki_revue_starlight oshi_no_ko")),
    ("gun", "총기 소녀·군사", _works("girls'_frontline girls'_frontline_2:_exilium goddess_of_victory:_nikke girls_und_panzer world_witches_series lycoris_recoil last_origin gunslinger_girl 86_-eightysix- upotte!! senjou_no_valkyria_(series) heavily_armed_high_school_girls")),
    ("mecha", "메카·거대로봇", _works("gundam neon_genesis_evangelion macross code_geass darling_in_the_franxx muv-luv tengen_toppa_gurren_lagann phantasy_star metroid super_robot_wars xenoblade_chronicles_(series) nier_(series) gridman_universe")),
    ("magical", "마법소녀", _works("mahou_shoujo_madoka_magica precure lyrical_nanoha bishoujo_senshi_sailor_moon cardcaptor_sakura senki_zesshou_symphogear magia_record:_mahou_shoujo_madoka_magica_gaiden mahou_shoujo_ni_akogarete mahou_shoujo_no_majo_saiban")),
    ("china", "중화 세계관", _works(_CHINA_WORKS)),
    ("fighting", "격투 게임", _works("street_fighter guilty_gear the_king_of_fighters tekken blazblue darkstalkers dead_or_alive soulcalibur skullgirls fatal_fury under_night_in-birth")),
    ("dark", "다크 판타지", _works("elden_ring dark_souls_(series) bloodborne berserk claymore_(series) made_in_abyss goblin_slayer! overlord_(maruyama) fear_&_hunger_(series) warhammer_40k the_witcher_(series) dungeon_meshi")),
    ("monster", "몬스터 소녀", _works("monster_girl_encyclopedia monster_musume_no_iru_nichijou mon-musu_quest! helltaker jashin-chan_dropkick")),
]
# 복식 (사용자 승인 2026-09-08, 10종) — 태그로만 판정한다. 최대 2개까지 고르고 **OR 로 풀을 넓힌다**(AND 아님 — 사용자 정정:
# 「섞는건 and가 아님. 둘 다 나올수 있다는 거지. 풀을 늘리는 거임」). AND 로 걸면 앵커까지 얹었을 때 32장이라 못 쓴다(실측).
# 표본만 넓힐 뿐 질의에는 넣지 않는다 — 둘 다 맞는 그림을 우대하면 그게 곧 soft AND 라, 사용자가 뜻한 「풀을 늘리는 것」이 아니게 된다.
COSTUMES = [
    ("modern", "현대복", "school_uniform serafuku blazer hoodie t-shirt jeans denim_shorts sneakers cardigan business_suit office_lady pencil_skirt necktie casual sweater hood_up overalls"),
    ("swim", "수영복", "swimsuit bikini one-piece_swimsuit school_swimsuit competition_swimsuit"),
    ("fantasy", "판타지복", "cape cloak robe armor gauntlets pauldrons breastplate witch_hat wizard_hat circlet tiara chainmail tabard surcoat fur_cloak shoulder_armor faulds"),
    ("wafuku", "일본옷", "kimono yukata japanese_clothes hakama haori obi furisode wide_sleeves miko geta tabi sarashi happi jinbei"),
    ("gym", "운동복", "gym_uniform buruma sportswear track_jacket leotard sports_bra bike_shorts"),
    ("uniform", "제복·직업복", "maid nun police_uniform nurse waitress apron maid_headdress flight_attendant"),
    ("gothic", "고딕·빅토리안", "gothic_lolita lolita_fashion victorian steampunk corset frilled_dress"),
    ("bodysuit", "바디수트·SF복", "bodysuit pilot_suit power_armor cyberpunk science_fiction robot_joints"),
    ("china", "중화복", "chinese_clothes china_dress hanfu tangzhuang"),
    ("military", "군복", "military_uniform camouflage flak_jacket tactical_clothes combat_boots military_jacket epaulettes"),
]
COSTUME_BIT = {k: 1 << i for i, (k, _, _) in enumerate(COSTUMES)}
COSTUME_TAGS = {k: v.split() for k, _, v in COSTUMES}
GENRE_BIT = {k: 1 << i for i, (k, _, _) in enumerate(GENRES)}
# ★장르도 복식처럼 여러 개를 골라 OR 로 풀을 넓힌다 (사용자 지시 2026-09-09 「갯수제한없이 계속 섞을 수 있게」).
#  전에는 드롭다운 하나라 장르가 늘 한 개였다. 복식과 같은 방식으로 맞춰 두 축의 조작이 같아졌다.
def segs_of(seg):
    """장르 인자를 키 튜플로 고른다. 문자열('all'·'fantasy'·'fantasy,school')도 받는다. 사전에 없는 것과 'all' 은 걸러진다."""
    if isinstance(seg, str): seg = [x.strip() for x in seg.split(',')]
    return tuple(x for x in seg if x in GENRE_BIT)
def seg_bits(segs):
    """고른 장르들의 비트 합 (OR). 비면 0 = 장르 조건 없음."""
    b = 0
    for x in segs: b |= GENRE_BIT.get(x, 0)
    return b
SEG_FILE = DIR / "_roll_since2016_genre_v2.npy"   # v2 = 장르·복식 분리 (2026-09-08). 옛 _genre.npy 는 비트 배치가 다르다
if not SEG_FILE.exists():
    if not Path(DB).exists(): need_dump(SEG_FILE.name)
    print(f"그림별 장르 비트를 매기는 중 (첫 1회, 장르 {len(GENRES)}개, 약 1분)…", file=sys.stderr)
    expr = " + ".join(f"(CASE WHEN {cond} THEN {1 << i} ELSE 0 END)" for i, (_, _, cond) in enumerate(GENRES))
    r = duckdb.connect(DB, read_only=True).execute(f"""
        SELECT id, ({expr})::BIGINT AS g FROM post WHERE list_contains(general_tags, '1girl') ORDER BY id""").fetchnumpy()
    _seg = np.zeros(len(UPID), np.uint32)
    _ids = r["id"].astype(np.int32); _sv = r["g"].astype(np.uint32)
    _pos = np.searchsorted(UPID, _ids); _ok = (_pos < len(UPID)) & (UPID[np.minimum(_pos, len(UPID) - 1)] == _ids)
    _seg[_pos[_ok]] = _sv[_ok]
    tmp = SEG_FILE.with_suffix(".part.npy"); np.save(tmp, _seg); tmp.rename(SEG_FILE); del r, _seg, _ids, _sv, _pos, _ok
SEGMENT = np.load(SEG_FILE, mmap_mode="r")
COS_FILE = DIR / "_roll_since2016_costume.npy"
if not COS_FILE.exists():
    if not Path(DB).exists(): need_dump(COS_FILE.name)
    print(f"그림별 복식 비트를 매기는 중 (첫 1회, 복식 {len(COSTUMES)}종, 약 1분)…", file=sys.stderr)
    expr = " + ".join(f"(CASE WHEN {_tags(tg)} THEN {1 << i} ELSE 0 END)" for i, (_, _, tg) in enumerate(COSTUMES))
    r = duckdb.connect(DB, read_only=True).execute(f"""
        SELECT id, ({expr})::BIGINT AS g FROM post WHERE list_contains(general_tags, '1girl') ORDER BY id""").fetchnumpy()
    _cos = np.zeros(len(UPID), np.uint32)
    _ids = r["id"].astype(np.int32); _cv = r["g"].astype(np.uint32)
    _pos = np.searchsorted(UPID, _ids); _ok = (_pos < len(UPID)) & (UPID[np.minimum(_pos, len(UPID) - 1)] == _ids)
    _cos[_pos[_ok]] = _cv[_ok]
    tmp = COS_FILE.with_suffix(".part.npy"); np.save(tmp, _cos); tmp.rename(COS_FILE); del r, _cos, _ids, _cv, _pos, _ok
COSTUME_SEG = np.load(COS_FILE, mmap_mode="r")
# ---- 캐릭터당 상한 (사용자 승인 2026-09-07 「1안·캐릭터 기준」) ----
# 그림마다 「첫 캐릭터 태그」의 정수 id 를 매긴다 (int32, 0 = 캐릭터 태그 없음 = 오리지널). 표본을 뜰 때 같은 캐릭터의 그림을 K장까지만 넣는다.
# 왜: 표본이 작을 때 인기 캐릭터 한 명이 표본을 삼킨다 — 현대 ∩ dog_ears ∧ dog_tail 2,047장 중 히비키(블루아카) 1,442장(70%)이라 흑발 65%·치어리더 49% 로 굴렸다.
# 실측(같은 표본, 캐릭터당 100장): 705장으로 줄며 흑발 22%·치어리더 11%, 대신 school_uniform·shirt·dress 가 올라온다. 전체 장르 15,626장에서는 흑발 16%→12% 로 큰 표본은 거의 안 건드린다.
# 2026-09-05 에 접은 「캐릭터 1명 = 1표」와 다르다 — 그때는 오리지널 그림이 가중치의 77% 가 되는 부작용이었고, 상한은 오리지널을 건드리지 않고 상위 몇 캐릭터만 깎는다.
CHAR_FILE = DIR / "_roll_since2016_char.npy"
if not CHAR_FILE.exists():
    if not Path(DB).exists(): need_dump(CHAR_FILE.name)
    print("그림별 캐릭터 id 를 매기는 중 (첫 1회, 약 1분)…", file=sys.stderr)
    r = duckdb.connect(DB, read_only=True).execute("""
        SELECT id, dense_rank() OVER (ORDER BY character_tags[1]) AS c FROM post
        WHERE list_contains(general_tags, '1girl') AND len(character_tags) > 0 ORDER BY id""").fetchnumpy()
    _ch = np.zeros(len(UPID), np.int32)
    _ids = r["id"].astype(np.int32); _cv = r["c"].astype(np.int32)
    _pos = np.searchsorted(UPID, _ids); _ok = (_pos < len(UPID)) & (UPID[np.minimum(_pos, len(UPID) - 1)] == _ids)
    _ch[_pos[_ok]] = _cv[_ok]
    tmp = CHAR_FILE.with_suffix(".part.npy"); np.save(tmp, _ch); tmp.rename(CHAR_FILE); del r, _ch, _ids, _cv, _pos, _ok
CHAR = np.load(CHAR_FILE, mmap_mode="r")
def cap_chars(pids, cap, rng):
    """같은 캐릭터 태그의 그림을 cap 장까지만 남긴다 (rng 로 어느 장을 남길지 정한다 — 고정 시드라 재현 가능). 캐릭터 태그가 없는 그림은 그대로. cap 0 이면 그대로."""
    if not cap or not len(pids): return pids
    c = np.asarray(CHAR[np.searchsorted(UPID, pids)])
    perm = rng.permutation(len(pids)); cp = c[perm]
    s = np.argsort(cp, kind="stable"); cs = cp[s]
    first = np.r_[True, cs[1:] != cs[:-1]]                 # 그룹 시작
    start = np.maximum.accumulate(np.where(first, np.arange(len(cs)), 0))
    rank = np.arange(len(cs)) - start                       # 그룹 안 순번
    keep = (cs == 0) | (rank < cap)
    return np.sort(pids[perm[s[keep]]])
def warm():
    """순차로 한 번 읽어 OS 캐시를 데운다 (HDD 첫 회 4~5초, 이미 캐시에 있으면 0.2초). 안 하면 첫 굴리기가 HDD 무작위 읽기가 된다."""
    t = time.time()
    for arr in (TIDS, OFFS, UPID, RATING, SEGMENT, COSTUME_SEG, CHAR):
        for i in range(0, len(arr), 20_000_000): int(arr[i:i + 20_000_000].sum())
    print(f"색인 캐시 데우기 {time.time()-t:.1f}s", file=sys.stderr)
warm()
SAMPLE_CAP = 50000         # 앵커 그림이 이보다 많으면 시드로 무작위 추출. 비율 오차 0.5%p 안 (사용자 승인 2026-09-04)
NAME = {tid: tag for tag, tid in con.execute(f"SELECT tag, tid FROM '{DICT}'").fetchall()}
TID = {v: k for k, v in NAME.items()}
TOPTAGS = [t for t, in con.execute(f"SELECT tag FROM '{DICT}' WHERE n >= 300 ORDER BY n DESC").fetchall()]
# ★색인만 있고 원본 표(`_tags_long.parquet`, 1.6GB)가 없으면 상위 12,000 밖의 태그는 앵커로 쓸 수 없다 —
#   그 태그가 달린 그림 목록이 원본에만 있다 (`posting`). 내려받은 색인으로 도는 것이 보통이므로 그때는
#   **고를 수 있는 것만** 자동완성에 보인다. 원본이 함께 있으면 원본대로 전부 보인다.
HAVE_LONG = Path(LONG).exists()
if not HAVE_LONG: TOPTAGS = [t for t in TOPTAGS if TID.get(t, TID_CUT + 1) <= TID_CUT]
# 부위별 태그 풀 (상위 12,000 태그 중 in_slot 이 참인 것). 굴리기 루프가 표본의 태그 5천 개마다 in_slot(문자열 검사)을 부르면 슬롯당 50ms, 굴리기당 0.7초가 들었다 (2026-09-04 실측)
SLOT_POOL = {slot: [tid for tid in range(1, TID_CUT + 1) if tid in NAME and in_slot(NAME[tid], slot)] for slot, _, _ in SLOTS}
SLOT_POOL_ARR = {k: np.array(v, dtype=np.int64) for k, v in SLOT_POOL.items()}

# ---- 포즈·표정 궁합 표 (사용자 승인 2026-09-08 「lift는 알아서 해」) ----
# 왜 필요한가 (실측): 급 사다리는 조건 급이 평활 상수(300)보다 작으면 조건을 사실상 못 건다.
#   앵커 표본 705장 → all_fours 급 22장 → 22/(22+300) = 7% 만 자료, 93% 는 「질의를 일부만 맞춘 그림」이 채운다.
#   그래서 P(waving | all_fours) 가 1.61% 로 나왔다 — 실제는 0.066% 이고, 1.61% 는 P(waving | arm_up)=0.97% 쪽 값이다.
#   자세를 고정하고 25회씩 굴리면 all_fours 는 7/25 가 모순 조합을 물었다(서기·무릎꿇기는 0/25).
# 처방: 색인 전체(단일 인물 그림)에서 잰 실측 lift 를 후보 확률에 곱한다. all_fours 만 47,125장이라 이쪽 표본은 넉넉하다.
#   배경 유형의 둘째 태그에서 같은 병을 pair_p() 로 고친 것과 같은 처방이다.
SOLO_EXCL = ("comic", "multiple_views", "4koma", "reference_sheet", "character_sheet", "collage")
PAIR_FILE = DIR / "_roll_pairs_solo.npz"
PAIR_TAGS = sorted({t for _, _, _, pool in PSLOTS for t in pool} | {t for _, _, _, _, pool in XSLOTS for t in pool}
                   | {t for _, _, _, pool in SSLOTS for t in pool})   # 씬도 같은 병이 있다 (밤 + 햇빛 등)
if not PAIR_FILE.exists():
    if not ROLL_DB.exists(): need_dump(PAIR_FILE.name)
    print(f"포즈·표정·씬 궁합 표를 만드는 중 (첫 1회, 태그 {len(PAIR_TAGS)}개, 약 1분)…", file=sys.stderr)
    _c = duckdb.connect()
    _c.execute(f"ATTACH '{ROLL_DB.as_posix()}' AS rr (READ_ONLY)")
    _c.execute(f"CREATE VIEW d AS SELECT * FROM '{DICT}'")
    _have = [t for t in PAIR_TAGS if t in TID]
    _lit = "(" + ",".join(str(TID[t]) for t in _have) + ")"
    _ex = [t for t in ("solo",) + SOLO_EXCL if t in TID]
    _c.execute(f"""CREATE TABLE pop AS SELECT pid FROM rr.tags WHERE tid = {TID['solo']}
                   EXCEPT SELECT pid FROM rr.tags WHERE tid IN ({','.join(str(TID[t]) for t in _ex if t != 'solo')})""")
    _tot = _c.execute("SELECT count(*) FROM pop").fetchone()[0]
    _n = _c.execute(f"""SELECT t.tid, count(*) FROM rr.tags t JOIN pop ON t.pid = pop.pid
                        WHERE t.tid IN {_lit} GROUP BY 1""").fetchall()
    _pr = _c.execute(f"""SELECT a.tid, b.tid, count(*) FROM rr.tags a JOIN rr.tags b ON a.pid = b.pid AND a.tid < b.tid
                         JOIN pop ON a.pid = pop.pid WHERE a.tid IN {_lit} AND b.tid IN {_lit} GROUP BY 1, 2""").fetchall()
    np.savez_compressed(PAIR_FILE, total=np.int64(_tot),
                        ntid=np.array([r[0] for r in _n], np.int32), ncnt=np.array([r[1] for r in _n], np.int64),
                        pa=np.array([r[0] for r in _pr], np.int32), pb=np.array([r[1] for r in _pr], np.int32),
                        pn=np.array([r[2] for r in _pr], np.int64))
    _c.close(); del _c, _n, _pr
_pz = np.load(PAIR_FILE)
PAIR_TOTAL = int(_pz["total"])
PAIR_N = dict(zip(_pz["ntid"].tolist(), _pz["ncnt"].tolist()))
PAIR_BOTH = {(int(a), int(b)): int(n) for a, b, n in zip(_pz["pa"], _pz["pb"], _pz["pn"])}
del _pz
PAIR_FLOOR, PAIR_CAP, PAIR_MIN_N = 0.02, 20.0, 100   # 바닥·천장, 그리고 조건 태그가 이보다 드물면 보정하지 않는다(잡음)
def pair_lift(a, b):
    """단일 인물 그림에서 잰 lift = P(b | a) / P(b). 표본이 모자라면 None (보정 안 함)."""
    ta, tb = TID.get(a), TID.get(b)
    if ta is None or tb is None: return None
    na, nb = PAIR_N.get(ta, 0), PAIR_N.get(tb, 0)
    if na < PAIR_MIN_N or nb <= 0: return None
    both = PAIR_BOTH.get((ta, tb) if ta < tb else (tb, ta), 0)
    return (both / na) / (nb / PAIR_TOTAL)
def pair_adjust(cands, picked, none_p, req):
    """이미 고른 태그와의 실측 궁합으로 후보 확률을 보정한다. 후보 전체가 안 어울리면 그만큼 「없음」이 늘어난다.
    반환 (보정된 cands, 보정된 none_p)."""
    if not picked or not cands: return cands, none_p
    out, s0, s1 = [], 0.0, 0.0
    for t, conf, lf, k in cands:
        m = 1.0
        for pk in picked:
            v = pair_lift(pk, t)
            if v is not None: m *= v
        m = min(max(m, PAIR_FLOOR), PAIR_CAP)
        s0 += conf; s1 += conf * m
        out.append((t, conf * m, lf, k))
    if req or s0 <= 0: return out, none_p
    return out, min(0.995, 1 - (1 - none_p) * min(1.0, s1 / s0))
def _is_kemono(t):
    """수인 요소: 동물귀·꼬리·animal_ear_fluff·○○_girl(동물). 엘프귀·악마 꼬리·용 꼬리는 종족 파츠지 수인이 아니라 뺀다."""
    if t in ("pointy_ears", "long_pointy_ears", "demon_tail", "dragon_tail", "demon_girl", "dragon_girl"): return False
    return (t.endswith("_ears") or t.endswith("_tail") or t.startswith("tail_") or t.endswith("_ear_fluff")
            or t in ("animal_ears", "animal_ear_fluff", "tail", "kemonomimi_mode", "animal_ear_headphones", "paw_pose", "animal_hands", "claws")
            or (t.endswith("_girl") and t.split("_")[0] in "cat fox dog wolf mouse rabbit bunny horse cow bear sheep deer raccoon tiger squirrel bird goat pig monkey lion leopard".split()))
FOCUS_BETA = 0.8   # 「앵커 특화」 체크(사용자 결정 2026-09-05 「앵커 특유만 하나 넣기」): 문턱 대신 conf × lift^0.8 로 후보를 고른다.
# 실측(앵커 6개 × 시드 12): β 0·0.3 은 현재 문턱과 구분이 안 되고 0.8 만 뚜렷(뽑힌 태그 평균 lift 3.5~8.6 → 10.9~47.4). 그래서 단계 대신 체크 하나.
KEMONO = {t for t in TID if _is_kemono(t)}   # 183개 (2026-09-05). 옵션 「수인」을 끄면 디자인 후보와 빈 칸 앵커에서 뺀다 — 동물귀·꼬리는 아주 자주 쓰는 요소라 따로 둔다 (사용자 지시)
NPOST = {}; BASE = {}
def base_for(rating):
    if rating not in BASE:
        rf = {"all": "1=1", "gs": "rating IN ('g','s')", "e": "rating = 'e'"}[rating]
        NPOST[rating] = int(CONSTS["nposts"][rating])   # ★덤프에서 한 번 재어 매니페스트에 적어 둔 값 (원본은 여기서 danbooru.duckdb 를 열었다)
        p = DIR / f"_tag_counts_since2016_{rating}.parquet"   # tagassoc.py 의 _tag_counts_<rating> 은 전 기간이라 따로 둔다
        if not p.exists():
            if not Path(LONG).exists(): need_dump(p.name)
            con.execute(f"COPY (SELECT tid, count(*) AS n FROM '{LONG}' WHERE {rf} AND pid >= {ID_SINCE} GROUP BY 1) TO '{p.as_posix()}' (FORMAT PARQUET)")
        BASE[rating] = dict(con.execute(f"SELECT tid, n FROM '{p.as_posix()}'").fetchall())
    return BASE[rating], NPOST[rating]
LOCK = threading.Lock()
POSTING = {}
POSTING_CAP = 400000       # 앵커 후보 목록 상한. long_hair(470만 장)를 통째로 꺼내면 3.6초라, DuckDB 저수지 표본(고정 시드)으로 40만 장만 받는다. 표본 상한 5만보다 넉넉하다
def posting(tag, full=False):
    """태그가 달린 그림 id(정렬). 상위 12,000 안이면 roll.tags(태그 순 정렬이라 프루닝), 밖이면 parquet 원본에서.
    full=False 면 40만 장 상한(저수지 표본, 재현 가능). 마스크를 만드는 남성 태그처럼 전부 필요할 때만 full=True."""
    key = (tag, full)
    if key in POSTING: return POSTING[key]
    tid = TID[tag]
    if tid > TID_CUT and not HAVE_LONG:
        raise ValueError(f"「{tag}」 는 색인 밖의 태그입니다 (상위 12,000 안의 태그만 앵커로 쓸 수 있습니다)")
    src = "roll.tags" if tid <= TID_CUT else f"'{LONG}'"
    q = f"SELECT pid FROM {src} WHERE tid = {tid}"
    if not full: q = f"SELECT pid FROM ({q}) USING SAMPLE reservoir({POSTING_CAP} ROWS) REPEATABLE (1)"   # ★서브쿼리 필수 — 바깥에 두면 WHERE 보다 먼저 전체 표에서 표본을 떠 태그마다 전체 스캔이 된다 (2026-09-04 실측)
    p = con.execute(q).fetchnumpy()["pid"].astype(np.int32)
    p = np.sort(p)   # (pid, tid) 는 표에서 유일하므로 정렬만 하면 된다. np.unique 는 해시 방식이라 1boy 240만 장에서 0.6초 (2026-09-05 프로파일)
    if src != "roll.tags": p = p[np.isin(p, UPID)]         # 1girl 우주 밖의 그림은 색인에 없다
    POSTING[key] = p; return p

class Sample:
    """앵커 그림들의 (행, 태그) 배열. 조건 집계는 마스크 AND + bincount."""
    def __init__(self, pids, rng):
        if len(pids) > SAMPLE_CAP: pids = np.sort(rng.choice(pids, SAMPLE_CAP, replace=False))
        idx = np.searchsorted(UPID, pids)
        lens = (OFFS[idx + 1] - OFFS[idx]).astype(np.int64)
        starts = np.asarray(OFFS[idx], dtype=np.int64)
        gather = np.repeat(starts - np.cumsum(lens) + lens, lens) + np.arange(int(lens.sum()), dtype=np.int64)
        self.n = len(idx); self.idx = idx; self.rows = np.repeat(np.arange(self.n, dtype=np.int32), lens); self.stid = np.asarray(TIDS[gather])   # idx: 행 → UPID 색인
        self.char = np.asarray(CHAR[idx]) if self.n else np.zeros(0, np.int32)   # 그림별 캐릭터 id (급 안 인물당 상한용, 2026-09-07)
        self._finish()
    @classmethod
    def from_arrays(cls, rows, stid, char=None):
        """전역 색인 없이 (행, 태그) 배열로 만든 표본 — 결정적 회귀 테스트용 (2026-09-07)."""
        self = cls.__new__(cls)
        self.rows = np.asarray(rows, dtype=np.int32); self.stid = np.asarray(stid, dtype=np.int16)
        self.n = int(self.rows.max()) + 1 if len(self.rows) else 0; self.idx = np.arange(self.n)
        self.char = np.asarray(char, dtype=np.int32) if char is not None else np.zeros(self.n, np.int32)
        self._finish(); return self
    def _finish(self):
        self.masks = {}; self.curs = {}; self.cntv = {}; self.anym = {}; self.tcache = {}
        self.offs = np.searchsorted(self.rows, np.arange(self.n + 1, dtype=self.rows.dtype))          # 그림 i 의 태그 = stid[offs[i]:offs[i+1]]
        self.well = (self.offs[1:] - self.offs[:-1]) >= WELL_TAGS                                      # 태그가 넉넉한 그림 (「없음」 결측 처리용)
        self.order = np.argsort(self.stid, kind="stable"); self.sst = self.stid[self.order]   # 태그 순 정렬 → 태그 하나의 행들을 이진 탐색으로 (평활 후 속도, 2026-09-05)
    def mask(self, tag):
        tid = TID.get(tag, -1)
        if tid not in self.masks:
            lo, hi = np.searchsorted(self.sst, np.array([tid, tid + 1], dtype=self.sst.dtype))   # ★dtype 을 맞춰야 한다 — 파이썬 int 를 주면 1.5M 배열을 int64 로 매번 복사해 16ms (2026-09-05 실측)
            m = np.zeros(self.n, bool); m[self.rows[self.order[lo:hi]]] = True; self.masks[tid] = m
        return self.masks[tid]
    def cur(self, fixed):
        key = tuple(fixed)
        if key not in self.curs:
            m = np.ones(self.n, bool)
            for t in fixed: m &= self.mask(t)
            self.curs[key] = m; _cap(self.curs, 2000)
        return self.curs[key]
def _cap(d, n):
    """표본이 앵커마다 고정되어 굴릴수록 조건 캐시가 쌓이므로 오래된 것부터 버린다 (집계 벡터 하나 96KB)."""
    while len(d) > n: d.pop(next(iter(d)))

SUB = {}
def _sample(key, pids_fn, seed=None, cap=0):
    """표본 캐시. 키는 조건뿐 — 앵커(조건)마다 5만 장 표본을 한 번만 뽑아 고정한다 (사용자 승인 2026-09-05 「1안으로」).
    시드는 후보 추첨에만 작용한다. 전에는 (조건, 시드)로 시드마다 새 표본을 떠서 재굴림마다 역색인·집계를 다시 했다(0.6초 → 0.25초).
    표본 상한 5만에서 비율 오차 0.5%p 안이라 결과 분포는 같다. 최근 24개만 둔다."""
    k = key + (cap,)
    if k in SUB: return SUB[k]
    rng = np.random.default_rng(1)
    p = pids_fn(); n_raw = len(p)
    smp = Sample(cap_chars(p, cap, rng), rng); smp.n_raw = n_raw   # n_raw = 캐릭터당 상한 전 장수 (화면 표시용)
    if len(SUB) >= 24: SUB.pop(next(iter(SUB)))
    SUB[k] = (smp, smp.n); return SUB[k]

BOY = None
def boy_mask():
    """우주(1girl 그림) 위의 「남성 있음」 마스크. setdiff 정렬 대신 한 번 만들어 인덱싱으로 거른다 (금발 103만 장 앵커에서 1.3초 → 0.5초)."""
    global BOY
    if BOY is None:
        BOY = np.zeros(len(UPID), bool)
        for b in ("1boy", "2boys", "multiple_boys"):
            if b in TID: BOY[np.searchsorted(UPID, posting(b, full=True))] = True
    return BOY

SOLO = None
# 만화·여러 컷·복수 인물 제외 (사용자 지시 2026-09-08 「단일 캐릭터 디자인이 목적이니까 만화컷 같은건 다 빼」).
# ★왜 중요한가 — 실측: all_fours + waving 이 같이 달린 31장을 받아 보니 만화(다른 칸)·같은 캐릭터 여러 포즈 모음·남녀가 하나씩 하는 그림이었다.
# 한 인물이 동시에 두 자세를 하는 그림이 아닌데도 동시 출현으로 세어져, 못 그리는 조합의 확률을 부풀리고 있었다.
# solo 로 좁히면 모순 쌍의 lift 가 2~14배 더 떨어지고(all_fours+waving 0.23→0.11) 자연스러운 쌍은 오히려 오른다(all_fours+paw_pose 3.87→5.34).
def solo_mask():
    """우주 위의 「단일 인물 그림」 마스크 = solo 이고 만화·여러 컷 모음이 아닌 것."""
    global SOLO
    if SOLO is None:
        SOLO = np.zeros(len(UPID), bool)
        if "solo" in TID: SOLO[np.searchsorted(UPID, posting("solo", full=True))] = True
        for t in SOLO_EXCL:
            if t in TID: SOLO[np.searchsorted(UPID, posting(t, full=True))] = False
    return SOLO

def cos_mask(idx, cos):
    """복식 비트 마스크 (OR — 고른 복식 중 하나라도 맞으면 통과). cos 가 비면 전부 통과."""
    bits = 0
    for c in cos:
        bits |= COSTUME_BIT.get(c, 0)
    return np.ones(len(idx), bool) if not bits else (COSTUME_SEG[idx] & bits) != 0
SEG_MIN = 300              # 앵커 목록이 40만 상한에 잘렸을 때 전체 목록으로 다시 세는 문턱, 그리고 빈 칸 굴리기의 앵커 채택 문턱
def sub_table(anchor, rating, seed=0, seg="all", cap=0, cos=()):
    """앵커 태그(들)가 전부 있고 남성이 없는 단일 인물(solo) 1girl 그림. anchor 는 문자열 또는 튜플(AND).
    seg = 장르 키 목록, cos = 복식 키 목록 — 둘 다 개수 제한 없이 OR 로 풀을 넓힌다. 비면 그 조건 없음.
    cap = 캐릭터당 상한(0 이면 없음, cap_chars)."""
    anchors = (anchor,) if isinstance(anchor, str) else tuple(anchor)
    cos = tuple(c for c in cos if c in COSTUME_BIT)
    segs = segs_of(seg); bit = seg_bits(segs)
    key = ("design", anchors, rating, segs, cos)
    def filt(p):
        idx = np.searchsorted(UPID, p); keep = ~boy_mask()[idx] & solo_mask()[idx] & cos_mask(idx, cos)
        if rating == "gs": keep &= RATING[idx] <= 1
        return idx, keep
    def pids():
        p = posting(anchors[0])
        for a in anchors[1:]: p = np.intersect1d(p, posting(a), assume_unique=True)
        idx, keep = filt(p)
        if not bit: return p[keep]
        k2 = keep & ((SEGMENT[idx] & bit) != 0); n2 = int(k2.sum())
        if n2 < SEG_MIN and any(len(posting(a)) >= POSTING_CAP for a in anchors):
            # 층화 추출: 앵커 목록이 40만 장 상한에 잘린 것이면 상한 때문에 장르 그림이 비례로 깎인 것이라(long_hair 250만 장 → 40만), 전체 목록으로 다시 센다 (2026-09-05)
            p = posting(anchors[0], full=True)
            for a in anchors[1:]: p = np.intersect1d(p, posting(a, full=True), assume_unique=True)
            idx, keep = filt(p); k2 = keep & ((SEGMENT[idx] & bit) != 0); n2 = int(k2.sum())
        SEG_INFO[key] = (n2, n2 < STOP_RATIO * M_PRIOR)   # (장르 안 앵커 그림 수, 사전분포가 지배하는가 → 화면 표시)
        # ★언제나 교집합을 쓴다. 작으면 roll() 이 「장르 전체 사다리 × 앵커 lift」를 사전분포로 얹어 장르·앵커 둘 다 반영한다 (평활, 2026-09-05).
        # 그 전 한때는 여기서 장르 전체로 갈아탔고(앵커 조건 상실), 처음엔 장르를 버렸다(전 장르와 같은 디자인).
        return p[k2]
    return _sample(key, pids, seed, cap)
SEG_INFO = {}

def nsfw_sub(act, seed=0):
    """행위 태그가 있는 e 등급 1girl 그림 (상대 남성 허용)."""
    def pids():
        p = posting(act); return p[RATING[np.searchsorted(UPID, p)] == 3]
    return _sample(("nsfw", act), pids, seed)

# ---- 후보 확률: Dirichlet 사전분포 평활 (사용자 승인 2026-09-05 「적용」) ----
# 전에는 계단이 셋이었다: 조건 표본이 문턱(디자인 600·NSFW 400·씬 800) 아래면 조건을 풀고, 후보는 30장(씬 20장) 이상만, 장르 ∩ 앵커 < 300 이면 되돌림.
# 경계에서 결과가 뚝 바뀌고(299장/301장), 작은 장르에서는 장르가 통째로 무시됐다. 이제 식 하나다:
#   P̂(B | 조건) = (k + m·P₀(B)) / (n + m)     k = 조건 그림 중 B 있는 수, n = 조건 그림 수, P₀ = 한 단계 넓은 조건의 같은 식(재귀)
# 사다리(levels_of)는 옛 완화 순서 그대로다 — pre+picked → picked 의 오래된 것부터 뺌 → pre → 표본 전체. 표본 자체가 작으면(n < STOP_RATIO·m)
# 그 아래에 넓은 표본(장르 전체, 장르가 없으면 전체 1girl)의 같은 사다리를 두고, 장르 ∩ 앵커가 작은 경우엔 거기에 앵커 표본의 lift 를 곱한다
# (「장르답고 앵커와도 어울리는」 사전분포 — 나이브 베이즈 결합). n ≫ m 이면 자료가, n ≪ m 이면 사전이 지배하고 그 사이는 연속이다.
# m 은 measure_m.py 가 보류 표본 우도로 정한다 (README 「후보 확률」). 정보검색의 Dirichlet prior smoothing (Zhai & Lafferty 2001) 과 같은 식.
M_PRIOR = 300

def is_req(must, slot):
    """그 슬롯이 「반드시 뽑기」인가. must 는 True(켜진 슬롯 전부) 또는 슬롯 키 모음.
    ★일괄 옵션에서 슬롯별로 바꿨다 (사용자 지시 2026-09-08 「없음 제거를 일괄로 하지말고 각 항목을 한번 더 체크」)."""
    if must is True: return True
    if not must: return False
    return slot in must
STOP_RATIO = 10   # n ≥ STOP_RATIO·m 이면 더 넓은 단계를 안 센다 (사전 몫 < 9%). 단계마다 bincount 하나라 속도용

# ---- 조건 = 이웃 검색, 급 사다리 (설계문서 「설계_굴리기_유사도검색_2026-09-07.md」 §8, 관점별 검증 반영) ----
# 옛 방식은 「지금까지 고른 태그가 전부 있는 그림」(완전 일치)을 조건으로 썼고, 조건이 넷을 넘으면 그림이 수십 장 → 0장으로 무너져
# 오래된 조건부터 풀어야 했다(levels_of). 이제 조건은 **질의 점수 급**이다: 질의 항목(태그, 또는 믿을 만한 슬롯의 「없음」)마다 정수 idf 를 두고
# 그림 점수 S = Σ idf·[항목 있음]. 점수가 같은 그림이 한 급이고(완전 일치 = 최상급), 급을 위에서부터 누적한 집합을 사다리의 단계로 삼아
# 기존 Dirichlet 평활(m=300, measure_m 실측)로 잇는다. 고정 top-k 를 쓰지 않는 이유: 점수 값이 최대 2^|Q| 개라 k 컷은 거의 늘 동점 덩어리 안에
# 떨어져 「완전 일치 중 무작위 k장」이 되고 재현성이 깨진다 (검증 2026-09-07, 정보검색·통계·소프트웨어 세 관점 일치).
# 급 안에서는 인물당 상한(max(6, 2%))을 가중치로 건다 — 표본 상한(캐릭터당 100장)은 이웃 안에서 무효라(상위 이웃에 한 인물 100장이 통째로 듦) 다시 걸어야 한다.
# 점수는 int32 라 합산 순서와 무관하게 동점이 정확히 동점이다(재현성).
NEIGH_CAP_FRAC = 0.02; NEIGH_CAP_MIN = 6   # 급 안 인물당 상한 = max(6, 급 크기의 2%)
NONE_QUERY = {"feet", "onepiece", "upper", "lower", "outer", "body"}   # 「없음」을 질의에 넣는 슬롯 — 부재가 믿을 만한 곳(신발은 부재 태그, 옷은 배타 의미) + 가슴.
# ★가슴을 넣은 이유(2026-09-08 실측): 가슴 태그는 몸이 드러난 그림에만 달려(가슴 있음 → 수영복 24%, 없음 → 4%), 뽑혔을 때만 조건이 걸리고 「없음」이면 조건 없이
#  옷을 고르니 수영복이 표본 9.1% 대비 13.8% 로 부풀었다. 「가슴 없음」을 질의에 넣자 9.2% (lift 문턱 해제와 함께). 뒤 슬롯 「없음」 연쇄는 없었다(옷 전부 없음 5.5% → 5.2%).
# 가슴·다리·손·허리·눈색의 「없음」은 태거 결측이 지배하므로 질의에 넣지 않는다(넣으면 이웃이 태그 적은 그림으로 쏠려 뒤 슬롯이 연쇄로 빈다 — 통계·정보검색 검토).

def q_items(tags, none_slots=()):
    """질의 항목 목록: ('tag', 태그) 와 ('none', 슬롯). 순서는 결과에 무관(점수 합)."""
    return [("tag", t) for t in tags if t] + [("none", s) for s in none_slots]
def levels_of(pre, picked):
    """호환용 — 옛 호출부(measure_m.py 등)가 (pre, picked) 를 넘기면 질의 항목으로 바꾼다."""
    return q_items(list(pre) + list(picked))
def _any_mask(sub, tids):
    """표본 안에서 tids 중 하나라도 있는 그림 마스크. 표본에 캐시. 풀이 크면(수백 태그) isin 한 번."""
    key = frozenset(int(t) for t in tids)
    if key not in sub.anym:
        m = np.zeros(sub.n, bool)
        if len(key) > 20: m[sub.rows[np.isin(sub.stid, np.fromiter(key, dtype=sub.stid.dtype, count=len(key)))]] = True
        else:
            for t in key: m |= sub.mask(NAME[t])
        sub.anym[key] = m; _cap(sub.anym, 2000)
    return sub.anym[key]
def _item_mask(sub, item):
    kind, key = item
    return sub.mask(key) if kind == "tag" else ~_slot_mask(sub, key)
def _idf_q(sub, item):
    """정수 idf(×1000), 표본 안 빈도 기준. 앵커 태그는 표본 전부에 있어 0 — 질의에 넣어도 무효다."""
    c = int(_item_mask(sub, item).sum())
    return int(round(1000.0 * math.log(sub.n / max(c, 0.5)))) if sub.n else 0
def query_tiers(sub, items):
    """점수 급: 점수 내림차순으로 그림을 정렬해 두고, 급 j = 「그 정렬의 접두사(점수 ≥ j번째로 큰 값)」. (order, bounds, vals) 를 표본에 캐시한다.
    접두사라서 급을 내려갈 때 새로 드는 그림(delta)만 더하면 되고(증분 집계), 마스크를 급마다 만들지 않는다 (2026-09-07 속도: 급이 수백 개면 마스크 방식은 회당 0.7~3초)."""
    key = ("tiers", tuple(items))
    if key not in sub.tcache:
        if not items or sub.n == 0:
            order = np.arange(sub.n); bounds = np.array([sub.n], dtype=np.int64); vals = np.array([0], dtype=np.int32)
        else:
            S = np.zeros(sub.n, np.int32)
            for it in items:
                w = _idf_q(sub, it)
                if w > 0: S += w * _item_mask(sub, it).astype(np.int32)
            order = np.argsort(-S, kind="stable"); Ss = S[order]
            change = np.nonzero(Ss[1:] != Ss[:-1])[0] + 1
            bounds = np.append(change, sub.n).astype(np.int64); vals = Ss[np.r_[0, change]]
        sub.tcache[key] = (order, bounds, vals, {}); _cap(sub.tcache, 60)   # 상태에 인물 카운트 배열(int32, 최대 캐릭터 id 길이)이 들어 캐시를 작게
    return sub.tcache[key]
def n_levels(sub, items): return len(query_tiers(sub, items)[1])
def level_plan(sub, items, m):
    """사다리가 밟을 급 번호들: 접두사 크기가 1·m/10·m/3·m·3m·10m 장을 처음 넘는 급 (기하 간격, 최대 6단계).
    급이 점수 값마다 하나라 수십~수백 개인데 전부 밟으면 급마다 집계를 다시 해 회당 0.4~3초였다(2026-09-07 프로파일: ladder_any 가 80급 × 166회).
    작은 급들을 건너뛰어도 평활은 같은 식이고, 건너뛴 급의 그림은 다음 단계에 포함된다."""
    order, bounds, vals, st = query_tiers(sub, items)
    plan = []
    for t in (1, m // 10, m // 3, m, 3 * m, STOP_RATIO * m):
        j = min(int(np.searchsorted(bounds, t, side="left")), len(bounds) - 1)
        if not plan or j > plan[-1]: plan.append(j)
    return plan
def tier_counts(sub, items, j):
    """급 j(접두사)의 (가중 장수 n, 태그별 가중 장수 벡터, 그림별 가중치 벡터(길이 sub.n)).
    증분: 급 j−1 의 원시 집계에 delta 그림의 태그만 더한다. 인물당 상한(max(6, 2%))은 상한을 넘는 인물만 골라 그 인물 그림의 집계를 (상한/장수) 배로 깎는 보정으로 건다."""
    order, bounds, vals, st = query_tiers(sub, items)
    if j in st: return st[j]
    # 원시 누적 (증분)
    if "raw" not in st: st["raw"] = (-1, 0, np.zeros(TID_CUT + 1), np.zeros(int(sub.char.max()) + 1 if len(sub.char) else 1, np.int32))
    jd, n_raw, cnt_raw, ccount = st["raw"]
    while jd < j:
        jd += 1; lo = int(bounds[jd - 1]) if jd > 0 else 0; hi = int(bounds[jd])
        idx = order[lo:hi]
        if len(idx):
            lens = sub.offs[idx + 1] - sub.offs[idx]; starts = sub.offs[idx]
            gather = np.repeat(starts - np.cumsum(lens) + lens, lens) + np.arange(int(lens.sum()), dtype=np.int64)
            cnt_raw = cnt_raw + np.bincount(sub.stid[gather], minlength=TID_CUT + 1)
            ch = sub.char[idx]; ch = ch[ch > 0]
            if len(ch): np.add.at(ccount, ch, 1)
            n_raw += len(idx)
        st["raw"] = (jd, n_raw, cnt_raw, ccount)
        if jd == j: break
    jd, n_raw, cnt_raw, ccount = st["raw"] if st["raw"][0] == j else _replay(sub, items, j)
    pref = order[:int(bounds[j])]
    cap = max(NEIGH_CAP_MIN, NEIGH_CAP_FRAC * len(pref))
    over = np.nonzero(ccount > cap)[0]
    wf = np.zeros(sub.n); wf[pref] = 1.0
    cnt = cnt_raw.astype(np.float64); n = float(n_raw)
    if len(over):
        chp = sub.char[pref]
        for c in over:
            f = cap / ccount[c]; rows = pref[chp == c]
            lens = sub.offs[rows + 1] - sub.offs[rows]; starts = sub.offs[rows]
            gather = np.repeat(starts - np.cumsum(lens) + lens, lens) + np.arange(int(lens.sum()), dtype=np.int64)
            cnt -= (1.0 - f) * np.bincount(sub.stid[gather], minlength=TID_CUT + 1)
            n -= (1.0 - f) * len(rows); wf[rows] = f
    st[j] = (n, cnt, wf)
    return st[j]
def _replay(sub, items, j):
    """급을 거꾸로 요청했을 때(캐시가 j 보다 앞서 있음) 원시 누적을 처음부터 j 까지 다시 만든다. 드물다(사다리는 위에서 아래로만 내려간다)."""
    order, bounds, vals, st = query_tiers(sub, items)
    st["raw"] = (-1, 0, np.zeros(TID_CUT + 1), np.zeros(int(sub.char.max()) + 1 if len(sub.char) else 1, np.int32))
    tier_counts(sub, items, j); return st["raw"]
def counts_vec(sub, cond):
    """호환용: cond(태그 목록)가 전부 있는 그림 수와 태그별 장수. 완전 일치(최상급)만 센다 — 앵커 lift·랜덤 앵커 등 조건 없는 집계에 쓴다."""
    if not cond:
        if "all" not in sub.cntv: sub.cntv["all"] = (sub.n, np.bincount(sub.stid, minlength=TID_CUT + 1))
        return sub.cntv["all"]
    n, cnt, _ = tier_counts(sub, q_items(cond), 0)
    return int(round(n)), cnt

def ladder(sub, items, pool, m, base=None):
    """pool(tid 배열)의 태그별 평활 확률. 반환 (n_top, k_top, p). 급을 위에서부터 n ≥ STOP_RATIO·m 까지 세고, base 는 가장 넓은 급 아래의 사전 벡터."""
    stats = []
    for j in level_plan(sub, items, m):
        n, cnt, _ = tier_counts(sub, items, j); stats.append((n, cnt[pool]))
        if n >= STOP_RATIO * m: break
    n, k = stats[-1]
    if base is not None: p = (k + m * base) / (n + m)
    else: p = k / n if n else np.zeros(len(pool))
    for n, k in reversed(stats[:-1]): p = (k + m * p) / (n + m)
    return stats[0][0], stats[0][1], p

# ---- 「없음」의 결측 처리 (사용자 결정 2026-09-05 「3안」, 이웃 방식에서도 유지 — 이웃은 겹침 많은 태그 부자 그림으로 쏠려 「없음」이 낮게 나오므로 ② 없이는 더 편향된다) ----
#  ① 부재 태그가 있는 부위(신발 barefoot/no_shoes)는 「부재 태그 ÷ (부재 태그 + 후보 중 하나라도 있음)」 — 상태를 알 수 있는 그림만으로.
#  ② 나머지는 태그가 WELL_TAGS 개 이상 달린 그림만으로 「후보 없음」 비율을 잰다.
WELL_TAGS = 30
ABSENCE = {"feet": ["barefoot", "no_shoes"]}
def ladder_any(sub, items, tids, m, base=None, well=False, abs_tids=None):
    """tids 중 하나라도 있을 확률의 평활 (「없음」 = 1 − 이것). 급 사다리·인물 가중치는 ladder 와 같다."""
    anym = _any_mask(sub, tids); absm = _any_mask(sub, abs_tids) if abs_tids else None
    stats = []
    for j in level_plan(sub, items, m):
        n0, _, wf = tier_counts(sub, items, j)
        if abs_tids: a = float(wf[anym].sum()); n = a + float(wf[absm].sum())
        elif well: n = float(wf[sub.well].sum()); a = float(wf[sub.well & anym].sum())
        else: n = n0; a = float(wf[anym].sum())
        stats.append((n, a))
        if n >= STOP_RATIO * m: break
    n, a = stats[-1]
    if base is not None: pa = (a + m * base) / (n + m)
    else: pa = a / n if n else 0.0
    for n, a in reversed(stats[:-1]): pa = (a + m * pa) / (n + m)
    return pa

def anchor_lift(anc_sub, pool, base, N, m):
    """앵커 표본에서의 lift 벡터 = P̂(B|앵커) / P(B), [0.25, 4] 로 자른다. 앵커 표본 자체도 전체 빈도 쪽으로 평활."""
    n, cnt = counts_vec(anc_sub, [])
    pb = np.array([base.get(int(t), 0) / N for t in pool]); pb = np.where(pb > 0, pb, 1e-9)
    pa = (cnt[pool] + m * pb) / (n + m)
    return np.clip(pa / pb, 0.25, 4.0)

def prior_for(prior_sub, items, pool, m, lift=None):
    """넓은 표본에 같은 질의를 던진 사다리로 사전 벡터. lift 를 주면(앵커 표본 lift) 곱한다."""
    if prior_sub is None: return None
    p = ladder(prior_sub, items, pool, m)[2]
    if lift is not None: p = np.minimum(1.0, p * lift)
    return p

def slot_cands(sub, items, pool, base, N, m, exclude, prior=None, top=12, lift_min=0.7, beta=None):
    """부위 후보 (tag, conf, lift, k) 상위 top 개. conf = 평활 확률. lift 문턱(디자인만)은 평활 확률로 계산하니 작은 표본에서 튀지 않는다.
    beta 를 주면 문턱 대신 conf × lift^β 순으로 고른다."""
    if not len(pool): return 0, []
    n, k, p = ladder(sub, items, pool, m, prior)
    cands = []
    for i, tid in enumerate(pool):
        t = NAME[int(tid)]
        if t in exclude or p[i] <= 0: continue
        pb = base.get(int(tid), 0) / N; lift = p[i] / pb if pb else 0
        if lift_min and lift < lift_min: continue
        cands.append((t, float(p[i]), float(lift), int(round(k[i]))))
    cands.sort(key=lambda c: -(c[1] * (c[2] ** beta if beta else 1.0)))
    return int(round(n)), cands[:top]

def none_prob(sub, items, cands, m, prior_sub=None, slot=None, pool_tids=None):
    """「없음」 확률 = 1 − P(후보 중 하나라도 있음), 같은 사다리로 평활. 바닥 5%. slot 에 부재 태그가 있으면 ①, 없으면 ②(태그 넉넉한 그림).
    pool_tids 를 주면 살아남은 후보가 아니라 그 부위 태그 풀 전체 기준(디자인 모듈) — 후보만으로 세면 문턱에서 빠진 태그가 통째로 「없음」이 된다."""
    tids = [int(t) for t in pool_tids] if pool_tids is not None and len(pool_tids) else [TID[c[0]] for c in cands]
    if not tids: return 1.0
    abs_tids = [TID[t] for t in ABSENCE.get(slot, []) if t in TID] or None
    well = abs_tids is None
    base = ladder_any(prior_sub, items, tids, m, well=well, abs_tids=abs_tids) if prior_sub is not None else None
    return max(0.05, 1 - ladder_any(sub, items, tids, m, base, well=well, abs_tids=abs_tids))

def pair_p(sub, first, rest_tids, m, prior_sub=None):
    """첫 태그가 있는 그림 중 rest_tids 도 있는 비율 (태그 넉넉한 그림 기준, 넓은 표본의 같은 조건부로 평활). 배경 유형의 둘째 태그에 쓴다.
    ★급 사다리를 밟지 않는다 (2026-09-08 실측): 사다리는 최상급이 작으면 넓은 급을 섞는데, 여기서 넓은 급은 「그 부류 태그가 아무거나 있음」(주변 빈도)이라
    조건부보다 3배 높다 — P(다른 배경 태그|white) 실제 4~6% 인데 사다리로는 11~22% 가 나왔다. 같은 조건부를 넓은 표본에서 재 사전으로만 쓴다."""
    def ratio(s):
        fm = s.mask(first) & s.well
        n = float(fm.sum())
        return (float((fm & _any_mask(s, rest_tids)).sum()), n) if n else None
    base = None
    if prior_sub is not None:
        r = ratio(prior_sub)
        if r: base = r[0] / r[1]
    r = ratio(sub)
    if r is None: return base if base is not None else 0.0
    a, n = r
    return (a + m * base) / (n + m) if base is not None else a / n

def any_n(sub, cond, tids, well=False):
    """호환용: cond(완전 일치)가 전부 있는 그림 중 tids 가운데 하나라도 있는 그림 수."""
    m = _any_mask(sub, tids)
    if cond: m = m & sub.cur(cond)
    if well: m = m & sub.well
    return int(m.sum())
def well_n(sub, cond):
    """호환용: cond 가 전부 있는 그림 중 태그가 넉넉한 그림 수."""
    return int((sub.well & sub.cur(cond)).sum()) if cond else int(sub.well.sum())

def wide_sample(seg, rating, seed, cap=0, cos=()):
    """사전용 넓은 표본: 장르 전체(seg=all 이면 전체 1girl 그림), 남성 없음, 단일 인물, 등급 필터, 5만 장 상한.
    cos 를 주면 복식으로도 좁힌다 (OR). cap = 캐릭터당 상한(sub_table 과 같은 값을 쓴다)."""
    bit = seg_bits(segs_of(seg))
    cos = tuple(c for c in cos if c in COSTUME_BIT)
    def pids():
        gi = np.nonzero((SEGMENT & bit) != 0)[0] if bit else np.arange(len(UPID))
        keep = ~boy_mask()[gi] & solo_mask()[gi] & cos_mask(gi, cos)
        if rating == "gs": keep &= RATING[gi] <= 1
        return np.asarray(UPID[gi[keep]], dtype=np.int32)
    return _sample(("wide", segs_of(seg), rating, cos), pids, seed, cap)[0]

def e_sample(seed):
    """사전용 e 등급 전체 표본 (NSFW·체위 씬)."""
    def pids(): return np.asarray(UPID[np.asarray(RATING) == 3], dtype=np.int32)
    return _sample(("wide_e",), pids, seed)[0]

def small(sub): return sub.n < STOP_RATIO * M_PRIOR

# 착의 모드 (사용자 지시 2026-09-08 「누드/완전누드로 나눠서 누드는 몸통만, 완전누드는 전부」).
# 누드 = 몸에 입는 옷만 벗는다 — 스타킹·장갑·머리 장식·목걸이·신발은 남고 통계대로 굴린다.
#   실측 근거: e 등급 표본의 completely_nude 9,403장에서 머리 장식 36.7% · 신발 15.0% · 목 8.5% · 다리 3.1% · 손 2.6% 가 살아 있다.
#   전에는 「완전 누드」 하나뿐이었고 착용 슬롯 13개를 통째로 꺼서 머리 장식이 절대 안 나왔다 (NUDE_SKIP 상수는 있었지만 쓰이지 않는 죽은 코드였다).
NUDE_OFF = {"onepiece", "upper", "outer", "lower", "waist", "armor"}   # 누드: 몸에 입는 옷
BARE_OFF = set(WEARABLE)                                              # 완전 누드: 착용 슬롯 전부
# ---- 디자인 굴리기: 슬롯 순서대로, 조건은 급 사다리 (설계문서 §8, 2026-09-07) ----
# 슬롯 21개를 순서대로 돌며 「앵커 + 지금까지 고른 태그 + 믿을 만한 슬롯의 「없음」」을 질의로 급 사다리(ladder)에서 그 슬롯의 분포를 얻는다.
# 「블록마다 그림 한 장 통째로」(2026-09-07 밤 잠깐 시도)는 걷어냈다 — 정합은 서지만 블록이 그림 한 장의 복제라 목표(요소 단위 의미 매칭)에서 벗어났다.
TID_SLOT = {}   # tid → 슬롯 (SLOTS 순서 우선 — slot_of 와 같은 규칙)
for _slot, _, _ in SLOTS:
    for _t in SLOT_POOL[_slot]: TID_SLOT.setdefault(int(_t), _slot)
SLOT_TIDS = {slot: np.array(sorted(t for t, s in TID_SLOT.items() if s == slot), dtype=np.int64) for slot, _, _ in SLOTS}
def _slot_mask(sub, slot):
    """표본 안에서 「그 슬롯 태그가 하나라도 있는 그림」 마스크. 표본에 캐시."""
    key = ("slotmask", slot)
    if key not in sub.anym:
        m = np.zeros(sub.n, bool); m[sub.rows[np.isin(sub.stid, SLOT_TIDS[slot].astype(sub.stid.dtype))]] = True; sub.anym[key] = m
    return sub.anym[key]

def roll(anchor, temp, rating, fixed, seed, mode="dressed", seg="all", on=None, kemono=True, beta=None, exclude=(), charcap=0, must=False, cos=()):
    """mode: dressed(전부 입기) · nude(완전 누드) · select(선택만 입기). on: 켜진 슬롯 집합(None 이면 기본).
    bare = 알몸 기준으로 뽑는가 — nude 모드, 또는 select 모드에서 원피스·상의·하의가 전부 꺼져 있을 때. 이때 나체 그림(nude)을 조건으로 쓰고 nude/completely_nude 태그를 붙인다.
    fixed: slot → tag(칩 고정) 또는 ""(「없음」 고정). must: 켜진 슬롯 전부 「없음」 없이(사용자 지시 2026-09-07). 기본은 통계대로(없음 포함)."""
    anchors = [anchor] if isinstance(anchor, str) else list(anchor)
    on = set(on) if on is not None else {sl for sl, _, _ in SLOTS} - DEFAULT_OFF
    if mode == "nude": on -= NUDE_OFF          # 누드: 몸에 입는 옷만
    elif mode == "bare": on -= BARE_OFF        # 완전 누드: 착용 전부
    bare = mode in ("nude", "bare") or not (on & {"onepiece", "upper", "lower"})
    nude = bare
    excl0 = set(anchors) | (set() if kemono else KEMONO) | set(exclude)   # 수인 끄면 후보에서 뺀다. exclude = 사용자가 적은 제외 태그 (속옷·탈의 태그는 in_slot 에서 이미 빠진다)
    nude_tag = "completely_nude" if mode == "bare" else ("nude" if bare else "")
    base, N = base_for(rating)
    cos = tuple(c for c in cos if c in COSTUME_BIT)
    sub, n_anchor = sub_table(tuple(anchors), rating, seed, seg, charcap, cos)
    prior_sub = wide_sample(seg, rating, seed, charcap, cos) if small(sub) else None            # 표본이 작으면 장르·복식 전체를 사전으로
    anc_sub = sub_table(tuple(anchors), rating, seed, (), charcap, cos)[0] if (prior_sub is not None and segs_of(seg)) else None   # 장르가 걸려 있으면 앵커 lift 도 곱한다
    chosen = dict(fixed)                       # slot → tag (사용자가 고정한 것)
    by_slot = {}; extra = []
    for a in anchors:
        sl = slot_of(a)
        if sl: by_slot.setdefault(sl, []).append(a)
        else: extra.append(a)
    out = []; picked = []; none_slots = []     # picked: 질의에 쓰는 태그(앵커 제외, 고른 순서). none_slots: 「없음」으로 결정된 믿을 만한 슬롯
    # ★원피스 배타 규칙을 걷어냈다 (사용자 지시 2026-09-08 「통계에 맡겨」). 전에는 원피스를 뽑으면 상의·하의를 통째로 건너뛰었는데,
    # 실측은 그렇지 않다 — 개 귀 표본(태그 30개 이상 5,547장)에서 원피스 그림의 30.0% 가 상의를, 31.1% 가 하의를 함께 단다
    # (원피스 위 셔츠·재킷, 드레스 아래 반바지). 그 30% 가 통째로 사라져 상의가 25.5%(실제 50.6%)·하의 32.0%(53.2%)로 나왔다.
    # 이제는 원피스가 질의에 들어간 채로 상의·하의를 굴린다 — 조건부 확률이 알아서 낮춘다.
    if extra: out.append({"slot": "anchor_extra", "label": "추가 태그", "chosen": ", ".join(extra), "anchor": True, "cands": [], "n": n_anchor})
    pre = ["nude"] if nude and "nude" in TID else []     # 누드면 나체 그림에서 뽑는다 (사이하이·장갑이 남는 비율이 다르다)
    for si, (slot, label, required) in enumerate(SLOTS):
        required = is_req(must, slot)
        if slot in by_slot:
            out.append({"slot": slot, "label": label, "chosen": ", ".join(by_slot[slot]), "anchor": True, "cands": [], "n": n_anchor}); continue
        if slot not in on:
            out.append({"slot": slot, "label": label, "chosen": None, "skipped": True, "why": "꺼짐", "cands": [], "n": 0}); continue
        qitems = q_items(pre + picked, none_slots); cond = pre + picked + ["!" + s for s in none_slots]; pool = SLOT_POOL_ARR[slot]
        lift = anchor_lift(anc_sub, pool, base, N, M_PRIOR) if anc_sub is not None else None
        prior = prior_for(prior_sub, qitems, pool, M_PRIOR, lift)
        # ★lift 문턱(0.7)은 뺐다(2026-09-08 실측): 급 사다리가 이미 앵커 표본 안에서 확률을 재므로 문턱은 「실제 비율대로」와 어긋난다 — 개 귀 앵커에서 dress(13%, lift 0.69)가
        #  통째로 빠져 그 몫이 swimsuit·school_uniform 으로 갔다. 앵커 특유 조합은 「앵커 특화」(β) 옵션이 따로 맡는다.
        n, cands = slot_cands(sub, qitems, pool, base, N, M_PRIOR, excl0 | set(picked), prior, lift_min=0, beta=beta)
        if not cands and required:
            out.append({"slot": slot, "label": label, "chosen": None, "cands": [], "n": n, "cond": cond}); continue
        opts = [c[0] for c in cands]
        if not required:
            none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub, slot, pool_tids=pool)
            wt = [c[1] * (c[2] ** beta if beta else 1.0) for c in cands]
            scale = (1 - none_p) / max(sum(wt), 1e-9)   # 후보 가중치 합 = P(하나라도 있음)
            weights = [(w * scale) ** (1 / temp) for w in wt]; opts.append(None); weights.append(none_p ** (1 / temp))
        else:
            none_p = None
            weights = [(c[1] * (c[2] ** beta if beta else 1.0)) ** (1 / temp) for c in cands]
        fx = chosen.get(slot)
        if fx is not None and fx != "" and fx in TID: pick = fx      # 고정 태그는 후보 12개 밖이어도 존중
        elif fx == "": pick = None
        elif opts:
            g = np.random.default_rng([seed & 0x7fffffff, si])      # 슬롯마다 난수를 갈라 칩 고정이 뒤 슬롯의 난수열을 밀지 않게 (검증 2026-09-07)
            w = np.array(weights, dtype=float); pick = opts[int(g.choice(len(opts), p=w / w.sum()))] if w.sum() > 0 else None
        else: pick = None
        if pick: picked.append(pick)
        elif slot in NONE_QUERY: none_slots.append(slot)
        out.append({"slot": slot, "label": label, "chosen": pick, "n": n, "cond": cond,
                    "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lf, 2), "k": k} for t, conf, lf, k in cands],
                    "none_p": (round(none_p, 3) if none_p is not None else None)})
    tags = anchors + picked + ([nude_tag] if nude_tag else [])
    return {"anchor": ",".join(anchors), "n_anchor": n_anchor, "n_raw": getattr(sub, "n_raw", n_anchor), "charcap": charcap, "must": (True if must is True else sorted(must or ())), "seed": seed, "temp": temp, "rating": rating, "mode": mode, "bare": bare, "nude_tag": nude_tag, "on": sorted(on), "seg": list(segs_of(seg)), "cos": list(cos), "seg_n": SEG_INFO.get(("design", tuple(anchors), rating, segs_of(seg), tuple(cos)), (None, False))[0], "seg_fallback": SEG_INFO.get(("design", tuple(anchors), rating, segs_of(seg), tuple(cos)), (None, False))[1], "slots": out, "tags": tags,
            "prompt": ", ".join(t.replace("_", " ") for t in tags)}

RANDOM_POOL = None
def random_anchor(rng, rating, seg="all", kemono=True):
    """빈 칸으로 굴렸을 때 쓸 앵커. 디자인 부위에 드는 태그 중 그림 3,000장 이상인 것에서 빈도 비례로 뽑는다 (사용자 지시 2026-09-04).
    장르를 골랐으면 그 장르 안에 SEG_MIN 장 이상 있는 앵커가 나올 때까지 다시 뽑는다 (최대 12회) — 작은 장르(경찰 9,611장)에서 아무 앵커나 고르면 거의 매번 전 장르로 되돌아간다."""
    global RANDOM_POOL
    if RANDOM_POOL is None:
        base, _ = base_for("all")
        RANDOM_POOL = [(NAME[tid], base.get(tid, 0)) for tid in sorted({t for pool in SLOT_POOL.values() for t in pool}) if base.get(tid, 0) >= 3000 and NAME[tid] != "1girl"]
    bit = seg_bits(segs_of(seg)); pick = None
    for _ in range(12):
        pick = rng.choices([t for t, _ in RANDOM_POOL], weights=[n ** 0.5 for _, n in RANDOM_POOL])[0]   # 제곱근 가중: 흔한 태그로만 쏠리지 않게
        if not kemono and pick in KEMONO: continue
        if not bit: break
        idx = np.searchsorted(UPID, posting(pick))
        if int((~boy_mask()[idx] & ((SEGMENT[idx] & bit) != 0)).sum()) >= SEG_MIN: break   # sub_table 과 같은 조건(남성 제외)으로 세야 문턱이 맞는다
    return pick

def act_weights():
    base, N = base_for("e")
    return [(a, base.get(TID[a], 0)) for a in ACTS if a in TID and base.get(TID[a], 0) > 0]

def roll_nsfw(act, temp, fixed, seed, body, design, level=2, nude=False, nude_tag="", exclude=(), must=False, on=None):
    """act: 행위 태그(없으면 e 빈도로 뽑음). body: 디자인의 가슴 태그(조건). design: [{slot, tag}] 디자인 결과 — 옷 상태에 따라 걷어 낸다."""
    rng = random.Random(seed)
    base, N = base_for("e")
    acts = act_weights(); wsum = sum(w for _, w in acts)
    if not act:
        act = rng.choices([a for a, _ in acts], weights=[w ** (1 / temp) for _, w in acts])[0]
    sub, n_act = nsfw_sub(act, seed)
    prior_sub = e_sample(seed) if small(sub) else None    # 드문 체위면 e 등급 전체를 사전으로
    pre = [d["tag"] for d in design if d.get("tag") in TID]   # 디자인 태그 전부를 질의에 앞세운다 (사용자 지시 2026-09-08 「모든 모듈이 캐릭터를 기준으로」). 가슴 크기 훅이 이 안에 든다
    if body and body in TID and body not in pre: pre.append(body)
    out = [{"slot": "act", "label": "체위·행위", "chosen": act, "anchor": True, "n": n_act, "none_p": None,
            "cands": [{"tag": a, "conf": round(w / wsum, 4), "lift": 1.0, "k": w} for a, w in sorted(acts, key=lambda x: -x[1])]}]
    picked = []
    for key, label, pool in NSLOTS:
        if on is not None and key not in on:
            out.append({"slot": key, "label": label, "chosen": [], "k": 0, "skipped": True, "why": "꺼짐", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        if nude and key == "clothes":
            out.append({"slot": key, "label": label, "chosen": [], "k": 0, "skipped": True, "why": "누드", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        poolset = {t for t in pool if t in TID and t != act and t not in exclude}
        qitems = levels_of(pre, picked); cond = list(pre) + list(picked)
        pool = np.array(sorted(TID[t] for t in poolset if TID[t] <= TID_CUT), dtype=np.int64)
        prior = prior_for(prior_sub, qitems, pool, M_PRIOR)
        n, cands = slot_cands(sub, qitems, pool, base, N, M_PRIOR, set(picked), prior, lift_min=0)
        items = [c[0] for c in cands]
        k = LEVEL_K.get(level, LEVEL_K[2]).get(key, 1)
        # ★강제 뽑기를 뺐다 (사용자 지시 2026-09-08 「nsfw도 통계대로」). 전에는 삽입류 체위면 삽입 슬롯을,
        # 강도 2 이상이면 중첩 슬롯을 반드시 뽑았다. 이제 「없음」 제거 옵션(must)으로만 강제한다.
        required = is_req(must, key) and cands
        none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub) if cands else 1.0
        scale = 1.0 if required else (1 - none_p) / max(sum(c[1] for c in cands), 1e-9)
        weights = [(c[1] * scale) ** (1 / temp) for c in cands]
        fx = fixed.get(key)
        if k == 0:
            picks = []
        elif fx is not None:
            fx = [fx] if isinstance(fx, str) else list(fx)
            picks = [t for t in fx if t in items]
        else:
            picks = []
            for i in range(k):
                pool_i = [(t, w) for t, w in zip(items, weights) if t not in picks]
                if not pool_i: break
                ts = [t for t, _ in pool_i]; ws = [w for _, w in pool_i]
                if i == 0 and not required: ts.append(None); ws.append(none_p ** (1 / temp))
                pick = rng.choices(ts, weights=ws)[0]
                if pick is None: break
                picks.append(pick)
        picked.extend(picks)
        out.append({"slot": key, "label": label, "chosen": picks, "k": k, "n": n, "cond": cond, "none_p": (None if required or k == 0 else round(none_p, 3)),
                    "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": k} for t, conf, lift, k in cands]})
    strip = set()
    for t in picked: strip |= STRIP.get(t, set())
    kept = [d["tag"] for d in design if d.get("tag") and d.get("slot") not in strip]
    if nude and nude_tag: kept.append(nude_tag)   # 디자인 묶음의 nude/completely_nude 는 슬롯이 아니라 design 목록에 안 실려 온다
    tags = kept + [act] + picked
    return {"act": act, "n_act": n_act, "seed": seed, "temp": temp, "level": level, "body": body, "slots": out, "nsfw_tags": [act] + picked,
            "stripped": sorted(strip), "prompt": ", ".join(t.replace("_", " ") for t in tags)}

UMBRELLA = ["indoors", "outdoors"]   # 포괄 태그. 구체 장소 후보가 없을 때만 쓴다 — 늘 가장 흔해서 넣어 두면 매번 indoors 로 수렴한다 (2026-09-04 실측)
# 씬 태그는 드물어(장소 6%·조명 2%) 표본이 작으면 후보가 빈다 — 옛 문턱 800 대신 평활(M_PRIOR)이 넓은 표본으로 메운다.
def roll_scene(anchor, rating, act, temp, fixed, seed, bg="auto", seg="all", frame="auto", exclude=(), charcap=0, design=(), no_white=False, must=False, on=None):
    """act 가 있으면 그 체위의 e 등급 표본, 없으면 디자인 앵커의 표본에서 씬 슬롯을 순서대로 뽑는다. design = 디자인 모듈이 고른 태그 — 모든 슬롯의 질의에 앞세운다
    (사용자 지시 2026-09-08 「모든 모듈이 캐릭터를 기준으로」: 수영복 → ocean·beach 83%, 교복 → desk·classroom 43%, brown_hair 현대 표본).
    bg: 'auto'(통계대로) = 배경 유형 슬롯의 「없음」 확률로 단순/실경 갈래를 정한다 · 'simple' = 배경 유형을 반드시 뽑는다 · 'real' = 배경 유형을 건너뛴다.
    no_white(「흰·회색 배경 제외」, 사용자 지시 2026-09-08): 배경 유형 후보에서 흰·투명·회색만 뺀다(유채색·그라데이션·무늬는 남는다).
      ★단순/실경 갈래는 거르기 전 구체 배경 태그 풀 전체로 정한다 — 흰 배경이 그 질량의 40%(개 귀·현대 표본)라, 거른 풀로 「없음」을 재면 단순 비율이 71% → 30% 아래로 떨어진다.
    실경이면 장소·시간·조명을 반드시 고른다 — 일러스트 전반에서는 배경 태그가 드물어 그대로 뽑으면 씬이 거의 빈다.
    frame: 'auto' = 프레이밍을 앵커와 무관한 넓은 표본(장르 전체 / 체위면 e 전체)에서 뽑는다 — 앵커 표본에서 뽑으면 사이하이 앵커가 full_body 로 쏠린다
    (사용자 결정 2026-09-05 「2안」). 'off' = 프레이밍 슬롯을 건너뛴다. 그 밖의 값 = 그 태그로 고정."""
    rng = random.Random(seed)
    design = [t for t in design if t in TID]
    if act:
        sub, n0 = nsfw_sub(act, seed); base, N = base_for("e"); src = f"{act} (e 등급)"
        prior_sub = e_sample(seed) if small(sub) else None
    else:
        anchors = tuple(anchor.split(",")) if isinstance(anchor, str) else tuple(anchor)
        # ★씬만 복식(cos)을 안 건다 — 장르는 세계관이라 배경을 정하고, 복식은 그 안 인물의 의상이라 배경과 무관하다 (사용자 지시 2026-09-08
        # 「장르가 복식보다 더 배경에 영향을 줘야함」). 그래서 배경·장소·조명은 장르만 걸린 표본에서 뽑는다.
        sub, n0 = sub_table(anchors, rating, seed, seg, charcap); base, N = base_for(rating); src = " + ".join(anchors)
        prior_sub = wide_sample(seg, rating, seed, charcap) if small(sub) else None
    out = []; picked = []
    for key, label, required, pool in SSLOTS:
        if on is not None and key not in on:
            out.append({"slot": key, "label": label, "chosen": None, "skipped": True, "why": "꺼짐", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        if key == "frame":
            if frame == "off":
                out.append({"slot": key, "label": label, "chosen": None, "skipped": True, "why": "꺼짐", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
            wsub = e_sample(seed) if act else wide_sample(seg, rating, seed, charcap)
            fpool = np.array(sorted(TID[t] for t in pool if t in TID and TID[t] <= TID_CUT and t not in exclude), dtype=np.int64)
            n, cands = slot_cands(wsub, [], fpool, base, N, M_PRIOR, set(), None, lift_min=0)
            # ★구도에도 「없음」을 둔다 (사용자 지시 2026-09-08 「반드시 뽑는다는 거 말고 통계에 맡겨야함」).
            #  전에는 required=True 라 켜져 있으면 100% 뽑았는데, 실제로는 개 귀 표본(태그 30개 이상)의 50.5% 만 구도 태그를 단다.
            #  풀 전체 기준으로 재고(pool_tids), 「없음」을 지우려면 must 를 켠다.
            fnone = none_prob(wsub, [], cands, M_PRIOR, pool_tids=fpool) if cands else 1.0
            freq = is_req(must, key) and cands
            fscale = 1.0 if freq else (1 - fnone) / max(sum(c[1] for c in cands), 1e-9)
            items = [c[0] for c in cands]; weights = [(c[1] * fscale) ** (1 / temp) for c in cands]
            if not freq: items.append(None); weights.append(fnone ** (1 / temp))
            fx = fixed.get(key)
            if frame != "auto" and frame in TID: pick = frame
            elif fx is not None and (fx in items or fx == ""): pick = fx or None
            else: pick = rng.choices(items, weights=weights)[0] if items else None
            if pick: picked.append(pick)
            out.append({"slot": key, "label": label, "chosen": pick, "n": n, "cond": [], "none_p": (None if freq else round(fnone, 3)), "src": "앵커 무관",
                        "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": k} for t, conf, lift, k in cands]})
            continue
        if key == "bg" and bg == "real":
            out.append({"slot": key, "label": label, "chosen": None, "skipped": True, "why": "실경", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        anchor_set = set(anchor.split(",")) if isinstance(anchor, str) else set(anchor)
        if key == "bg":
            # 구체 배경 태그를 둘까지 뽑는다 (사용자 결정 2026-09-08 「1안·상한 2개」). 포괄 태그 simple_background 는 코드가 붙이므로 상한에 안 센다 (사용자 지적).
            # ★둘째 추첨을 첫 태그로 조건 지어야 한다 (실측): 구체 태그가 있는 그림 중 2개 이상은 6.5~9.1% 로 드물지만 종류마다 갈린다 —
            #  단색끼리는 3~4% 뿐인데, 그라데이션은 69~73%·무늬는 50~67%·two-tone 은 77~100% 가 단색을 동반한다. 같은 분포에서 두 번 뽑으면 이 구조가 뭉개진다.
            poolset = {t for t in pool if t in TID and TID[t] <= TID_CUT and t not in anchor_set and t != act and t not in exclude}
            full = np.array(sorted(TID[t] for t in poolset), dtype=np.int64)   # 거르기 전 구체 태그 풀 — 「없음」(= 갈래·둘째 태그 유무)의 기준
            if no_white: poolset -= WHITE_BG
            fx = fixed.get(key); fx = ([fx] if isinstance(fx, str) else list(fx)) if fx is not None else None
            picks = []; disp = None
            for draw in range(2):
                qitems = q_items(design + picked + picks); cond = design + picked + picks
                pool_arr = np.array(sorted(TID[t] for t in poolset if t not in picks), dtype=np.int64)
                prior = prior_for(prior_sub, qitems, pool_arr, M_PRIOR)
                n, cands = slot_cands(sub, qitems, pool_arr, base, N, M_PRIOR, set(picked) | set(picks), prior, lift_min=0)
                rest = np.array([t for t in full if NAME[int(t)] not in picks], dtype=np.int64)   # 「없음」은 거르기 전 풀 기준 — 흰 배경을 빼도 갈래·동반 구조가 안 흔들린다
                # 첫 태그: 「배경 유형이 있는가」 = 단순·실경 갈래라 디자인 조건까지 넣은 급 사다리로 («없음»이 곧 실경). 둘째 태그: 「이 태그에 다른 배경 태그가 따라붙는가」라 첫 태그만의 조건부로 잰다(pair_p).
                # 「어느 태그인가」는 둘 다 디자인까지 넣은 질의(qitems)로 고른다 — 유무와 선택의 조건이 다르다.
                if not cands: none_p = 1.0
                elif draw == 0: none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub, pool_tids=rest)
                else: none_p = max(0.05, 1 - pair_p(sub, picks[0], [int(t) for t in rest], M_PRIOR, prior_sub))
                req = (bg == "simple" and draw == 0 and bool(cands))
                if disp is None: disp = (n, cond, cands, (None if req else none_p))   # 화면에는 첫 추첨의 후보·「없음」을 보인다
                if fx is not None: pick = fx[draw] if draw < len(fx) and fx[draw] in TID else None
                elif cands:
                    scale = 1.0 if req else (1 - none_p) / max(sum(c[1] for c in cands), 1e-9)
                    items = [c[0] for c in cands]; weights = [(c[1] * scale) ** (1 / temp) for c in cands]
                    if not req: items.append(None); weights.append(none_p ** (1 / temp))
                    pick = rng.choices(items, weights=weights)[0]
                else: pick = None
                if not pick: break
                picks.append(pick)
            if picks:
                if any(p in SOLID_BG for p in picks): picked.append("simple_background")   # 단색 배경은 포괄 태그와 함께 (사용자 결정 2026-09-08 「1안」)
                picked.extend(picks)
            n, cond, cands, none_p = disp if disp else (0, design + picked, [], 1.0)
            out.append({"slot": key, "label": label, "chosen": picks, "k": 2, "n": n, "cond": cond,
                        "none_p": (None if none_p is None else round(none_p, 3)),
                        "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": kk} for t, conf, lift, kk in cands]})
            continue
        poolset = {t for t in pool if t in TID and t not in anchor_set and t != act and t not in exclude}
        qitems = q_items(design + picked); cond = design + picked   # 조건 = 급 사다리 (2026-09-07), 디자인 태그를 앞세운다 (2026-09-08)
        def cands_for(ps, top=12):
            pool = np.array(sorted(TID[t] for t in ps if TID[t] <= TID_CUT), dtype=np.int64)
            prior = prior_for(prior_sub, qitems, pool, M_PRIOR)
            return pool, slot_cands(sub, qitems, pool, base, N, M_PRIOR, set(picked), prior, top=top, lift_min=0)
        pool_arr, (n, cands) = cands_for(poolset)
        if key == "place" and not cands:
            pool_arr, (n, cands) = cands_for({t for t in UMBRELLA if t in TID})
        items = [c[0] for c in cands]
        req = is_req(must, key) and cands   # 슬롯별 「반드시 뽑기」 (사용자 지시 2026-09-08)
        none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub) if cands else 1.0
        cands, none_p = pair_adjust(cands, picked, none_p, req)   # 실측 궁합 보정 (2026-09-08) — 태그·순서는 그대로라 items 는 유효하다
        scale = 1.0 if req else (1 - none_p) / max(sum(c[1] for c in cands), 1e-9)
        weights = [(c[1] * scale) ** (1 / temp) for c in cands]
        if not req: items.append(None); weights.append(none_p ** (1 / temp))
        fx = fixed.get(key)
        if fx is not None and (fx in items or fx == ""): pick = fx or None
        else: pick = rng.choices(items, weights=weights)[0] if items else None
        if pick: picked.append(pick)
        out.append({"slot": key, "label": label, "chosen": pick, "n": n, "cond": cond, "none_p": (None if req else round(none_p, 3)),
                    "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": k} for t, conf, lift, k in cands]})
    return {"src": src, "n_src": n0, "seed": seed, "temp": temp, "bg": bg, "no_white": no_white, "simple": any(sl["slot"] == "bg" and sl.get("chosen") for sl in out), "slots": out, "scene_tags": picked,
            "prompt": ", ".join(t.replace("_", " ") for t in picked)}

def roll_pose(anchor, rating, act, temp, fixed, seed, seg="all", exclude=(), charcap=0, design=(), cos=(), must=False, on=None):
    """포즈 모듈: 자세 → 팔 → 손짓 → 다리 → 몸통 → 소지. 표본·사전은 씬과 같다. exclude = 이미 고른 태그(NSFW 모듈 결과 등) — 후보에서 뺀다.
    design = 디자인 모듈이 고른 태그 — 질의에 앞세운다 (사용자 지시 2026-09-08 「모든 모듈이 캐릭터를 기준으로」)."""
    rng = random.Random(seed)
    design = [t for t in design if t in TID]
    if act:
        sub, n0 = nsfw_sub(act, seed); base, N = base_for("e"); src = f"{act} (e 등급)"
        prior_sub = e_sample(seed) if small(sub) else None
    else:
        anchors = tuple(anchor.split(",")) if isinstance(anchor, str) else tuple(anchor)
        sub, n0 = sub_table(anchors, rating, seed, seg, charcap, cos); base, N = base_for(rating); src = " + ".join(anchors)
        prior_sub = wide_sample(seg, rating, seed, charcap, cos) if small(sub) else None
    anchor_set = set(anchor.split(",")) if isinstance(anchor, str) else set(anchor)
    excl = anchor_set | set(exclude) | ({act} if act else set())
    out = []; picked = []
    for key, label, required, pool in PSLOTS:
        if on is not None and key not in on:
            out.append({"slot": key, "label": label, "chosen": None, "skipped": True, "why": "꺼짐", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        poolset = {t for t in pool if t in TID and TID[t] <= TID_CUT and t not in excl}
        qitems = q_items(design + picked); cond = design + picked   # 조건 = 급 사다리 (2026-09-07), 디자인 태그를 앞세운다 (2026-09-08)
        pool_arr = np.array(sorted(TID[t] for t in poolset), dtype=np.int64)
        prior = prior_for(prior_sub, qitems, pool_arr, M_PRIOR)
        n, cands = slot_cands(sub, qitems, pool_arr, base, N, M_PRIOR, set(picked), prior, lift_min=0)
        items = [c[0] for c in cands]
        req = is_req(must, key) and cands   # 슬롯별 「반드시 뽑기」 (사용자 지시 2026-09-08)
        none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub) if cands else 1.0
        cands, none_p = pair_adjust(cands, picked, none_p, req)   # 실측 궁합 보정 (2026-09-08) — 태그·순서는 그대로라 items 는 유효하다
        scale = 1.0 if req else (1 - none_p) / max(sum(c[1] for c in cands), 1e-9)
        weights = [(c[1] * scale) ** (1 / temp) for c in cands]
        if not req: items.append(None); weights.append(none_p ** (1 / temp))
        fx = fixed.get(key)
        if fx is not None and (fx in items or fx == ""): pick = fx or None
        else: pick = rng.choices(items, weights=weights)[0] if items else None
        if pick: picked.append(pick)
        out.append({"slot": key, "label": label, "chosen": pick, "n": n, "cond": cond, "none_p": (None if req else round(none_p, 3)),
                    "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": k} for t, conf, lift, k in cands]})
    return {"src": src, "n_src": n0, "seed": seed, "temp": temp, "slots": out, "pose_tags": picked,
            "prompt": ", ".join(t.replace("_", " ") for t in picked)}

def roll_expr(anchor, rating, act, temp, fixed, seed, seg="all", exclude=(), charcap=0, design=(), cos=(), must=False, on=None):
    """표정 모듈: 감정 → 입 → 눈 → 눈썹 → 볼·상태(둘까지) → 시선. 표본·사전은 포즈·씬과 같다. exclude = NSFW 모듈이 이미 고른 태그(ahegao 등) — 후보에서 뺀다.
    design = 디자인 모듈이 고른 태그 — 질의에 앞세운다 (사용자 지시 2026-09-08 「모든 모듈이 캐릭터를 기준으로」).
    포즈 모듈과 다른 점 둘: 후보 컷을 풀 전체로 푼다(top=len(pool) — 12개 컷이면 angry·serious·smug 가 후보에도 못 든다),
    볼·상태는 NSFW 반응 슬롯처럼 같은 분포에서 중복 없이 둘까지 뽑는다(blush + sweat). fixed 값은 문자열 하나(단일 슬롯) 또는 목록(볼·상태)."""
    rng = random.Random(seed)
    if act:
        sub, n0 = nsfw_sub(act, seed); base, N = base_for("e"); src = f"{act} (e 등급)"
        prior_sub = e_sample(seed) if small(sub) else None
    else:
        anchors = tuple(anchor.split(",")) if isinstance(anchor, str) else tuple(anchor)
        sub, n0 = sub_table(anchors, rating, seed, seg, charcap, cos); base, N = base_for(rating); src = " + ".join(anchors)
        prior_sub = wide_sample(seg, rating, seed, charcap, cos) if small(sub) else None
    anchor_set = set(anchor.split(",")) if isinstance(anchor, str) else set(anchor)
    excl = anchor_set | set(exclude) | ({act} if act else set())
    out = []; picked = []
    design = [t for t in design if t in TID]
    for key, label, required, k, pool in XSLOTS:
        if on is not None and key not in on:
            out.append({"slot": key, "label": label, "chosen": None, "skipped": True, "why": "꺼짐", "n": 0, "cond": [], "none_p": None, "cands": []}); continue
        poolset = {t for t in pool if t in TID and TID[t] <= TID_CUT and t not in excl}
        qitems = q_items(design + picked); cond = design + picked   # 조건 = 급 사다리 (2026-09-07), 디자인 태그를 앞세운다 (2026-09-08)
        pool_arr = np.array(sorted(TID[t] for t in poolset), dtype=np.int64)
        prior = prior_for(prior_sub, qitems, pool_arr, M_PRIOR)
        n, cands = slot_cands(sub, qitems, pool_arr, base, N, M_PRIOR, set(picked), prior, top=len(pool_arr), lift_min=0)
        items = [c[0] for c in cands]
        req = is_req(must, key) and cands   # 슬롯별 「반드시 뽑기」 (사용자 지시 2026-09-08)
        none_p = none_prob(sub, qitems, cands, M_PRIOR, prior_sub) if cands else 1.0
        cands, none_p = pair_adjust(cands, picked, none_p, req)   # 실측 궁합 보정 (2026-09-08) — 태그·순서는 그대로라 items 는 유효하다
        scale = 1.0 if req else (1 - none_p) / max(sum(c[1] for c in cands), 1e-9)
        weights = [(c[1] * scale) ** (1 / temp) for c in cands]
        fx = fixed.get(key)
        if fx is not None:
            fx = [fx] if isinstance(fx, str) else list(fx)
            picks = [t for t in fx if t in items]
        else:
            picks = []
            for i in range(k):
                pool_i = [(t, w) for t, w in zip(items, weights) if t not in picks]
                if not pool_i: break
                ts = [t for t, _ in pool_i]; ws = [w for _, w in pool_i]
                if not req: ts.append(None); ws.append(none_p ** (1 / temp))   # 둘째 추첨에도 「없음」을 둔다 — 첫 추첨에만 두면 0개 아니면 늘 2개가 된다 (2026-09-07 실측 40회: 1개 0회)
                pick = rng.choices(ts, weights=ws)[0]
                if pick is None: break
                picks.append(pick)
        picked.extend(picks)
        out.append({"slot": key, "label": label, "chosen": (picks if k > 1 else (picks[0] if picks else None)), "k": k, "n": n, "cond": cond,
                    "none_p": (None if req else round(none_p, 3)),
                    "cands": [{"tag": t, "conf": round(conf, 4), "lift": round(lift, 2), "k": kk} for t, conf, lift, kk in cands]})
    return {"src": src, "n_src": n0, "seed": seed, "temp": temp, "slots": out, "expr_tags": picked,
            "prompt": ", ".join(t if not re.search(r"[a-z]", t) else t.replace("_", " ") for t in picked)}   # ^_^ · >_< 는 밑줄이 이모티콘의 일부라 그대로 둔다

# ---- 창구가 넘겨 준 쿼리를 읽는 조각 (원본 roll_server.py 의 핸들러 위에 있던 것 그대로) ----
def _excl(q):
    """쿼리의 exclude= (쉼표 목록) → 태그 집합. 사전에 없는 것은 그냥 무시된다 (후보에 없으니 뺄 것도 없다)."""
    return {t.strip().lower().replace(" ", "_") for t in q.get("exclude", [""])[0].split(",") if t.strip()}
# 여러 명이 있어야 성립하는 태그 — 인원수 1 이면 표정·포즈·씬 후보에서 뺀다 (사용자 지시 2026-09-08).
# 고른 기준은 「그 태그가 달린 1girl 그림 중 solo 도 달린 비율」(색인 636만 장 평균 74.1%·중앙값 75.6%):
# pov_crotch 0.4% · eye_contact 1.0% · straddling 2.9% · looking_at_another 4.4% · pov 9.0%. 여기서 뚝 끊기고 다음이 torogao 15%·ahegao 19%(NSFW 상황 태그라 성질이 다름).
MULTI_ONLY = {"pov_crotch", "eye_contact", "straddling", "looking_at_another", "pov"}
def _people(q):
    """쿼리의 people= → 인원수(1~3). 1 이면 MULTI_ONLY 를 후보에서 뺀다."""
    try: return max(1, min(3, int(q.get("people", ["1"])[0] or 1)))
    except ValueError: return 1

def _on(q):
    """쿼리의 on= (JSON 목록) → 켜진 슬롯 집합. 없으면 None(전부 켜짐)."""
    v = q.get("on", ["null"])[0]
    try: a = json.loads(v)
    except (ValueError, TypeError): return None
    return set(a) if isinstance(a, list) else None

def _must(q):
    """쿼리의 must= → True(전부) 또는 req= 의 슬롯 키 모음. 슬롯별 「반드시 뽑기」 (사용자 지시 2026-09-08)."""
    if q.get("must", ["0"])[0] == "1": return True
    return {t.strip() for t in q.get("req", [""])[0].split(",") if t.strip()}

def _cos(q):
    """쿼리의 cos= (쉼표 목록) → 복식 키 목록. 개수 제한 없이 OR 로 풀을 넓힌다 (사용자 지시 2026-09-09)."""
    return tuple(c for c in (x.strip() for x in q.get("cos", [""])[0].split(",")) if c in COSTUME_BIT)

def _design(q):
    """쿼리의 design= (쉼표 목록) → 디자인 모듈이 고른 태그 목록. 표정·포즈·씬의 질의에 앞세운다."""
    return [t.strip() for t in q.get("design", [""])[0].split(",") if t.strip()]
CHARCAP_DEFAULT = 100   # 캐릭터당 상한 기본값 (사용자 승인 2026-09-07). 30 과 100 은 분포가 비슷해 데이터를 더 남기는 100. 0 = 없음
def _charcap(q):
    """쿼리의 charcap= → 캐릭터당 상한(int). 없으면 기본값, 0 이면 상한 없음."""
    v = q.get("charcap", [""])[0].strip()
    return CHARCAP_DEFAULT if v == "" else max(0, int(float(v)))

