/* 태그 굴리기 — 앱 화면에 붙는 부분.
 *
 * 하는 일은 하나다: 화면에서 「생성 화면에 단추」를 켜 두었으면 생성 푸터에 **랜덤 생성**을 세우고,
 * 누르면 지금 조건으로 굴려 넣은 뒤 생성까지 건다.
 *
 * ★★**굴리고 넣는 규칙은 여기 두지 않는다.** 그것은 굴리기 화면(`web/index.html`)의 `rollAndPut` 이
 *   전부 알고 있고, 조건·인원·착의에 따라 넣는 자리가 달라진다. 여기에 베껴 두면 둘이 어긋난다 —
 *   그래서 이 확장은 **캔버스 창에 시키고 결과만 기다린다.**
 * ★캔버스는 한 번 열리면 다른 모드에 가도 떼지 않고 숨기므로, 생성 화면에 있어도 그 창이 살아 있다.
 *   아직 한 번도 안 열었으면 열어 두고 잠깐 기다린다.
 * ★생성은 **앱이 한다** (`generate` 액션). 큐에 넣는 것이라 기다릴 필요가 없다.
 */
(() => {
  const ID = "tag-roll";
  const SAY = {
    ko: { label: "랜덤 생성", busy: "굴리는 중…", fail: "굴리기에 실패했습니다" },
    en: { label: "Random generate", busy: "Rolling…", fail: "Rolling failed" },
    ja: { label: "ランダム生成", busy: "ロール中…", fail: "ロールに失敗しました" },
  };
  const say = (api) => SAY[api.locale?.()] ?? SAY.ko;

  /** 이 플러그인의 캔버스 창 — 없으면 열고 잠깐 기다린다 */
  async function frame(api) {
    const find = () => document.querySelector(`iframe[data-plugin-canvas="${ID}"]`);
    let f = find();
    if (f) return f;
    try {
      api.openCanvas(ID);
    } catch (e) {
      return null;
    }
    for (let i = 0; i < 40 && !(f = find()); i++) await new Promise((r) => setTimeout(r, 100));
    // 페이지가 스스로 뜰 시간 — 메시지를 너무 일찍 보내면 듣는 쪽이 아직 없다
    if (f) await new Promise((r) => setTimeout(r, 800));
    return f;
  }

  /** 캔버스에 「굴려서 넣어라」를 시키고 끝날 때까지 기다린다 */
  function ask(f) {
    return new Promise((resolve) => {
      const id = Date.now() + Math.random();
      const done = (ok) => {
        window.removeEventListener("message", onMsg);
        clearTimeout(timer);
        resolve(ok);
      };
      const onMsg = (e) => {
        const m = e.data;
        if (m && m.type === "tagroll" && m.id === id) done(!!m.ok);
      };
      window.addEventListener("message", onMsg);
      // ★표본이 크면 한 번 굴리는 데 몇 초가 걸린다 — 넉넉히 두되 영원히 기다리지는 않는다
      const timer = setTimeout(() => done(false), 120000);
      f.contentWindow.postMessage({ type: "tagroll", call: "rollAndPut", id }, "*");
    });
  }

  window.peropix.registerExtension({
    name: "tag-roll.quick",
    async setup(api) {
      // ★못 읽거나 꺼져 있으면 단추를 세우지 않는다 — 켠 적 없는 사용자에게 화면이 달라지면 안 된다
      let on = false;
      try {
        const r = await fetch(`${api.backend}/plug/${ID}/api/opt`);
        on = !!(await r.json()).quickGen;
      } catch (e) {
        return;
      }
      if (!on) return;

      api.addButton("generate.footer", {
        label: { ko: SAY.ko.label, en: SAY.en.label, ja: SAY.ja.label },
        onClick: async () => {
          const s = say(api);
          const f = await frame(api);
          if (!f) return api.toast(s.fail, "warn");
          api.toast(s.busy);
          const ok = await ask(f);
          if (!ok) return api.toast(s.fail, "warn");
          await api.action("generate", {});
        },
      });
    },
  });
})();
