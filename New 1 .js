// لعبة الأسئلة الجماعية — سيرفر Deno KV
// الغرف محفوظة في Deno KV المشتركة، فتعمل حتى لو تعددت نسخ السيرفر (isolates)
// لا توجد مؤقتات على السيرفر — كل التوقيت يقوده اللاعبون عبر الأحداث

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const QUESTIONS = require("./questions");

const QUESTION_TIME = 20; // ثانية لكل سؤال
const REVEAL_TIME = 5;    // ثواني عرض الإجابة
const ROOM_TTL_MS = 3 * 60 * 60 * 1000; // الغرفة تعيش 3 ساعات
const EV_TTL_MS = 120 * 1000;           // أحداث البث تعيش دقيقتين

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "index.html")));

function makeCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}
function shuffled(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function evId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
}
const clone = (o) => JSON.parse(JSON.stringify(o));

function lobbyPlayers(room) {
  return room.players.map((p) => ({
    name: p.name, score: p.score, streak: p.streak,
    isHost: p.sid === room.hostSid, connected: p.connected,
  }));
}
function scores(room) {
  return room.players
    .map((p) => ({ name: p.name, score: p.score, streak: p.streak, connected: p.connected }))
    .sort((a, b) => b.score - a.score);
}
function questionPayload(room) {
  const q = room.questions[room.qIndex];
  return { index: room.qIndex + 1, total: room.questions.length, text: q.q, options: q.options, endsAt: room.qEnd };
}
function syncState(room, sid) {
  const base = { code: room.code, state: room.state, players: lobbyPlayers(room), isHost: room.hostSid === sid };
  if (room.state === "question") base.question = questionPayload(room);
  else if (room.state === "reveal") {
    const q = room.questions[room.qIndex];
    base.reveal = { correct: q.answer, scores: scores(room), index: room.qIndex + 1 };
  } else if (room.state === "finished") base.finished = { scores: scores(room) };
  return base;
}

(async () => {
  let kv;
  try {
    kv = await Deno.openKv();
  } catch (e) {
    console.error("Deno KV failed — اربط قاعدة بيانات Deno KV بالمشروع من لوحة التحكم:", e.message);
    throw e;
  }
  console.log("Deno KV ready");

  const roomKey = (code) => ["qz", "room", code];
  const evKey = (code, id) => ["qz", "ev", code, id];

  // مقابس هذه النسخة فقط
  const localSockets = new Map(); // sid -> { socket, code }
  const seenEvents = new Set();

  // تعديل الغرفة بعملية ذرية + كتابة أحداث البث بنفس العملية
  async function mutateRoom(code, fn, maxRetries = 8) {
    for (let i = 0; i < maxRetries; i++) {
      try {
        const entry = await kv.get(roomKey(code));
        const cur = entry.value ? clone(entry.value) : null;
        let res;
        try { res = fn(cur); } catch (e) { return { ok: false, reason: "err" }; }
        if (!res || res.abort) return { ok: false, reason: (res && res.reason) || "abort" };
        const atom = kv.atomic();
        if (entry.value) atom.check({ key: roomKey(code), versionstamp: entry.versionstamp });
        else atom.check({ key: roomKey(code), versionstamp: null });
        if (res.room) atom.set(roomKey(code), res.room, { expireIn: ROOM_TTL_MS });
        else atom.delete(roomKey(code));
        for (const e of (res.events || [])) {
          atom.set(evKey(code, evId()), { code, type: e.type, data: e.data, ts: Date.now() }, { expireIn: EV_TTL_MS });
        }
        if ((res.events || []).length > 0) bumpSignal(atom);
        const cr = await atom.commit();
        if (cr.ok) return { ok: true, room: res.room || null };
      } catch (e) {
        if (/locked|busy/i.test(e.message || "") && i < maxRetries - 1) {
          await new Promise((r) => setTimeout(r, 60 + Math.random() * 120));
          continue;
        }
        throw e;
      }
    }
    return { ok: false, reason: "busy" };
  }

  // مفتاح إشارة عام: أي حدث جديد يرفع إشعاراً لكل النسخ
  function bumpSignal(atom) {
    atom.set(["qz", "signal"], { t: Date.now(), r: Math.random() });
  }

  function routeEvent(ev) {
    for (const info of localSockets.values()) {
      if (info.code === ev.code) info.socket.emit(ev.type, ev.data);
    }
  }

  // حلقة مراقبة الإشارة: عند أي حدث جديد نجلب الأحداث غير المقروءة لغرفنا المحلية
  (async function watchLoop() {
    for (;;) {
      try {
        const stream = kv.watch([["qz", "signal"]]);
        for await (const batch of stream) {
          if (!batch[0] || !batch[0].value) continue;
          const codes = new Set();
          for (const info of localSockets.values()) if (info.code) codes.add(info.code);
          for (const code of codes) {
            for (let li = 0; li < 4; li++) {
              try {
                for await (const entry of kv.list({ prefix: ["qz", "ev", code] })) {
                  const id = entry.key[3];
                  if (seenEvents.has(id)) continue;
                  seenEvents.add(id);
                  try { routeEvent(entry.value); } catch (e) { console.error("route err", e.message); }
                }
                break;
              } catch (e) {
                if (/locked|busy/i.test(e.message || "") && li < 3) {
                  await new Promise((r) => setTimeout(r, 80 + Math.random() * 120));
                  continue;
                }
                console.error("list err", e.message);
                break;
              }
            }
          }
          if (seenEvents.size > 3000) {
            const it = seenEvents.values();
            for (let i = 0; i < 1000; i++) { const v = it.next().value; if (v === undefined) break; seenEvents.delete(v); }
          }
        }
      } catch (e) {
        console.error("watch err, retry:", e.message);
        await new Promise((r) => setTimeout(r, 3000));
      }
    }
  })();

  // فحص: هل حان وقت كشف الإجابة؟
  async function maybeReveal(code) {
    await mutateRoom(code, (cur) => {
      if (!cur || cur.state !== "question") return { abort: true };
      const q = cur.questions[cur.qIndex];
      const sids = cur.players.filter((p) => p.connected).map((p) => p.sid);
      const allAnswered = sids.every((s) => cur.answers[s] !== undefined);
      if (!allAnswered && Date.now() < cur.qEnd) return { abort: true };
      cur.state = "reveal";
      cur.revealEnd = Date.now() + REVEAL_TIME * 1000;
      return { room: cur, events: [{ type: "reveal", data: { correct: q.answer, scores: scores(cur), index: cur.qIndex + 1 } }] };
    });
  }

  const safe = (fn) => (...a) => { Promise.resolve(fn(...a)).catch((e) => console.error("handler:", e.message)); };

  io.on("connection", (socket) => {
    localSockets.set(socket.id, { socket, code: null });

    socket.on("create_room", safe(async ({ name }) => {
      const cleanName = String(name || "مضيف").slice(0, 20);
      for (let a = 0; a < 8; a++) {
        const code = makeCode();
        const room = {
          code, state: "lobby", hostSid: socket.id,
          players: [{ sid: socket.id, name: cleanName, score: 0, streak: 0, connected: true }],
          questions: [], qIndex: 0, qStart: 0, qEnd: 0, revealEnd: 0, answers: {}, createdAt: Date.now(),
        };
        const r = await mutateRoom(code, (existing) => {
          if (existing) return { abort: true, reason: "collision" };
          return { room, events: [] };
        });
        if (r.ok) {
          localSockets.get(socket.id).code = code;
          socket.emit("room_joined", { code, players: lobbyPlayers(room), isHost: true });
          return;
        }
      }
      socket.emit("error_msg", "تعذر إنشاء الغرفة، حاول مجدداً");
    }));

    socket.on("join_room", safe(async ({ code, name }) => {
      code = String(code || "").toUpperCase().trim();
      const cleanName = String(name || "لاعب").slice(0, 20);
      const entry = await kv.get(roomKey(code));
      const room = entry.value;
      if (!room) return socket.emit("error_msg", "رمز الغرفة غير صحيح");

      if (room.state === "lobby") {
        const r = await mutateRoom(code, (cur) => {
          if (!cur || cur.state !== "lobby") return { abort: true, reason: "state" };
          const old = cur.players.find((p) => p.name === cleanName && !p.connected);
          if (old) { old.sid = socket.id; old.connected = true; }
          else cur.players.push({ sid: socket.id, name: cleanName, score: 0, streak: 0, connected: true });
          return { room: cur, events: [{ type: "lobby_update", data: { code, players: lobbyPlayers(cur) } }] };
        });
        if (!r.ok) return socket.emit("error_msg", "تعذر الدخول، حاول مجدداً");
        localSockets.get(socket.id).code = code;
        socket.emit("room_joined", { code, players: lobbyPlayers(r.room), isHost: r.room.hostSid === socket.id });
        return;
      }

      // دخول أثناء اللعب: تبنّي مقعد بنفس الاسم إن كان صاحبه منقطعاً
      const r = await mutateRoom(code, (cur) => {
        if (!cur) return { abort: true, reason: "gone" };
        const p = cur.players.find((x) => x.name === cleanName && !x.connected);
        if (!p) return { abort: true, reason: "started" };
        const wasHost = cur.hostSid === p.sid;
        p.sid = socket.id; p.connected = true;
        if (wasHost) cur.hostSid = socket.id;
        return { room: cur, events: [{ type: "lobby_update", data: { code, players: lobbyPlayers(cur) } }] };
      });
      if (!r.ok) return socket.emit("error_msg", r.reason === "started" ? "اللعبة بدأت بالفعل" : "تعذر الدخول");
      localSockets.get(socket.id).code = code;
      socket.emit("room_joined", { code, players: lobbyPlayers(r.room), isHost: r.room.hostSid === socket.id, rejoin: true });
      socket.emit("sync_state", syncState(r.room, socket.id));
    }));

    socket.on("start_game", safe(async ({ count }) => {
      const info = localSockets.get(socket.id);
      if (!info || !info.code) return;
      const r = await mutateRoom(info.code, (cur) => {
        if (!cur || cur.state !== "lobby" || cur.hostSid !== socket.id) return { abort: true };
        const n = Math.min(Math.max(parseInt(count) || 10, 5), QUESTIONS.length);
        cur.questions = shuffled(QUESTIONS).slice(0, n);
        cur.qIndex = 0;
        cur.players.forEach((p) => { p.score = 0; p.streak = 0; });
        cur.state = "question";
        cur.qStart = Date.now();
        cur.qEnd = Date.now() + QUESTION_TIME * 1000;
        cur.answers = {};
        return { room: cur, events: [
          { type: "game_started", data: {} },
          { type: "question", data: questionPayload(cur) },
        ] };
      });
      if (!r.ok) socket.emit("error_msg", "تعذر بدء اللعبة");
    }));

    socket.on("submit_answer", safe(async ({ answer }) => {
      const info = localSockets.get(socket.id);
      if (!info || !info.code) return;
      const code = info.code;
      const now = Date.now();
      await mutateRoom(code, (cur) => {
        if (!cur || cur.state !== "question") return { abort: true };
        if (cur.answers[socket.id] !== undefined) return { abort: true };
        const p = cur.players.find((x) => x.sid === socket.id);
        if (!p || !p.connected) return { abort: true };
        const q = cur.questions[cur.qIndex];
        if (answer === q.answer) {
          const elapsed = Math.max(0, (now - cur.qStart) / 1000);
          const timeBonus = Math.max(0, Math.round(100 * (1 - elapsed / QUESTION_TIME)));
          p.streak += 1;
          p.score += 100 + timeBonus + Math.min(p.streak * 10, 50);
        } else p.streak = 0;
        cur.answers[socket.id] = answer;
        return { room: cur, events: [] };
      });
      await maybeReveal(code);
    }));

    socket.on("tick", safe(async () => {
      const info = localSockets.get(socket.id);
      if (info && info.code) await maybeReveal(info.code);
    }));

    socket.on("next_question", safe(async () => {
      const info = localSockets.get(socket.id);
      if (!info || !info.code) return;
      await mutateRoom(info.code, (cur) => {
        if (!cur || cur.state !== "reveal") return { abort: true };
        cur.qIndex++;
        if (cur.qIndex >= cur.questions.length) {
          cur.state = "finished";
          return { room: cur, events: [{ type: "finished", data: { scores: scores(cur) } }] };
        }
        cur.state = "question";
        cur.qStart = Date.now();
        cur.qEnd = Date.now() + QUESTION_TIME * 1000;
        cur.answers = {};
        return { room: cur, events: [{ type: "question", data: questionPayload(cur) }] };
      });
    }));

    socket.on("request_sync", safe(async () => {
      const info = localSockets.get(socket.id);
      if (!info || !info.code) return;
      const cur = (await kv.get(roomKey(info.code))).value;
      if (cur) socket.emit("sync_state", syncState(cur, socket.id));
    }));

    socket.on("disconnect", safe(async () => {
      const info = localSockets.get(socket.id);
      localSockets.delete(socket.id);
      if (!info || !info.code) return;
      const code = info.code;
      await mutateRoom(code, (cur) => {
        if (!cur) return { abort: true };
        const p = cur.players.find((x) => x.sid === socket.id);
        if (p) p.connected = false;
        const anyConnected = cur.players.some((x) => x.connected);
        if (!anyConnected && cur.state === "lobby") return { room: null };
        if (cur.hostSid === socket.id) {
          const np = cur.players.find((x) => x.connected) || cur.players[0];
          if (np) cur.hostSid = np.sid;
        }
        const events = [{ type: "lobby_update", data: { code, players: lobbyPlayers(cur) } }];
        if (cur.state === "question") {
          const sids = cur.players.filter((x) => x.connected).map((x) => x.sid);
          if (sids.every((s) => cur.answers[s] !== undefined)) {
            const q = cur.questions[cur.qIndex];
            cur.state = "reveal";
            cur.revealEnd = Date.now() + REVEAL_TIME * 1000;
            events.push({ type: "reveal", data: { correct: q.answer, scores: scores(cur), index: cur.qIndex + 1 } });
          }
        }
        return { room: cur, events };
      });
    }));
  });

  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => console.log("اللعبة شغالة على المنفذ " + PORT));
})().catch((e) => { console.error("فشل تشغيل السيرفر:", e.message); process.exit(1); });
