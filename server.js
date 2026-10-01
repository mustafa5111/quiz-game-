const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const QUESTIONS = require("./questions");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

const rooms = new Map();
const QUESTION_TIME = 20; // ثانية لكل سؤال
const REVEAL_TIME = 5;    // ثواني عرض الإجابة الصحيحة

function makeCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? makeCode() : code;
}

function shuffled(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function lobbyState(room) {
  return {
    code: room.code,
    players: [...room.players.values()].map(p => ({ id: p.id, name: p.name, isHost: p.id === room.hostId })),
  };
}

function scores(room) {
  return [...room.players.values()]
    .map(p => ({ name: p.name, score: p.score, streak: p.streak }))
    .sort((a, b) => b.score - a.score);
}

function sendQuestion(room) {
  const q = room.questions[room.qIndex];
  room.state = "question";
  room.qStart = Date.now();
  room.answers = new Map();
  io.to(room.code).emit("question", {
    index: room.qIndex + 1,
    total: room.questions.length,
    text: q.q,
    options: q.options,
    time: QUESTION_TIME,
  });
  room.timer = setTimeout(() => reveal(room), QUESTION_TIME * 1000);
}

function reveal(room) {
  clearTimeout(room.timer);
  const q = room.questions[room.qIndex];
  room.state = "reveal";
  const elapsed = (Date.now() - room.qStart) / 1000;
  for (const p of room.players.values()) {
    const ans = room.answers.get(p.id);
    if (ans !== undefined && ans === q.answer) {
      const timeBonus = Math.max(0, Math.round(100 * (1 - elapsed / QUESTION_TIME)));
      p.streak += 1;
      p.score += 100 + timeBonus + Math.min(p.streak * 10, 50);
    } else {
      p.streak = 0;
    }
  }
  io.to(room.code).emit("reveal", { correct: q.answer, scores: scores(room) });
  room.timer = setTimeout(() => {
    room.qIndex++;
    if (room.qIndex >= room.questions.length) {
      room.state = "finished";
      io.to(room.code).emit("finished", { scores: scores(room) });
    } else {
      sendQuestion(room);
    }
  }, REVEAL_TIME * 1000);
}

io.on("connection", (socket) => {
  socket.on("create_room", ({ name }) => {
    const code = makeCode();
    const room = { code, hostId: socket.id, players: new Map(), state: "lobby", questions: [], qIndex: 0 };
    room.players.set(socket.id, { id: socket.id, name: (name || "مضيف").slice(0, 20), score: 0, streak: 0 });
    rooms.set(code, room);
    socket.join(code);
    socket.emit("room_joined", { ...lobbyState(room), isHost: true });
  });

  socket.on("join_room", ({ code, name }) => {
    code = (code || "").toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return socket.emit("error_msg", "رمز الغرفة غير صحيح");
    if (room.state !== "lobby") return socket.emit("error_msg", "اللعبة بدأت بالفعل");
    room.players.set(socket.id, { id: socket.id, name: (name || "لاعب").slice(0, 20), score: 0, streak: 0 });
    socket.join(code);
    socket.emit("room_joined", { ...lobbyState(room), isHost: false });
    io.to(code).emit("lobby_update", lobbyState(room));
  });

  socket.on("start_game", ({ count }) => {
    const room = [...rooms.values()].find(r => r.hostId === socket.id);
    if (!room || room.state !== "lobby") return;
    const n = Math.min(Math.max(parseInt(count) || 10, 5), QUESTIONS.length);
    room.questions = shuffled(QUESTIONS).slice(0, n);
    room.qIndex = 0;
    for (const p of room.players.values()) { p.score = 0; p.streak = 0; }
    io.to(room.code).emit("game_started", {});
    sendQuestion(room);
  });

  socket.on("submit_answer", ({ answer }) => {
    const room = [...rooms.values()].find(r => r.players.has(socket.id));
    if (!room || room.state !== "question") return;
    if (room.answers.has(socket.id)) return;
    room.answers.set(socket.id, answer);
    // إذا أجاب الجميع نكشف مبكراً
    if (room.answers.size === room.players.size) reveal(room);
  });

  socket.on("disconnect", () => {
    for (const [code, room] of rooms) {
      if (room.players.has(socket.id)) {
        room.players.delete(socket.id);
        if (room.players.size === 0) {
          clearTimeout(room.timer);
          rooms.delete(code);
        } else {
          if (room.hostId === socket.id) room.hostId = [...room.players.keys()][0];
          if (room.state === "lobby") io.to(code).emit("lobby_update", lobbyState(room));
        }
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("اللعبة شغالة على المنفذ " + PORT));
