const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");
const compression = require("compression");
const { createClient } = require("@supabase/supabase-js");

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "canvas.json");
const SAVE_INTERVAL_MS = 10000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_TABLE = "canvas_pixels";

const ALLOWED_COLORS = new Set([
  "#175145","#2e8065","#51b341","#9bd547","#fff971","#ff7f4f",
  "#ff4f4f","#ee3046","#df426e","#ff88dd","#a62654","#621b52",
  "#371848","#0c082a","#261152","#272573","#4876bb","#7fd3e6",
  "#c7f7f2","#ffffff","#bbbbbb","#666666","#fdcbb0","#d29c8a",
  "#9e4d4d","#712835","#5d1835","#35082a"
]);
const ERASER_COLOR = "#ffffff";

const app = express();
app.use(compression({ threshold: 1024 }));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e6,
});

// Supabase client (lazy init)
let supabase = null;
if (SUPABASE_URL && SUPABASE_ANON_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
  console.log("[supabase] client initialized");
} else {
  console.log("[supabase] not configured, using file-based storage");
}

// ---- Canvas Data (infinite, sparse) ----
const canvasData = new Map();
let dirty = false;
let ready = false;
let loadError = null;

async function loadFromSupabase() {
  console.log("[load] loading from Supabase...");
  const { data, error } = await supabase
    .from(SUPABASE_TABLE)
    .select("x,y,color");

  if (error) {
    console.error("[load] Supabase error:", error.message);
    return;
  }

  let removed = 0;
  for (const row of data) {
    const { x, y, color } = row;
    if (typeof color !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(color)) {
      removed++;
      continue;
    }
    let normalized = color.toLowerCase();
    if (normalized === "#7fd3e0") normalized = "#7fd3e6";
    if (normalized === ERASER_COLOR) {
      removed++;
      continue;
    }
    if (ALLOWED_COLORS.has(normalized)) {
      canvasData.set(`${x},${y}`, normalized);
    } else {
      removed++;
    }
  }
  console.log(`[load] ${canvasData.size} pixels loaded from Supabase${removed ? `, ${removed} removed` : ""}`);
}

async function saveToSupabase() {
  if (!supabase) return false;
  if (!dirty) return true;

  console.log(`[save] saving ${canvasData.size} pixels to Supabase...`);
  const payload = Array.from(canvasData, ([k, color]) => {
    const [x, y] = k.split(",").map(Number);
    return { x, y, color };
  });

  // Delete all existing rows, then insert fresh snapshot
  // (Simpler approach for full sync; could use upsert for partial updates)
  try {
    await supabase.from(SUPABASE_TABLE).delete().neq("x", null);
    if (payload.length > 0) {
      const { error, rowCount } = await supabase
        .from(SUPABASE_TABLE)
        .upsert(payload, { onConflict: ["x", "y"] });

      if (error) {
        console.error("[save] Supabase upsert error:", error.message);
        return false;
      }
    }
    dirty = false;
    console.log(`[save] ${canvasData.size} pixels saved to Supabase`);
    return true;
  } catch (e) {
    console.error("[save] Supabase error:", e.message);
    return false;
  }
}

async function loadFromSupabaseBatch() {
  console.log("[load] loading from Supabase (batched)...");
  let from = 0;
  const batchSize = 10000;
  while (true) {
    const { data, error } = await supabase
      .from(SUPABASE_TABLE)
      .select("x,y,color")
      .order("x", { ascending: true })
      .order("y", { ascending: true })
      .range(from, from + batchSize - 1);
    if (error) {
      console.error("[load] Supabase error:", error.message);
      loadError = error.message;
      break;
    }
    if (!data || data.length === 0) break;
    for (const row of data) {
      const { x, y, color } = row;
      if (typeof color !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(color)) continue;
      let normalized = color.toLowerCase();
      if (normalized === "#7fd3e0") normalized = "#7fd3e6";
      if (normalized === ERASER_COLOR) continue;
      if (ALLOWED_COLORS.has(normalized)) canvasData.set(`${x},${y}`, normalized);
    }
    from += batchSize;
    if (data.length < batchSize) break;
  }
  console.log(`[load] ${canvasData.size} pixels loaded from Supabase`);
}

async function loadCanvas() {
  if (supabase) {
    try {
      await loadFromSupabaseBatch();
      ready = true;
      console.log("[load] ready");
      io.emit("ready", { pixels: canvasData.size });
    } catch (e) {
      console.error("[load] failed:", e.message);
      loadError = e.message;
      ready = true;
    }
  } else {
    try {
      if (fs.existsSync(DATA_FILE)) {
        const raw = fs.readFileSync(DATA_FILE, "utf-8");
        const obj = JSON.parse(raw);
        let removed = 0;
        for (const [k, v] of Object.entries(obj)) {
          if (typeof v === "string" && /^#[0-9A-Fa-f]{6}$/.test(v)) {
            let normalized = v.toLowerCase();
            if (normalized === "#7fd3e0") normalized = "#7fd3e6";
            if (normalized === ERASER_COLOR) { removed++; continue; }
            if (ALLOWED_COLORS.has(normalized)) canvasData.set(k, normalized);
            else removed++;
          }
        }
        console.log(`[load] ${canvasData.size} pixels loaded from ${DATA_FILE}${removed ? `, ${removed} removed (old palette)` : ""}`);
        if (removed > 0) { dirty = true; saveCanvas(); }
      } else {
        console.log("[load] no existing canvas file, starting empty");
      }
    } catch (e) {
      console.error("[load] failed:", e.message);
      loadError = e.message;
    }
    ready = true;
  }
}

function saveCanvas() {
  if (!dirty) return;
  if (supabase) {
    saveToSupabase().catch(e => {
      console.error("[save] failed:", e.message);
    });
  } else {
    try {
      const obj = Object.fromEntries(canvasData);
      fs.writeFileSync(DATA_FILE, JSON.stringify(obj));
      dirty = false;
      console.log(`[save] ${canvasData.size} pixels saved`);
    } catch (e) {
      console.error("[save] failed:", e.message);
    }
  }
}

// Initial load (async if Supabase) — don't block listen but set ready flag
loadCanvas();
// periodic save
setInterval(saveCanvas, SAVE_INTERVAL_MS);
// graceful shutdown
process.on("SIGINT", () => {
  console.log("\n[SIGINT] saving...");
  saveCanvas();
  process.exit(0);
});
process.on("SIGTERM", () => {
  saveCanvas();
  process.exit(0);
});

// ---- Health ----
app.get("/health", (req, res) => res.json({ ok: true, ready, pixels: canvasData.size, storage: supabase ? "supabase" : "file", error: loadError || undefined }));

// ---- Viewport API ----
// GET /api/pixels?x0=&y0=&x1=&y1=  (grid coords)
const MAX_VIEWPORT_PIXELS = 50000;
const MAX_VIEWPORT_SPAN = 500; // max width/height in grid units
app.get("/api/pixels", async (req, res) => {
  const x0 = parseInt(req.query.x0, 10);
  const y0 = parseInt(req.query.y0, 10);
  const x1 = parseInt(req.query.x1, 10);
  const y1 = parseInt(req.query.y1, 10);
  if ([x0,y0,x1,y1].some(v => !Number.isInteger(v))) {
    return res.status(400).json({ error: "x0,y0,x1,y1 required as integers" });
  }
  if (Math.abs(x0) > 1_000_000 || Math.abs(x1) > 1_000_000 || Math.abs(y0) > 1_000_000 || Math.abs(y1) > 1_000_000) {
    return res.status(400).json({ error: "coords out of range" });
  }
  const minX = Math.min(x0,x1), maxX = Math.max(x0,x1);
  const minY = Math.min(y0,y1), maxY = Math.max(y0,y1);
  const spanX = maxX - minX + 1, spanY = maxY - minY + 1;
  if (spanX > MAX_VIEWPORT_SPAN || spanY > MAX_VIEWPORT_SPAN) {
    return res.status(400).json({ error: `viewport too large (max ${MAX_VIEWPORT_SPAN}x${MAX_VIEWPORT_SPAN})` });
  }
  if (spanX * spanY > 250000) {
    return res.status(400).json({ error: "viewport area too large (max 250k cells, zoom in)" });
  }
  // Cache 10s on client, allow CDN 30s
  res.set("Cache-Control", "public, max-age=10, s-maxage=30");

  if (supabase) {
    try {
      const { data, error } = await supabase
        .from(SUPABASE_TABLE)
        .select("x,y,color")
        .gte("x", minX).lte("x", maxX)
        .gte("y", minY).lte("y", maxY)
        .limit(MAX_VIEWPORT_PIXELS);
      if (error) {
        console.error("[api/pixels] supabase error:", error.message);
        return res.status(500).json({ error: error.message });
      }
      // Filter by ALLOWED_COLORS (defense)
      const filtered = [];
      for (const r of data) {
        const c = (r.color||"").toLowerCase();
        if (c === ERASER_COLOR) continue;
        if (ALLOWED_COLORS.has(c)) filtered.push({ x: r.x, y: r.y, color: c });
      }
      return res.json({ pixels: filtered, truncated: data.length >= MAX_VIEWPORT_PIXELS });
    } catch (e) {
      console.error("[api/pixels] error:", e.message);
      return res.status(500).json({ error: e.message });
    }
  } else {
    const out = [];
    for (const [k, color] of canvasData) {
      const comma = k.indexOf(",");
      const x = parseInt(k.slice(0, comma), 10);
      const y = parseInt(k.slice(comma+1), 10);
      if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
        out.push({ x, y, color });
        if (out.length >= MAX_VIEWPORT_PIXELS) break;
      }
    }
    return res.json({ pixels: out, truncated: out.length >= MAX_VIEWPORT_PIXELS });
  }
});

// Lightweight meta endpoint for initial viewport decision
app.get("/api/meta", (req, res) => {
  res.set("Cache-Control", "public, max-age=5, s-maxage=10");
  res.json({ pixels: canvasData.size, ready, storage: supabase ? "supabase" : "file" });
});

// ---- Static serving ----
app.use(express.static(path.join(__dirname, "public"), {
  maxAge: "1d",
  etag: true,
  lastModified: true,
  setHeaders(res, filePath) {
    if (filePath.endsWith("index.html")) {
      res.setHeader("Cache-Control", "no-cache");
    }
  }
}));

function isValidDraw(payload) {
  if (!payload || typeof payload !== "object") return false;
  const { x, y, color } = payload;
  if (!Number.isInteger(x) || !Number.isInteger(y)) return false;
  if (typeof color !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(color)) return false;
  if (!ALLOWED_COLORS.has(color.toLowerCase())) return false;
  if (Math.abs(x) > 1_000_000 || Math.abs(y) > 1_000_000) return false;
  return true;
}

function isValidDrawBatch(payload) {
  if (!Array.isArray(payload)) return false;
  if (payload.length === 0 || payload.length > 50000) return false;
  for (const p of payload) {
    if (!isValidDraw(p)) return false;
  }
  return true;
}

// ---- Rate limit: 3 ops per 1000ms (all tools, per socket) ----
const DRAW_LIMIT = 3;
const DRAW_WINDOW_MS = 1000;
const BATCH_LIMIT = 20; // pen drag emits many small batches; allow higher burst
const socketDrawTimes = new Map(); // socket.id -> number[] for single draw
const socketBatchTimes = new Map(); // socket.id -> number[] for batch
function checkRateLimit(socket){
  const now = Date.now();
  let times = socketDrawTimes.get(socket.id) || [];
  times = times.filter(t => now - t < DRAW_WINDOW_MS);
  if(times.length >= DRAW_LIMIT) {
    socketDrawTimes.set(socket.id, times);
    return false;
  }
  times.push(now);
  socketDrawTimes.set(socket.id, times);
  return true;
}
function checkBatchRateLimit(socket, payloadLen){
  const now = Date.now();
  // large batch (rect/circle/line/fill commit) -> strict 2/sec, small pen drag -> 20/sec
  const isLargeBatch = payloadLen > 10;
  if(isLargeBatch){
    let times = socketDrawTimes.get(socket.id) || [];
    times = times.filter(t => now - t < DRAW_WINDOW_MS);
    if(times.length >= DRAW_LIMIT) {
      socketDrawTimes.set(socket.id, times);
      return false;
    }
    times.push(now);
    socketDrawTimes.set(socket.id, times);
    return true;
  }
  let times = socketBatchTimes.get(socket.id) || [];
  times = times.filter(t => now - t < DRAW_WINDOW_MS);
  if(times.length >= BATCH_LIMIT) {
    socketBatchTimes.set(socket.id, times);
    return false;
  }
  times.push(now);
  socketBatchTimes.set(socket.id, times);
  return true;
}

// ---- Socket.io ----
io.on("connection", (socket) => {
  console.log(`[connect] ${socket.id} connected, total=${io.engine.clientsCount}`);

  // New protocol: viewport-based. Keep legacy init only for ?legacy=1 clients
  const isLegacy = socket.handshake.query.legacy === "1" || socket.handshake.query.legacy === "true";
  if (isLegacy) {
    // Warning: large payload for 320k rows — legacy only
    socket.emit("init", Object.fromEntries(canvasData));
  } else {
    socket.emit("ready", { pixels: canvasData.size, ready });
    // also send userCount
  }
  io.emit("userCount", io.engine.clientsCount);

  socket.on("draw", (data) => {
    if (!isValidDraw(data)) return;
    if (!checkRateLimit(socket)) return;
    const { x, y, color } = data;
    const normalized = color.toLowerCase();
    const key = `${x},${y}`;
    if (normalized === ERASER_COLOR) {
      if (canvasData.has(key)) {
        canvasData.delete(key);
        dirty = true;
      } else {
        dirty = true;
      }
    } else {
      canvasData.set(key, normalized);
      dirty = true;
    }
    io.emit("draw", { x, y, color: normalized });
  });

  socket.on("drawBatch", (data) => {
    if (!isValidDrawBatch(data)) return;
    if (!checkBatchRateLimit(socket, data.length)) return;
    const normalizedBatch = [];
    for (const { x, y, color } of data) {
      const normalized = color.toLowerCase();
      const key = `${x},${y}`;
      if (normalized === ERASER_COLOR) {
        if (canvasData.has(key)) canvasData.delete(key);
      } else {
        canvasData.set(key, normalized);
      }
      normalizedBatch.push({ x, y, color: normalized });
    }
    dirty = true;
    io.emit("drawBatch", normalizedBatch);
  });

  socket.on("disconnect", () => {
    socketDrawTimes.delete(socket.id);
    socketBatchTimes.delete(socket.id);
    console.log(`[disconnect] ${socket.id}`);
    io.emit("userCount", io.engine.clientsCount);
  });
});

server.listen(PORT, () => {
  console.log(`epixel server listening on http://localhost:${PORT}`);
});