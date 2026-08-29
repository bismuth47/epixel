const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");
const { createClient } = require("@supabase/supabase-js");

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "canvas.json");
const SAVE_INTERVAL_MS = 10000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_TABLE = "canvas_pixels";

const ALLOWED_COLORS = new Set([
  "#175145","#2e8065","#51b341","#9bd547","#fff971","#ff7f4f",
  "#ff4f4f","#ee3046","#df426e","#a62654","#621b52","#371848",
  "#0c082a","#261152","#272573","#4876bb","#7fd3e0","#c7f7f2",
  "#ffffff","#d29c8a","#9e4d4d","#712835","#5d1835","#35082a"
]);
const ERASER_COLOR = "#ffffff";

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
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
    const normalized = color.toLowerCase();
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
  // For large datasets, use batched loading
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
      break;
    }

    if (!data || data.length === 0) break;

    for (const row of data) {
      const { x, y, color } = row;
      if (typeof color !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(color)) {
        continue;
      }
      const normalized = color.toLowerCase();
      if (normalized === ERASER_COLOR) continue;
      if (ALLOWED_COLORS.has(normalized)) {
        canvasData.set(`${x},${y}`, normalized);
      }
    }

    from += batchSize;
    if (data.length < batchSize) break;
  }

  console.log(`[load] ${canvasData.size} pixels loaded from Supabase`);
}

function loadCanvas() {
  if (supabase) {
    // Use batched loading for potentially large datasets
    loadFromSupabaseBatch().catch(e => {
      console.error("[load] failed:", e.message);
    });
  } else {
    try {
      if (fs.existsSync(DATA_FILE)) {
        const raw = fs.readFileSync(DATA_FILE, "utf-8");
        const obj = JSON.parse(raw);
        let removed = 0;
        for (const [k, v] of Object.entries(obj)) {
          if (typeof v === "string" && /^#[0-9A-Fa-f]{6}$/.test(v)) {
            const normalized = v.toLowerCase();
            if (normalized === ERASER_COLOR) {
              removed++;
              continue;
            }
            if (ALLOWED_COLORS.has(normalized)) {
              canvasData.set(k, normalized);
            } else {
              removed++;
            }
          }
        }
        console.log(`[load] ${canvasData.size} pixels loaded from ${DATA_FILE}${removed ? `, ${removed} removed (old palette)` : ""}`);
        if (removed > 0) {
          dirty = true;
          saveCanvas();
        }
      } else {
        console.log("[load] no existing canvas file, starting empty");
      }
    } catch (e) {
      console.error("[load] failed:", e.message);
    }
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

// Initial load (async if Supabase)
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

// ---- Static serving ----
app.use(express.static(path.join(__dirname, "public")));
app.get("/health", (req, res) => res.json({ ok: true, pixels: canvasData.size, storage: supabase ? "supabase" : "file" }));

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

// ---- Socket.io ----
io.on("connection", (socket) => {
  console.log(`[connect] ${socket.id} connected, total=${io.engine.clientsCount}`);

  // Send full canvas state to new client
  socket.emit("init", Object.fromEntries(canvasData));
  // Notify count
  io.emit("userCount", io.engine.clientsCount);

  socket.on("draw", (data) => {
    if (!isValidDraw(data)) return;
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
    console.log(`[disconnect] ${socket.id}`);
    io.emit("userCount", io.engine.clientsCount);
  });
});

server.listen(PORT, () => {
  console.log(`Everyone Draw server listening on http://localhost:${PORT}`);
});