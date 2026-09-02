const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");
const compression = require("compression");
const { createClient } = require("@supabase/supabase-js");
const { Pool } = require("pg");
const ChunkCodec = require("./shared/chunk-codec");
let pngWorker = null;
try { pngWorker = require("./server/png-worker"); } catch (e) { console.warn("[png-worker] not loaded:", e.message); }

try { require("dotenv").config(); } catch (e) {}

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "canvas.json");
const SAVE_INTERVAL_MS = 2000;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_DB_PASSWORD = process.env.SUPABASE_DB_PASSWORD;
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || "canvas_pixels";
const CHUNK_TABLE = "canvas_chunks";
const DELTA_TABLE = "canvas_pixel_deltas";
const CHUNK_SIZE = ChunkCodec.CHUNK_SIZE;
const PNG_BUCKET = (pngWorker && pngWorker.BUCKET) || "chunk-pngs";
const PNG_CRON_MS = 60 * 1000;
const PNG_MAX_PER_TICK = (pngWorker && pngWorker.MAX_CHUNKS_PER_TICK) || 20;

// Direct PostgreSQL pool (fallback when Supabase REST API key is invalid)
let pgPool = null;
if (SUPABASE_URL && SUPABASE_DB_PASSWORD) {
  const urlMatch = SUPABASE_URL.match(/https:\/\/([^.]+)\.supabase\.co/);
  if (urlMatch) {
    const projectRef = urlMatch[1];
    pgPool = new Pool({
      host: `aws-0-ap-northeast-1.pooler.supabase.com`,
      port: 6543,
      database: "postgres",
      user: `postgres.${projectRef}`,
      password: SUPABASE_DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 10000,
      query_timeout: 30000,
    });
  }
}

// ── Color code mapping (0–27 = 28 drawable colors, white/eraser is special) ──
const CODE_TO_COLOR = [
  "#175145","#2e8065","#51b341","#9bd547","#fff971","#ff7f4f",
  "#ff4f4f","#ee3046","#df426e","#ff88dd","#a62654","#621b52",
  "#2f154d","#000000","#333333","#272573","#4876bb","#7fd3e6",
  "#c7f7f2","#bbbbbb","#666666","#fdcbb0","#d29c8a",
  "#9e4d4d","#712835","#5d1835","#35082a","#ffbc60"
];
const COLOR_TO_CODE = new Map(CODE_TO_COLOR.map((c, i) => [c.toLowerCase(), i]));
const VALID_CODES = new Set(CODE_TO_COLOR.map((_, i) => i));
const ERASER_CODE = 255; // special sentinel, never stored in DB
const ERASER_COLOR = "#ffffff";

// Convert hex string or numeric code to a numeric code (0–27) or ERASER_CODE.
// Handles: numeric codes, hex strings (legacy), numeric-looking text (edge case).
// Returns undefined for invalid colors.
function normalizeColor(color) {
  if (typeof color === "number" && Number.isInteger(color)) {
    if (color === ERASER_CODE || (color >= 0 && color <= 27)) return color;
    return undefined;
  }
  if (typeof color !== "string") return undefined;
  // Check for numeric code stored as text (e.g. "5")
  if (/^\d+$/.test(color)) {
    const num = parseInt(color, 10);
    if (num === ERASER_CODE || (num >= 0 && num <= 27)) return num;
    return undefined;
  }
  // Legacy hex string
  let normalized = color.toLowerCase();
  if (normalized === "#7fd3e0") normalized = "#7fd3e6"; // legacy palette rename
  if (normalized === ERASER_COLOR) return ERASER_CODE;
  return COLOR_TO_CODE.get(normalized);
}

// Convert a hex string to a numeric code (used during data migration / legacy load)
function hexToCode(hex) {
  let normalized = hex.toLowerCase();
  if (normalized === "#7fd3e0") normalized = "#7fd3e6";
  if (normalized === ERASER_COLOR) return ERASER_CODE;
  return COLOR_TO_CODE.get(normalized);
}

const app = express();
app.use(compression({ threshold: 1024 }));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e6,
});

// Supabase client (prefer service_role key; fall back to anon)
let supabase = null;
let usePgFallback = false;
const SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log(`[supabase] client initialized (${SUPABASE_SERVICE_ROLE_KEY ? 'service_role' : 'anon'})`);
} else if (pgPool) {
  console.log("[supabase] JS client not configured, using pg pool connection");
} else {
  console.log("[supabase] not configured, using file-based storage");
}
if (pgPool) {
  console.log("[supabase] pg pool available as fallback");
}

// ── Storage mode determination ──
// Local: file (canvas.json) — no SUPABASE env → isolated, no side effect on production
// Production (Render): supabase — MUST have DB, never fall back to file
const STORAGE_MODE = (supabase || pgPool) ? "supabase" : "file";
const IS_PRODUCTION = process.env.NODE_ENV === "production" || process.env.RENDER === "true" || process.env.REQUIRE_SUPABASE === "true";
if (IS_PRODUCTION && STORAGE_MODE === "file") {
  console.error("[fatal] SUPABASE not configured in production — refusing to start with file storage (would expose empty/lossy canvas)");
  console.error("        Set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (or ANON_KEY) and SUPABASE_DB_PASSWORD in Render env vars");
  process.exit(1);
}
if (STORAGE_MODE === "supabase" && fs.existsSync(DATA_FILE)) {
  console.warn(`[storage] DB mode active — local ${path.basename(DATA_FILE)} exists but will be IGNORED (prevents local canvas.json from affecting production map)`);
}
console.log(`[storage] mode=${STORAGE_MODE} production=${IS_PRODUCTION}`);

// ---- Canvas Data (infinite, sparse) ----
const canvasData = new Map();
let dirty = false;
let ready = false;
let loadError = null;
// Incremental save state for Supabase (avoid full 66k delete+upsert that blocks event loop)
let isSaving = false;
const pendingUpserts = new Map(); // "x,y" -> color
const pendingDeletes = new Set(); // "x,y"

// Helper: convert hex string, Buffer, or Uint8Array from PostgREST BYTEA to Uint8Array
function bufferToUint8Array(data) {
  if (!data) return new Uint8Array(0);
  if (typeof data === "string") {
    if (data.startsWith("\\x")) {
      return Uint8Array.from(Buffer.from(data.slice(2), "hex"));
    }
    return Uint8Array.from(Buffer.from(data, "hex"));
  }
  if (Buffer.isBuffer(data)) {
    return new Uint8Array(data);
  }
  if (data instanceof Uint8Array) {
    return data;
  }
  return new Uint8Array(data);
}

// Helper: convert Uint8Array to \\x hex string for PostgREST BYTEA insert
function bufferToHex(buf) {
  return "\\x" + Buffer.from(buf).toString("hex");
}

// Helper: convert base64 string from Supabase JS client back to Uint8Array
function base64ToUint8Array(b64) {
  if (!b64) return new Uint8Array(0);
  return new Uint8Array(Buffer.from(b64, "base64"));
}

async function loadFromSupabaseBatch() {
  console.log("[load] loading from Supabase (chunks + legacy pixels)...");
  let chunksLoaded = 0;

  // ── 1. Load from canvas_chunks (primary storage) ──────────────
  try {
    let from = 0;
    const batchSize = 1000;
    while (true) {
       let data, error;
      if (supabase && !usePgFallback) {
        const result = await supabase
          .from(CHUNK_TABLE)
          .select("chunk_x,chunk_y,data")
          .order("chunk_x", { ascending: true })
          .order("chunk_y", { ascending: true })
          .range(from, from + batchSize - 1);
        data = result.data;
        error = result.error;
        // Detect auth failure and switch to pgPool fallback
        if (error && error.message.includes("Invalid API key") && pgPool) {
          console.log("[load] Supabase REST API auth failed, switching to pg pool fallback");
          usePgFallback = true;
          continue;
        }
      } else if (pgPool) {
        const result = await pgPool.query(
          `SELECT chunk_x, chunk_y, data FROM ${CHUNK_TABLE} ORDER BY chunk_x ASC, chunk_y ASC LIMIT $1 OFFSET $2`,
          [batchSize, from]
        );
        data = result.rows;
        error = null;
      } else {
        break;
      }
      if (error) {
        console.error("[load] canvas_chunks error:", error.message);
        break;
      }
      if (!data || data.length === 0) break;
      for (const chunk of data) {
        const binary = bufferToUint8Array(chunk.data);
        const pixels = ChunkCodec.decode(binary);
        for (const p of pixels) {
          const gx = chunk.chunk_x * CHUNK_SIZE + p.x;
          const gy = chunk.chunk_y * CHUNK_SIZE + p.y;
          canvasData.set(`${gx},${gy}`, p.colorId);
        }
        chunksLoaded++;
      }
      from += batchSize;
      if (data.length < batchSize) break;
    }
  } catch (e) {
    console.error("[load] canvas_chunks failed:", e.message);
  }

  // ── 2. Load from canvas_pixels (legacy/transition) ─────────────
  // This captures any pixels drawn since the last chunk migration.
  // Newer data overrides older data in the Map.
  try {
    let from = 0;
    const batchSize = 1000;
    while (true) {
      let data, error;
      if (supabase && !usePgFallback) {
        const result = await supabase
          .from(SUPABASE_TABLE)
          .select("x,y,color")
          .order("x", { ascending: true })
          .order("y", { ascending: true })
          .range(from, from + batchSize - 1);
        data = result.data;
        error = result.error;
      } else if (pgPool) {
        const result = await pgPool.query(
          `SELECT x, y, color FROM ${SUPABASE_TABLE} ORDER BY x ASC, y ASC LIMIT $1 OFFSET $2`,
          [batchSize, from]
        );
        data = result.rows;
        error = null;
      } else {
        break;
      }
      if (error) {
        console.error("[load] canvas_pixels error:", error.message);
        break;
      }
      if (!data || data.length === 0) break;
      for (const row of data) {
        const { x, y, color } = row;
        const code = normalizeColor(color);
        if (code === undefined || code === ERASER_CODE) continue;
        canvasData.set(`${x},${y}`, code);
      }
      from += batchSize;
      if (data.length < batchSize) break;
    }
  } catch (e) {
    console.error("[load] canvas_pixels legacy load failed:", e.message);
  }

  console.log(`[load] ${canvasData.size} pixels loaded from Supabase (${chunksLoaded} chunks + legacy pixels)`);
}

async function loadCanvas() {
  if (supabase || pgPool) {
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
    // file mode: local dev only (production would have exited above)
    console.log("[load] file mode — using local canvas.json (isolated from production)");
    try {
      if (fs.existsSync(DATA_FILE)) {
        const raw = fs.readFileSync(DATA_FILE, "utf-8");
        const obj = JSON.parse(raw);
        let removed = 0;
        for (const [k, v] of Object.entries(obj)) {
          const code = typeof v === "number" ? v : hexToCode(v);
          if (code === undefined || code === ERASER_CODE || !VALID_CODES.has(code)) {
            removed++;
            continue;
          }
          canvasData.set(k, code);
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

async function saveToSupabase() {
  if (!supabase && !pgPool) return false;
  if (isSaving) return true;
  if (pendingUpserts.size === 0 && pendingDeletes.size === 0) return true;

  isSaving = true;
  // snapshot and clear to allow new draws during save
  const upserts = new Map(pendingUpserts);
  const deletes = new Set(pendingDeletes);
  pendingUpserts.clear();
  pendingDeletes.clear();

  const totalOps = upserts.size + deletes.size;
  console.log(`[save] saving ${totalOps} ops (upserts=${upserts.size} deletes=${deletes.size}) totalPixels=${canvasData.size}...`);

  try {
    // Group operations by chunk
    const chunkOps = new Map(); // chunkKey -> {cx, cy, upserts: Map<localIdx, colorId>, deletes: Set<localIdx>}

    for (const [key, color] of upserts) {
      const [x, y] = key.split(",").map(Number);
      const cx = Math.floor(x / CHUNK_SIZE);
      const cy = Math.floor(y / CHUNK_SIZE);
      const lx = x - cx * CHUNK_SIZE;
      const ly = y - cy * CHUNK_SIZE;
      const localIdx = ChunkCodec.getIndex(lx, ly);
      const ck = `${cx},${cy}`;
      if (!chunkOps.has(ck)) chunkOps.set(ck, { cx, cy, upserts: new Map(), deletes: new Set() });
      chunkOps.get(ck).upserts.set(localIdx, color);
    }
    for (const key of deletes) {
      const [x, y] = key.split(",").map(Number);
      const cx = Math.floor(x / CHUNK_SIZE);
      const cy = Math.floor(y / CHUNK_SIZE);
      const lx = x - cx * CHUNK_SIZE;
      const ly = y - cy * CHUNK_SIZE;
      const localIdx = ChunkCodec.getIndex(lx, ly);
      const ck = `${cx},${cy}`;
      if (!chunkOps.has(ck)) chunkOps.set(ck, { cx, cy, upserts: new Map(), deletes: new Set() });
      chunkOps.get(ck).deletes.add(localIdx);
    }

    // Process each affected chunk: read → decode → apply ops → encode → upsert/delete
    const savePromises = [];
    for (const [ck, ops] of chunkOps) {
      savePromises.push(saveChunkToSupabase(ops.cx, ops.cy, ops.upserts, ops.deletes));
    }

    const results = await Promise.all(savePromises);
    const failedCount = results.filter(r => !r).length;
    if (failedCount > 0) {
      console.error(`[save] ${failedCount}/${chunkOps.size} chunks failed to save, re-queuing...`);
      for (const [ck, ops] of chunkOps) {
        for (const [localIdx, colorId] of ops.upserts) {
          const xy = ChunkCodec.getXY(localIdx);
          const gx = ops.cx * CHUNK_SIZE + xy.x;
          const gy = ops.cy * CHUNK_SIZE + xy.y;
          pendingUpserts.set(`${gx},${gy}`, colorId);
        }
        for (const localIdx of ops.deletes) {
          const xy = ChunkCodec.getXY(localIdx);
          const gx = ops.cx * CHUNK_SIZE + xy.x;
          const gy = ops.cy * CHUNK_SIZE + xy.y;
          pendingDeletes.add(`${gx},${gy}`);
        }
      }
      isSaving = false;
      return false;
    }

    console.log(`[save] ${totalOps} ops saved across ${chunkOps.size} chunks, remaining pending=${pendingUpserts.size + pendingDeletes.size}`);
    isSaving = false;
    return true;
  } catch (e) {
    console.error("[save] Supabase error:", e.message);
    // Re-queue all operations on exception
    for (const [key, color] of upserts) pendingUpserts.set(key, color);
    for (const key of deletes) pendingDeletes.add(key);
    isSaving = false;
    return false;
  }
}

async function saveChunkToSupabase(cx, cy, upserts, deletes) {
  const client = pgPool ? await pgPool.connect() : null;
  try {
    // Read existing chunk data (if any)
    let pixels = [];
    let existing;

    if (supabase && !usePgFallback) {
      const result = await supabase
        .from(CHUNK_TABLE)
        .select("data")
        .eq("chunk_x", cx)
        .eq("chunk_y", cy)
        .single();
      if (result.error && result.error.code !== "PGRST116") {
        console.error(`[save] chunk ${cx},${cy} read error:`, result.error.message);
        return false;
      }
      existing = result.data;
    } else if (pgPool) {
      const result = await client.query(
        `SELECT data FROM ${CHUNK_TABLE} WHERE chunk_x = $1 AND chunk_y = $2`,
        [cx, cy]
      );
      existing = result.rows[0];
    } else {
      return false;
    }

    if (existing && existing.data) {
      const buf = bufferToUint8Array(existing.data);
      pixels = ChunkCodec.decode(buf);
    }

    // Apply deletions
    if (deletes.size > 0) {
      pixels = pixels.filter(p => {
        const idx = ChunkCodec.getIndex(p.x, p.y);
        return !deletes.has(idx);
      });
    }

    // Apply upserts (color changes — add or replace)
    if (upserts.size > 0) {
      const pixelMap = new Map();
      for (const p of pixels) {
        pixelMap.set(ChunkCodec.getIndex(p.x, p.y), { x: p.x, y: p.y, colorId: p.colorId });
      }
      for (const [localIdx, colorId] of upserts) {
        const xy = ChunkCodec.getXY(localIdx);
        pixelMap.set(localIdx, { x: xy.x, y: xy.y, colorId });
      }
      pixels = Array.from(pixelMap.values());
    }

    // Encode and save
    if (pixels.length === 0) {
      // Delete the empty chunk
      if (supabase) {
        const { error: delErr } = await supabase
          .from(CHUNK_TABLE)
          .delete()
          .eq("chunk_x", cx)
          .eq("chunk_y", cy);
        if (delErr) {
          console.error(`[save] chunk ${cx},${cy} delete error:`, delErr.message);
          return false;
        }
      } else if (pgPool) {
        await client.query(
          `DELETE FROM ${CHUNK_TABLE} WHERE chunk_x = $1 AND chunk_y = $2`,
          [cx, cy]
        );
      }
      // Delta: record deletions as eraser (255) for overlay correction
      await recordDeltas(cx, cy, deletes, upserts, true, client);
    } else {
      const encoded = ChunkCodec.encode(pixels);
      if (supabase) {
        const hexData = bufferToHex(encoded);
        const { error: upsertErr } = await supabase
          .from(CHUNK_TABLE)
          .upsert({
            chunk_x: cx,
            chunk_y: cy,
            data: hexData,
            pixel_count: pixels.length,
            updated_at: new Date().toISOString(),
          }, { onConflict: ["chunk_x", "chunk_y"] });
        if (upsertErr) {
          console.error(`[save] chunk ${cx},${cy} upsert error:`, upsertErr.message);
          return false;
        }
        // Also ensure updated_at is bumped for png dirty detection (supabase path does it)
      } else if (pgPool) {
        const buf = bufferToHex(encoded);
        await client.query(
          `INSERT INTO ${CHUNK_TABLE} (chunk_x, chunk_y, data, pixel_count, updated_at) VALUES ($1, $2, $3, $4, NOW())
           ON CONFLICT (chunk_x, chunk_y) DO UPDATE SET data = $3, pixel_count = $4, updated_at = NOW()`,
          [cx, cy, buf, pixels.length]
        );
      }
      // Delta: record upserts + deletes
      await recordDeltas(cx, cy, deletes, upserts, false, client);
    }

    return true;
  } catch (e) {
    console.error(`[save] chunk ${cx},${cy} exception:`, e.message);
    return false;
  } finally {
    if (client) client.release();
  }
}

// Record delta rows for PNG overlay (best-effort, failures don't fail the chunk save)
async function recordDeltas(cx, cy, deletes, upserts, isDeleteAll, pgClient) {
  try {
    const rows = [];
    if (deletes && deletes.size > 0) {
      for (const localIdx of deletes) {
        const xy = ChunkCodec.getXY(localIdx);
        const gx = cx * CHUNK_SIZE + xy.x;
        const gy = cy * CHUNK_SIZE + xy.y;
        rows.push({ chunk_x: cx, chunk_y: cy, x: gx, y: gy, color: 255 });
      }
    }
    if (upserts && upserts.size > 0) {
      for (const [localIdx, colorId] of upserts) {
        const xy = ChunkCodec.getXY(localIdx);
        const gx = cx * CHUNK_SIZE + xy.x;
        const gy = cy * CHUNK_SIZE + xy.y;
        rows.push({ chunk_x: cx, chunk_y: cy, x: gx, y: gy, color: colorId });
      }
    }
    if (rows.length === 0) return;

    // Prefer pgPool direct insert (works even when supabase JS is primary)
    if (pgPool) {
      const client = pgClient || await pgPool.connect();
      const needRelease = !pgClient;
      try {
        // Batch insert 500 rows at a time
        for (let i = 0; i < rows.length; i += 500) {
          const batch = rows.slice(i, i + 500);
          const values = [];
          const params = [];
          let idx = 1;
          for (const r of batch) {
            values.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
            params.push(r.chunk_x, r.chunk_y, r.x, r.y, r.color);
          }
          await client.query(
            `INSERT INTO ${DELTA_TABLE} (chunk_x, chunk_y, x, y, color) VALUES ${values.join(",")}`,
            params
          );
        }
      } finally {
        if (needRelease) client.release();
      }
    } else if (supabase) {
      // Fallback via Supabase JS
      for (let i = 0; i < rows.length; i += 500) {
        const batch = rows.slice(i, i + 500);
        const { error } = await supabase.from(DELTA_TABLE).insert(batch);
        if (error) {
          console.warn(`[delta] insert failed ${cx},${cy}:`, error.message);
          break;
        }
      }
    }
  } catch (e) {
    console.warn(`[delta] record failed ${cx},${cy}:`, e.message);
  }
}

async function saveCanvas() {
  if (supabase || pgPool) {
    if (pendingUpserts.size === 0 && pendingDeletes.size === 0) return;
    try {
      await saveToSupabase();
    } catch (e) {
      console.error("[save] failed:", e.message);
    }
  } else {
    if (!dirty) return;
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
const saveInterval = setInterval(saveCanvas, SAVE_INTERVAL_MS);

// ---- PNG cron (60s, dirty chunks only) ----
let pngCronInterval = null;
let pngCronRunning = false;
async function runPngCron() {
  if (pngCronRunning) return;
  if (!pgPool) return;
  // Skip if DB not ready (migrations not yet applied)
  pngCronRunning = true;
  try {
    if (pngWorker && typeof pngWorker.processDirtyChunks === "function") {
      const res = await pngWorker.processDirtyChunks(pgPool, supabase, { limit: PNG_MAX_PER_TICK });
      if (res && (res.processed > 0 || res.errors > 0)) {
        console.log(`[png-cron] processed=${res.processed} errors=${res.errors}`);
      }
    }
  } catch (e) {
    console.error("[png-cron] error:", e.message);
  } finally {
    pngCronRunning = false;
  }
}
if (pgPool || supabase) {
  pngCronInterval = setInterval(runPngCron, PNG_CRON_MS);
  // 初回は起動30秒後に実行（DB負荷を避ける）
  setTimeout(runPngCron, 30000);
  console.log(`[png-cron] enabled every ${PNG_CRON_MS/1000}s (max ${PNG_MAX_PER_TICK}/tick, dirty-only)`);
} else {
  console.log("[png-cron] disabled (no DB)");
}

// Graceful shutdown: wait for in-progress save, then flush remaining pending changes
async function gracefulShutdown(signal) {
  console.log(`\n[${signal}] saving...`);
  clearInterval(saveInterval);
  if (pngCronInterval) clearInterval(pngCronInterval);
  // Wait for any in-progress Supabase save to finish (Render.com gives ~30s for SIGTERM)
  const startTime = Date.now();
  while (isSaving && Date.now() - startTime < 25000) {
    await new Promise(r => setTimeout(r, 200));
  }
  if (isSaving) {
    console.error(`[${signal}] save timed out, may exit with unsaved changes`);
  }
  // Flush any pending changes accumulated since last interval save
  await saveCanvas();
  console.log(`[${signal}] save complete, exiting`);
  process.exit(0);
}
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// ---- Health ----
app.get("/health", (req, res) => res.json({ ok: true, ready, pixels: canvasData.size, storage: (supabase || pgPool) ? "supabase" : "file", usePg: usePgFallback, pngCron: !!(pgPool || supabase), error: loadError || undefined }));

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
  // Always read from in-memory canvasData — this is the source of truth.
  // Supabase is only used for startup load + periodic persistence saves.
  // Reading from the DB here would return stale data (up to SAVE_INTERVAL_MS behind).
  res.set("Cache-Control", "no-cache");
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
});

// ---- Chunk API ----
// GET /api/chunks?cx=&cy=  returns binary chunk data (Phase4: ETag + Cache-Control)
app.get("/api/chunks", async (req, res) => {
  if (!supabase && !pgPool) {
    return res.status(503).json({ error: "no db" });
  }
  const cx = parseInt(req.query.cx, 10);
  const cy = parseInt(req.query.cy, 10);
  if (!Number.isInteger(cx) || !Number.isInteger(cy)) {
    return res.status(400).json({ error: "cx,cy required as integers" });
  }
  let data;
  let pixelCount = 0;
  let updatedAt = null;
  try {
    if (supabase && !usePgFallback) {
      const result = await supabase
        .from(CHUNK_TABLE)
        .select("data,pixel_count,updated_at")
        .eq("chunk_x", cx)
        .eq("chunk_y", cy)
        .single();
      if (result.error) {
        if (result.error.code === "PGRST116") {
          const emptyTag = `W/"${cx}-${cy}-empty"`;
          res.set("Cache-Control", "public, max-age=10, stale-while-revalidate=30");
          res.set("ETag", emptyTag);
          const inmEmpty2 = req.headers["if-none-match"];
          if (inmEmpty2 && (inmEmpty2 === emptyTag || inmEmpty2.split(",").map(s=>s.trim()).includes(emptyTag))) return res.status(304).end();
          return res.status(404).json({ error: "no chunk" });
        }
        return res.status(500).json({ error: result.error.message });
      }
      data = result.data;
      pixelCount = result.data.pixel_count || 0;
      updatedAt = result.data.updated_at;
    } else if (pgPool) {
      const pgClient = await pgPool.connect();
      try {
        const result = await pgClient.query(
          `SELECT data, pixel_count, updated_at FROM ${CHUNK_TABLE} WHERE chunk_x = $1 AND chunk_y = $2`,
          [cx, cy]
        );
        if (result.rows.length === 0) {
          const emptyTag = `W/"${cx}-${cy}-empty"`;
          res.set("Cache-Control", "public, max-age=10, stale-while-revalidate=30");
          res.set("ETag", emptyTag);
          const inmEmpty = req.headers["if-none-match"];
          if (inmEmpty && (inmEmpty === emptyTag || inmEmpty.split(",").map(s=>s.trim()).includes(emptyTag))) return res.status(304).end();
          return res.status(404).json({ error: "no chunk" });
        }
        data = result.rows[0];
        pixelCount = data.pixel_count || 0;
        updatedAt = data.updated_at;
      } finally {
        pgClient.release();
      }
    }
    const buf = bufferToUint8Array(data.data);
    const etag = `W/"${cx}-${cy}-${pixelCount}-${buf.length}-${updatedAt ? new Date(updatedAt).getTime() : 0}"`;
    const inm = req.headers["if-none-match"];
    if (inm && (inm === etag || inm.split(",").map(s=>s.trim()).includes(etag) || inm.includes(etag))) {
      res.set("ETag", etag);
      res.set("Cache-Control", "public, max-age=30, stale-while-revalidate=60");
      return res.status(304).end();
    }
    res.set("Content-Type", "application/octet-stream");
    res.set("Cache-Control", "public, max-age=30, stale-while-revalidate=60");
    res.set("ETag", etag);
    return res.send(Buffer.from(buf));
  } catch (e) {
    console.error("[api/chunks] error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---- PNG proxy endpoint (Supabase Storage) ----
// GET /api/chunks/:cx/:cy/png  256x256 PNG
app.get("/api/chunks/:cx/:cy/png", async (req, res) => {
  const cx = parseInt(req.params.cx, 10);
  const cy = parseInt(req.params.cy, 10);
  if (!Number.isInteger(cx) || !Number.isInteger(cy)) {
    return res.status(400).json({ error: "cx,cy must be integers" });
  }
  try {
    let pngMeta = null;
    if (pgPool) {
      const c = await pgPool.connect();
      try {
        const r = await c.query(
          `SELECT png_etag, png_generated_at, png_storage_path FROM ${CHUNK_TABLE} WHERE chunk_x=$1 AND chunk_y=$2`,
          [cx, cy]
        );
        pngMeta = r.rows[0] || null;
      } finally { c.release(); }
    } else if (supabase) {
      const { data, error } = await supabase.from(CHUNK_TABLE).select("png_etag,png_generated_at,png_storage_path").eq("chunk_x", cx).eq("chunk_y", cy).single();
      if (!error) pngMeta = data;
    }

    if (!pngMeta || !pngMeta.png_storage_path) {
      // PNG未生成: オンデマンド生成フォールバック（白背景256x256）
      if (!pngWorker) return res.status(503).json({ error: "png worker not available" });
      let pixels = [];
      let needGenerate = false;
      let updatedAtIso = null;
      if (pgPool) {
        const c2 = await pgPool.connect();
        try {
          const r2 = await c2.query(`SELECT data, updated_at, png_generated_at FROM ${CHUNK_TABLE} WHERE chunk_x=$1 AND chunk_y=$2`, [cx, cy]);
          if (r2.rows[0] && r2.rows[0].data) {
            pixels = ChunkCodec.decode(bufferToUint8Array(r2.rows[0].data));
            needGenerate = true;
            updatedAtIso = r2.rows[0].png_generated_at || r2.rows[0].updated_at;
          }
        } finally { c2.release(); }
      } else if (supabase) {
        const { data: ch } = await supabase.from(CHUNK_TABLE).select("data,updated_at,png_generated_at").eq("chunk_x", cx).eq("chunk_y", cy).single();
        if (ch && ch.data) {
          pixels = ChunkCodec.decode(bufferToUint8Array(ch.data));
          needGenerate = true;
          updatedAtIso = ch.png_generated_at || ch.updated_at;
        }
      }
      if (!needGenerate) return res.status(404).json({ error: "no chunk" });
      const pngBuf = await pngWorker.renderChunkToPng(pixels, CHUNK_SIZE);
      const etag = `W/"${cx}-${cy}-${pngBuf.length}-${Date.now()}"`;
      if (req.headers["if-none-match"] === etag) return res.status(304).end();
      // オンデマンドでもLast-Modifiedとサーバ時刻ヘッダを必ず付与してsince補正を可能にする
      const genAt = updatedAtIso ? new Date(updatedAtIso) : new Date();
      res.set("ETag", etag);
      res.set("Cache-Control", "public, max-age=30, stale-while-revalidate=60");
      res.set("Content-Type", "image/png");
      res.set("Last-Modified", genAt.toUTCString());
      res.set("X-PNG-Generated-At", genAt.toISOString());
      return res.send(pngBuf);
    }

    const etag = pngMeta.png_etag || `W/"${cx}-${cy}-${new Date(pngMeta.png_generated_at).getTime()}"`;
    if (req.headers["if-none-match"] && req.headers["if-none-match"] === etag) {
      res.set("ETag", etag);
      res.set("Cache-Control", "public, max-age=60, stale-while-revalidate=120");
      return res.status(304).end();
    }

    if (!supabase) return res.status(503).json({ error: "storage not configured" });
    const storagePath = pngMeta.png_storage_path;
    const { data: blob, error: dlErr } = await supabase.storage.from(PNG_BUCKET).download(storagePath);
    if (dlErr) {
      console.error(`[png] download failed ${storagePath}:`, dlErr.message);
      return res.status(502).json({ error: "storage download failed", detail: dlErr.message });
    }
    const arrayBuf = await blob.arrayBuffer();
    res.set("ETag", etag);
    res.set("Cache-Control", "public, max-age=60, stale-while-revalidate=120");
    res.set("Content-Type", "image/png");
    if (pngMeta.png_generated_at) {
      res.set("Last-Modified", new Date(pngMeta.png_generated_at).toUTCString());
      res.set("X-PNG-Generated-At", new Date(pngMeta.png_generated_at).toISOString());
    }
    return res.send(Buffer.from(arrayBuf));
  } catch (e) {
    console.error("[png] error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---- Delta endpoint ----
// GET /api/delta?cx=0&cy=0&since=2026-09-01T00:00:00.000Z  (alias: /api/deltas)
// or  GET /api/delta?chunks=0:0,1:0&since=...
async function handleDelta(req, res) {
  try {
    const sinceRaw = req.query.since;
    let since = null;
    if (sinceRaw) {
      since = new Date(sinceRaw);
      if (isNaN(since.getTime())) return res.status(400).json({ error: "invalid since" });
    } else {
      since = new Date(0);
    }

    let chunks = [];
    if (req.query.chunks) {
      chunks = String(req.query.chunks).split(",").map(s => {
        const [a,b] = s.split(":").map(Number);
        return [a,b];
      }).filter(([a,b]) => Number.isInteger(a) && Number.isInteger(b));
    } else if (req.query.cx !== undefined && req.query.cy !== undefined) {
      const cx = parseInt(req.query.cx, 10), cy = parseInt(req.query.cy, 10);
      if (Number.isInteger(cx) && Number.isInteger(cy)) chunks = [[cx,cy]];
    }
    if (chunks.length === 0) return res.status(400).json({ error: "chunks or cx,cy required" });
    if (chunks.length > 25) return res.status(400).json({ error: "too many chunks (max 25)" });

    const MAX_DELTAS = 10000;
    let out = [];
    let truncated = false;

    if (pgPool) {
      const client = await pgPool.connect();
      try {
        for (const [cx,cy] of chunks) {
          const r = await client.query(
            `SELECT x,y,color,created_at FROM ${DELTA_TABLE}
             WHERE chunk_x=$1 AND chunk_y=$2 AND created_at >= $3
             ORDER BY created_at ASC LIMIT $4`,
            [cx, cy, since.toISOString(), MAX_DELTAS - out.length]
          );
          for (const row of r.rows) out.push({ x: row.x, y: row.y, color: row.color, created_at: row.created_at });
          if (out.length >= MAX_DELTAS) { truncated = true; break; }
        }
      } finally { client.release(); }
    } else if (supabase) {
      for (const [cx,cy] of chunks) {
        const { data, error } = await supabase.from(DELTA_TABLE)
          .select("x,y,color,created_at")
          .eq("chunk_x", cx).eq("chunk_y", cy).gte("created_at", since.toISOString())
          .order("created_at", { ascending: true }).limit(MAX_DELTAS - out.length);
        if (error) return res.status(500).json({ error: error.message });
        if (data) for (const row of data) out.push({ x: row.x, y: row.y, color: row.color, created_at: row.created_at });
        if (out.length >= MAX_DELTAS) { truncated = true; break; }
      }
    } else {
      return res.status(503).json({ error: "no db" });
    }

    res.set("Cache-Control", "no-cache");
    return res.json({ deltas: out, truncated, serverTime: new Date().toISOString() });
  } catch (e) {
    console.error("[delta] error:", e.message);
    res.status(500).json({ error: e.message });
  }
}
app.get("/api/delta", handleDelta);
app.get("/api/deltas", handleDelta);

// ---- PNG meta (debug) ----
app.get("/api/png-meta", async (req, res) => {
  try {
    if (!pgPool) return res.json({ enabled: false, reason: "no pgPool" });
    const c = await pgPool.connect();
    try {
      const total = await c.query(`SELECT count(*)::int AS c FROM ${CHUNK_TABLE}`);
      const withPng = await c.query(`SELECT count(*)::int AS c FROM ${CHUNK_TABLE} WHERE png_generated_at IS NOT NULL`);
      const dirty = await c.query(`SELECT count(*)::int AS c FROM ${CHUNK_TABLE} WHERE png_generated_at IS NULL OR updated_at > png_generated_at`);
      const deltas = await c.query(`SELECT count(*)::int AS c FROM ${DELTA_TABLE} WHERE created_at > NOW() - INTERVAL '7 days'`);
      res.json({
        enabled: true,
        bucket: PNG_BUCKET,
        cronMs: PNG_CRON_MS,
        maxPerTick: PNG_MAX_PER_TICK,
        chunks: { total: total.rows[0].c, withPng: withPng.rows[0].c, dirty: dirty.rows[0].c },
        deltas7d: deltas.rows[0].c,
      });
    } finally { c.release(); }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---- Meta endpoint ----
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
    if (filePath.endsWith("index.html") || filePath.endsWith("manifest.json") || filePath.endsWith("sw.js")) {
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    }
    if (filePath.endsWith("sw.js")) {
      res.setHeader("Service-Worker-Allowed", "/");
    }
  }
}));

// Serve shared/ (chunk codec) for client-side use
app.use("/shared", express.static(path.join(__dirname, "shared")));

function isValidDraw(payload) {
  if (!payload || typeof payload !== "object") return false;
  const { x, y, color } = payload;
  if (!Number.isInteger(x) || !Number.isInteger(y)) return false;
  if (Math.abs(x) > 1_000_000 || Math.abs(y) > 1_000_000) return false;
  if (typeof color === "number") {
    if (color === ERASER_CODE || VALID_CODES.has(color)) return true;
    return false;
  }
  // Legacy hex-string support (backward compat)
  if (typeof color === "string" && /^#[0-9A-Fa-f]{6}$/.test(color)) return true;
  return false;
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
const BATCH_LIMIT = 80; // pen drag emits many small batches; allow 80Hz smooth stroke without "1秒に3回" false positive (120Hz screenでも余裕)
const UNDO_LIMIT = 10;
const UNDO_WINDOW_MS = 1000;
const socketDrawTimes = new Map(); // socket.id -> number[] for single draw
const socketBatchTimes = new Map(); // socket.id -> number[] for batch
const socketUndoTimes = new Map(); // socket.id -> number[] for undo/redo (separate bucket)
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
  // large batch (rect/circle/line/fill commit) -> strict 3/sec, small pen drag -> 80/sec
  // ペンは一筆で数十回の小バッチを送るため、10px閾値だと高速ドラッグで大バッチ扱いになり
  // 1秒3回制限が一筆中に誤発動していた。閾値を30にしてペン中間バッチは小バッチ扱いにする
  const isLargeBatch = payloadLen > 30;
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
function checkUndoRateLimit(socket){
  const now = Date.now();
  let times = socketUndoTimes.get(socket.id) || [];
  times = times.filter(t => now - t < UNDO_WINDOW_MS);
  if(times.length >= UNDO_LIMIT) {
    socketUndoTimes.set(socket.id, times);
    return false;
  }
  times.push(now);
  socketUndoTimes.set(socket.id, times);
  return true;
}
function getUndoRetryAfterMs(socket){
  const times = socketUndoTimes.get(socket.id) || [];
  if(times.length < UNDO_LIMIT) return 0;
  const oldest = times[0];
  return Math.max(0, UNDO_WINDOW_MS - (Date.now() - oldest));
}
function getDrawRetryAfterMs(socket, isLargeBatch){
  if(isLargeBatch){
    const times = socketDrawTimes.get(socket.id) || [];
    if(times.length < DRAW_LIMIT) return 0;
    return Math.max(0, DRAW_WINDOW_MS - (Date.now() - times[0]));
  } else {
    const times = socketBatchTimes.get(socket.id) || [];
    if(times.length < BATCH_LIMIT) return 0;
    return Math.max(0, DRAW_WINDOW_MS - (Date.now() - times[0]));
  }
}

// ---- Socket.io ----
io.on("connection", (socket) => {
  console.log(`[connect] ${socket.id} connected, total=${io.engine.clientsCount}`);

  // New protocol: viewport-based. Keep legacy init only for ?legacy=1 clients
  const isLegacy = socket.handshake.query.legacy === "1" || socket.handshake.query.legacy === "true";
  if (isLegacy) {
    socket.emit("init", Object.fromEntries(canvasData)); // codes (numbers) now
  } else {
    socket.emit("ready", { pixels: canvasData.size, ready });
    // also send userCount
  }
  io.emit("userCount", io.engine.clientsCount);

  socket.on("draw", (data, ack) => {
    if (!isValidDraw(data)) {
      if (typeof ack === "function") ack({ ok: false, reason: "invalid" });
      return;
    }
    if (!checkRateLimit(socket)) {
      if (typeof ack === "function") ack({ ok: false, reason: "rateLimited", retryAfterMs: Math.max(0, DRAW_WINDOW_MS - (Date.now() - (socketDrawTimes.get(socket.id)||[Date.now()])[0])) });
      return;
    }
    const { x, y, color } = data;
    const code = normalizeColor(color);
    if (code === undefined) {
      if (typeof ack === "function") ack({ ok: false, reason: "invalid" });
      return;
    }
    const key = `${x},${y}`;
    if (code === ERASER_CODE) {
      if (canvasData.has(key)) canvasData.delete(key);
      pendingUpserts.delete(key);
      pendingDeletes.add(key);
      if (!supabase && !pgPool) dirty = true;
    } else {
      canvasData.set(key, code);
      pendingDeletes.delete(key);
      pendingUpserts.set(key, code);
      if (!supabase && !pgPool) dirty = true;
    }
    io.emit("draw", { x, y, color: code, totalPixels: canvasData.size });
    if (typeof ack === "function") ack({ ok: true });
  });

  socket.on("drawBatch", (data, ack) => {
    if (!isValidDrawBatch(data)) {
      if (typeof ack === "function") ack({ ok: false, reason: "invalid" });
      return;
    }
    if (!checkBatchRateLimit(socket, data.length)) {
      const isLarge = data.length > 10;
      if (typeof ack === "function") ack({ ok: false, reason: "rateLimited", retryAfterMs: getDrawRetryAfterMs(socket, isLarge) });
      return;
    }
    const normalizedBatch = [];
    for (const { x, y, color } of data) {
      const code = normalizeColor(color);
      if (code === undefined) continue;
      const key = `${x},${y}`;
      if (code === ERASER_CODE) {
        if (canvasData.has(key)) canvasData.delete(key);
        pendingUpserts.delete(key);
        pendingDeletes.add(key);
      } else {
        canvasData.set(key, code);
        pendingDeletes.delete(key);
        pendingUpserts.set(key, code);
      }
      normalizedBatch.push({ x, y, color: code });
    }
    if (!supabase && !pgPool) dirty = true;
    io.emit("drawBatch", { pixels: normalizedBatch, totalPixels: canvasData.size });
    if (typeof ack === "function") ack({ ok: true });
  });

  // ---- Undo/Redo: separate rate limit bucket (10/s) + ack ----
  // Client's own undo is ack-waited for consistency; others receive via normal draw/drawBatch broadcast.
  socket.on("undo", (data, ack) => {
    const hasAck = typeof ack === "function";
    // Accept: single {x,y,color} , array [{x,y,color}], or {pixels:[...]}
    let batch = null;
    if (Array.isArray(data)) batch = data;
    else if (data && Array.isArray(data.pixels)) batch = data.pixels;
    else if (data && typeof data === "object" && "x" in data && "y" in data && "color" in data) batch = [data];
    else {
      if (hasAck) ack({ ok: false, reason: "invalid" });
      return;
    }
    if (!isValidDrawBatch(batch)) {
      if (hasAck) ack({ ok: false, reason: "invalid" });
      return;
    }
    if (!checkUndoRateLimit(socket)) {
      if (hasAck) ack({ ok: false, reason: "rateLimited", retryAfterMs: getUndoRetryAfterMs(socket) });
      else socket.emit("rateLimited", { type: "undo", retryAfterMs: getUndoRetryAfterMs(socket) });
      return;
    }
    const normalizedBatch = [];
    for (const { x, y, color } of batch) {
      const code = normalizeColor(color);
      if (code === undefined) continue;
      const key = `${x},${y}`;
      if (code === ERASER_CODE) {
        if (canvasData.has(key)) canvasData.delete(key);
        pendingUpserts.delete(key);
        pendingDeletes.add(key);
      } else {
        canvasData.set(key, code);
        pendingDeletes.delete(key);
        pendingUpserts.set(key, code);
      }
      normalizedBatch.push({ x, y, color: code });
    }
    if (!supabase && !pgPool) dirty = true;
    if (normalizedBatch.length === 1) {
      io.emit("draw", { x: normalizedBatch[0].x, y: normalizedBatch[0].y, color: normalizedBatch[0].color, totalPixels: canvasData.size });
    } else {
      io.emit("drawBatch", { pixels: normalizedBatch, totalPixels: canvasData.size });
    }
    if (hasAck) ack({ ok: true });
  });

  socket.on("disconnect", () => {
    socketDrawTimes.delete(socket.id);
    socketBatchTimes.delete(socket.id);
    socketUndoTimes.delete(socket.id);
    console.log(`[disconnect] ${socket.id}`);
    io.emit("userCount", io.engine.clientsCount);
  });
});

server.listen(PORT, () => {
  console.log(`epixel server listening on http://localhost:${PORT}`);
});
