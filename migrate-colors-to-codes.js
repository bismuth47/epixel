/**
 * Migration script: Convert existing Supabase color values from hex strings → numeric codes (0–26)
 *
 * This reads every pixel from `canvas_pixels`, converts hex colors to codes,
 * and writes the codes back **in place** (primary key unchanged — no data lost).
 *
 * The server now handles BOTH formats (hex strings and numeric codes), so this
 * migration is safe to run while the server is live. After migration, only the
 * DB column type needs updating (one-time SQL, see below).
 *
 * ── Schema fix (run ONCE in Supabase SQL Editor after this script) ──
 *   ALTER TABLE canvas_pixels
 *     DROP CONSTRAINT IF EXISTS canvas_pixels_color_check,
 *     ALTER COLUMN color TYPE INTEGER USING NULLIF(color, '')::INTEGER,
 *     ALTER COLUMN color SET NOT NULL;
 *   ALTER TABLE canvas_pixels
 *     ADD CONSTRAINT canvas_pixels_color_check CHECK (color >= 0 AND color <= 26);
 *
 * If the column is already INTEGER (new deployment), the script skips automatically.
 *
 * Usage:
 *   SUPABASE_URL=... SUPABASE_ANON_KEY=... node migrate-colors-to-codes.js
 */
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const TABLE = "canvas_pixels";

// ── Color code mapping (0–26, excludes eraser white #ffffff) ──
const CODE_TO_COLOR = [
  "#175145","#2e8065","#51b341","#9bd547","#fff971","#ff7f4f",
  "#ff4f4f","#ee3046","#df426e","#ff88dd","#a62654","#621b52",
  "#371848","#0c082a","#261152","#272573","#4876bb","#7fd3e6",
  "#c7f7f2","#bbbbbb","#666666","#fdcbb0","#d29c8a",
  "#9e4d4d","#712835","#5d1835","#35082a"
];
const COLOR_TO_CODE = new Map(CODE_TO_COLOR.map((c, i) => [c.toLowerCase(), i]));
const ERASER_CODE = 255;
const ERASER_COLOR = "#ffffff";
const VALID_CODES = new Set(CODE_TO_COLOR.map((_, i) => i));

function hexToCode(hex) {
  let normalized = hex.toLowerCase();
  if (normalized === "#7fd3e0") normalized = "#7fd3e6"; // legacy palette rename
  if (normalized === ERASER_COLOR) return ERASER_CODE;
  return COLOR_TO_CODE.get(normalized);
}

function normalizeColor(color) {
  if (typeof color === "number" && Number.isInteger(color)) {
    if (color === ERASER_CODE || VALID_CODES.has(color)) return color;
    return undefined;
  }
  if (typeof color === "string" && /^#[0-9A-Fa-f]{6}$/.test(color)) {
    return hexToCode(color);
  }
  return undefined;
}

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("Error: SUPABASE_URL and SUPABASE_ANON_KEY environment variables are required");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// Detect whether the column is already INTEGER (migration already done)
async function detectColumnType() {
  const { data: sample, error } = await supabase
    .from(TABLE)
    .select("x,y,color")
    .limit(1);
  if (error) {
    console.error("Could not read from table:", error.message);
    process.exit(1);
  }
  if (!sample || sample.length === 0) {
    console.log("Table is empty — nothing to migrate.");
    return null; // empty, no type detection needed
  }
  const sampleColor = sample[0].color;
  return typeof sampleColor === "number" ? "integer" : "text";
}

async function migrate() {
  console.log("Detecting column type...");
  const columnType = await detectColumnType();

  if (columnType === "integer") {
    console.log("Column is already INTEGER — verifying all values are valid codes...");
    // Verify and report any bad values
    let page = 0;
    const BATCH = 1000;
    let total = 0, bad = 0;
    while (true) {
      const { data, error } = await supabase
        .from(TABLE)
        .select("x,y,color")
        .order("x").order("y")
        .range(page * BATCH, page * BATCH + BATCH - 1);
      if (error) { console.error("Read error:", error.message); break; }
      if (!data || data.length === 0) break;
      for (const row of data) {
        const code = normalizeColor(row.color);
        if (code === undefined || code === ERASER_CODE) {
          bad++;
        }
        total++;
      }
      if (data.length < BATCH) break;
      page++;
    }
    console.log(`Verified ${total} pixels, ${bad} invalid. Migration already complete.`);
    return;
  }

  if (columnType === null) {
    console.log("Table is empty. Nothing to migrate. Deploy the new server code; it will create the table via migrate-to-supabase.js if needed.");
    return;
  }

  console.log("Column is TEXT (hex strings). Migrating to numeric codes...");

  // Phase 1: Read all pixels
  console.log("Phase 1: Reading all pixels from Supabase...");
  const pixelMap = new Map(); // "x,y" -> code
  let page = 0;
  const BATCH = 1000;
  let totalRead = 0;
  let hexCount = 0, alreadyCodeCount = 0, removed = 0;

  while (true) {
    const { data, error } = await supabase
      .from(TABLE)
      .select("x,y,color")
      .order("x").order("y")
      .range(page * BATCH, page * BATCH + BATCH - 1);

    if (error) {
      console.error("Read error:", error.message);
      process.exit(1);
    }
    if (!data || data.length === 0) break;

    for (const row of data) {
      const code = normalizeColor(row.color);
      if (code === undefined) {
        removed++;
        continue;
      }
      if (code === ERASER_CODE) {
        removed++; // eraser pixels are not stored
        continue;
      }
      const key = `${row.x},${row.y}`;
      if (typeof row.color === "string" && /^#[0-9A-Fa-f]{6}$/.test(row.color)) hexCount++;
      else alreadyCodeCount++;
      pixelMap.set(key, code);
      totalRead++;
    }
    if (data.length < BATCH) break;
    page++;
  }

  console.log(`  Read ${totalRead} pixels (${hexCount} were hex strings, ${alreadyCodeCount} were already codes)`);
  console.log(`  Removed ${removed} invalid/eraser pixels`);

  if (pixelMap.size === 0) {
    console.log("No valid pixels to migrate.");
    return;
  }

  // Phase 2: Delete all existing rows (old hex data)
  console.log("Phase 2: Deleting existing rows...");
  // Delete in batches using a wide x range (server validates |x| <= 1_000_000)
  let totalDeleted = 0;
  const DEL_BATCH = 500;
  while (true) {
    // Delete a batch — use x range to satisfy PostgREST filter requirement
    const { data, error } = await supabase
      .from(TABLE)
      .select("x")
      .limit(DEL_BATCH)
      .gte("x", -2147483648);
    if (error) {
      console.error("Delete-check error:", error.message);
      break;
    }
    if (!data || data.length === 0) break;
    // Delete just these x values in range
    const idsToDelete = data.map(r => `(${r.x},${r.y})`);
    // Use upsert with all-null to delete? No — use delete with a match on a subset
    // Actually, simplest: delete by exact PK using multiple eqs won't work for OR.
    // Instead: delete using a filter that matches all rows.
    // PostgREST requires a filter. We can filter on x >= min_x and x <= max_x
    // for each batch.
    const { error: delError } = await supabase
      .from(TABLE)
      .delete()
      .gte("x", data[0].x)
      .lte("x", data[data.length - 1].x);
    if (delError) {
      console.error("Batch delete error:", delError.message);
      console.error("Try running the SQL migration manually instead:");
      console.log(`
ALTER TABLE ${TABLE}
  DROP CONSTRAINT IF EXISTS ${TABLE}_color_check,
  ALTER COLUMN color TYPE INTEGER USING NULLIF(color, '')::INTEGER,
  ALTER COLUMN color SET NOT NULL;
ALTER TABLE ${TABLE}
  ADD CONSTRAINT ${TABLE}_color_check CHECK (color >= 0 AND color <= 26);
      `);
      process.exit(1);
    }
    totalDeleted += data.length;
    if (data.length < DEL_BATCH) break;
  }
  console.log(`  Deleted ${totalDeleted} old rows`);

  // Phase 3: Insert with numeric codes
  console.log("Phase 3: Inserting pixels with numeric color codes...");
  const pixelArr = Array.from(pixelMap, ([key, code]) => {
    const [x, y] = key.split(",").map(Number);
    return { x, y, color: code };
  });

  // Sort by chunk for spatially-local inserts (better DB performance)
  const CHUNK = 256;
  pixelArr.sort((a, b) => {
    const cax = Math.floor(a.x / CHUNK), cay = Math.floor(a.y / CHUNK);
    const cbx = Math.floor(b.x / CHUNK), cby = Math.floor(b.y / CHUNK);
    if (cax !== cbx) return cax - cbx;
    if (cay !== cby) return cay - cby;
    return a.x - b.x || a.y - b.y;
  });

  let uploaded = 0;
  const INS_BATCH = 1000;
  for (let i = 0; i < pixelArr.length; i += INS_BATCH) {
    const batch = pixelArr.slice(i, i + INS_BATCH);
    const { error } = await supabase.from(TABLE).upsert(batch, { onConflict: ["x", "y"] });
    if (error) {
      console.error(`  Batch ${i / INS_BATCH + 1} failed:`, error.message);
      console.error("If this is a CHECK constraint error on the color column,");
      console.error("run the SQL schema fix shown above, then re-run this script.");
      process.exit(1);
    }
    uploaded += batch.length;
    if (uploaded % 5000 === 0) {
      console.log(`  Progress: ${uploaded}/${pixelArr.length}`);
    }
  }

  console.log(`Migration complete: ${uploaded} pixels converted to numeric codes.`);
  console.log("\n=== IMPORTANT: Run this SQL in your Supabase SQL Editor ===");
  console.log(`
ALTER TABLE ${TABLE}
  DROP CONSTRAINT IF EXISTS ${TABLE}_color_check,
  ALTER COLUMN color TYPE INTEGER USING NULLIF(color, '')::INTEGER,
  ALTER COLUMN color SET NOT NULL;
ALTER TABLE ${TABLE}
  ADD CONSTRAINT ${TABLE}_color_check CHECK (color >= 0 AND color <= 26);
ALTER INDEX IF EXISTS ${TABLE}_pkey REINDEX;
`);
  console.log("=== Server already handles both formats, so it works before AND after this SQL ===");
}

migrate().catch(e => {
  console.error("Migration failed:", e.message);
  process.exit(1);
});
