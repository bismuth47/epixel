/**
 * Migration script: Convert existing Supabase color values from hex strings → numeric codes (0–26)
 *
 * SAFE approach: reads all data, converts hex→code, saves a local backup
 * (canvas-codes-backup.json), then UPSERTS codes in-place. No rows are
 * deleted — if the CHECK constraint blocks the update, the original hex data
 * remains intact and the script prints SQL for the user to run.
 *
 * The server (newly deployed) handles BOTH formats, so it works before and
 * after this migration.
 *
 * ── Workflow ──
 *   1. Run: node migrate-colors-to-codes.js
 *      - Reads all data, converts hex→code, saves backup, attempts upsert.
 *      - If CHECK constraint blocks: prints SQL, exits (data is safe).
 *   2. Run the printed SQL in Supabase SQL Editor (drop CHECK, TRUNCATE,
 *      change column to INTEGER, add new CHECK).
 *   3. Re-run: node migrate-colors-to-codes.js --restore
 *      - Inserts all codes from the backup into the new INTEGER column.
 *
 * ── Schema SQL (run in Supabase SQL Editor if script reports CHECK error) ──
 *   ALTER TABLE canvas_pixels
 *     DROP CONSTRAINT IF EXISTS canvas_pixels_color_check;
 *   TRUNCATE canvas_pixels;
 *   ALTER TABLE canvas_pixels
 *     ALTER COLUMN color TYPE INTEGER;
 *   ALTER TABLE canvas_pixels
 *     ALTER COLUMN color SET NOT NULL;
 *   ALTER TABLE canvas_pixels
 *     ADD CONSTRAINT canvas_pixels_color_check CHECK (color >= 0 AND color <= 26);
 *
 * Usage:
 *   node migrate-colors-to-codes.js
 *   node migrate-colors-to-codes.js --restore
 */
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TABLE = "canvas_pixels";
const BACKUP_FILE = path.join(__dirname, "canvas-codes-backup.json");

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

const SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) environment variables are required");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const isRestoreMode = process.argv.includes("--restore");

// ── SQL for manual schema change (run in Supabase SQL Editor) ──
// Must TRUNCATE first because hex strings (#175145) can't be cast to INTEGER.
// Data is safe in canvas-codes-backup.json — restore with --restore after this.
const SCHEMA_SQL = `
-- 1. Drop old CHECK constraint (allows any TEXT temporarily)
ALTER TABLE ${TABLE}
  DROP CONSTRAINT IF EXISTS ${TABLE}_color_check;

-- 2. Delete old hex data (backed up in canvas-codes-backup.json)
TRUNCATE ${TABLE};

-- 3. Change column type from TEXT to INTEGER
ALTER TABLE ${TABLE}
  ALTER COLUMN color TYPE INTEGER;
ALTER TABLE ${TABLE}
  ALTER COLUMN color SET NOT NULL;

-- 4. Add new CHECK constraint for codes 0–26
ALTER TABLE ${TABLE}
  ADD CONSTRAINT ${TABLE}_color_check CHECK (color >= 0 AND color <= 26);
`;

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
    console.log("Table is empty — nothing to migrate. Use --restore to insert from backup.");
    return null;
  }
  const sampleColor = sample[0].color;
  return typeof sampleColor === "number" ? "integer" : "text";
}

// ── Phase 1: Read all pixels and convert to codes ──
async function readAndConvert() {
  console.log("Reading all pixels from Supabase...");
  const pixelMap = new Map(); // "x,y" -> code
  let page = 0;
  const BATCH = 1000;
  let totalRead = 0, hexCount = 0, alreadyCodeCount = 0, removed = 0;

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
      if (code === undefined) { removed++; continue; }
      if (code === ERASER_CODE) { removed++; continue; } // eraser never stored
      const key = `${row.x},${row.y}`;
      if (typeof row.color === "string" && /^#[0-9A-Fa-f]{6}$/.test(row.color)) hexCount++;
      else alreadyCodeCount++;
      pixelMap.set(key, code);
      totalRead++;
    }
    if (data.length < BATCH) break;
    page++;
  }

  console.log(`  Read ${totalRead} pixels (${hexCount} hex strings, ${alreadyCodeCount} already codes)`);
  console.log(`  Skipped ${removed} invalid/eraser pixels`);
  return pixelMap;
}

// ── Phase 2: Upsert codes into the table ──
async function upsertCodes(pixelMap) {
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

  console.log(`Upserting ${pixelArr.length} pixels with numeric codes...`);
  let uploaded = 0;
  const UPSERT_BATCH = 1000;

  for (let i = 0; i < pixelArr.length; i += UPSERT_BATCH) {
    const batch = pixelArr.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from(TABLE).upsert(batch, { onConflict: ["x", "y"] });

    if (error) {
      console.error(`  UPSERT FAILED at batch ${i / UPSERT_BATCH + 1}:`, error.message);
      console.error("\n  → The table column has the old CHECK constraint for hex strings.");
      console.error("  → Original data is safe (no rows were modified).");
      console.error("  → Run this SQL in Supabase SQL Editor, then re-run with --restore:\n");
      console.log(SCHEMA_SQL);
      process.exit(1);
    }
    uploaded += batch.length;
    if (uploaded % 5000 === 0 || uploaded === pixelArr.length) {
      console.log(`  Progress: ${uploaded}/${pixelArr.length}`);
    }
  }

  console.log("\nMigration complete: " + uploaded + " pixels now store numeric codes.");
  if (uploaded > 0) {
    console.log("Column may still be TEXT — for optimal storage, run:");
    console.log(SCHEMA_SQL);
    console.log("Then: node migrate-colors-to-codes.js --restore");
  }
  console.log("\n=== Server handles both formats, so it works before AND after SQL ===");
}

// ── Restore from backup file ──
async function restoreFromBackup() {
  if (!fs.existsSync(BACKUP_FILE)) {
    console.error("No backup file found:", BACKUP_FILE);
    console.error("Run the script first (without --restore) to generate a backup.");
    process.exit(1);
  }

  const raw = fs.readFileSync(BACKUP_FILE, "utf-8");
  const pixelArr = JSON.parse(raw);
  console.log(`Restoring ${pixelArr.length} pixels from backup...`);

  let uploaded = 0;
  const BATCH = 1000;
  for (let i = 0; i < pixelArr.length; i += BATCH) {
    const batch = pixelArr.slice(i, i + BATCH);
    const { error } = await supabase.from(TABLE).upsert(batch, { onConflict: ["x", "y"] });
    if (error) {
      console.error(`Batch ${i / BATCH + 1} failed:`, error.message);
      console.error("Run the SQL schema fix first, then re-run with --restore.");
      process.exit(1);
    }
    uploaded += batch.length;
    if (uploaded % 5000 === 0) console.log(`  Progress: ${uploaded}/${pixelArr.length}`);
  }
  console.log(`Restore complete: ${uploaded} pixels inserted with numeric codes.`);
  // Optionally remove backup
  // fs.unlinkSync(BACKUP_FILE);
}

// ── Main ──
async function main() {
  if (isRestoreMode) {
    await restoreFromBackup();
    return;
  }

  console.log("Detecting column type...");
  const columnType = await detectColumnType();

  if (columnType === "integer") {
    console.log("Column is already INTEGER — verifying all values are valid codes...");
    let page = 0, total = 0, bad = 0;
    const BATCH = 1000;
    while (true) {
      const { data, error } = await supabase
        .from(TABLE).select("x,y,color").order("x").order("y")
        .range(page * BATCH, page * BATCH + BATCH - 1);
      if (error) { console.error("Read error:", error.message); break; }
      if (!data || data.length === 0) break;
      for (const row of data) {
        const code = row.color;
        if (typeof code !== "number" || !VALID_CODES.has(code)) bad++;
        total++;
      }
      if (data.length < BATCH) break;
      page++;
    }
    console.log(`Verified ${total} pixels, ${bad} invalid.`);
    if (bad > 0) {
      console.log("Some invalid values found. You may need to investigate.");
    } else {
      console.log("All values are valid numeric codes. Migration complete!");
    }
    return;
  }

  if (columnType === null) {
    console.log("Table is empty. Run 'node migrate-to-supabase.js' first to create table + data.");
    return;
  }

  console.log("Column is TEXT (hex strings). Converting to numeric codes...");

  // Read + convert
  const pixelMap = await readAndConvert();
  if (pixelMap.size === 0) {
    console.log("No valid pixels to migrate.");
    return;
  }

  // Save local backup (just in case)
  const backupArr = Array.from(pixelMap, ([key, code]) => {
    const [x, y] = key.split(",").map(Number);
    return { x, y, color: code };
  });
  fs.writeFileSync(BACKUP_FILE, JSON.stringify(backupArr));
  console.log(`  Backup saved to ${BACKUP_FILE}`);

  // Upsert (safe: old data not deleted if upsert fails)
  await upsertCodes(pixelMap);
}

main().catch(e => {
  console.error("Migration failed:", e.message);
  process.exit(1);
});
