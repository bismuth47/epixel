/**
 * Migration script: canvas.json → Supabase
 *
 * Usage:
 *   node migrate-to-supabase.js
 *
 * Environment variables:
 *   SUPABASE_URL - Your Supabase project URL
 *   SUPABASE_ANON_KEY - Your Supabase anon key
 */

const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const DATA_FILE = path.join(__dirname, "canvas.json");
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const TABLE = "canvas_pixels";

const ERASER_COLOR = "#ffffff";

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("Error: SUPABASE_URL and SUPABASE_ANON_KEY environment variables are required");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function ensureTable() {
  console.log("Checking if table exists...");
  const { error: checkError } = await supabase
    .from(TABLE)
    .select("x,y,color")
    .limit(1);

  if (!checkError) {
    console.log("Table with required columns already exists.");
    return true;
  }

  // Table doesn't exist or missing columns - create it
  if (checkError.message.includes("Could not find the table")) {
    console.log("Creating table...");
    const { error: createError } = await supabase.rpc("ensure_canvas_pixels_table");
    if (createError) {
      console.error("Failed to create table via RPC:", createError.message);
    } else {
      console.log("Table created successfully via RPC.");
      return true;
    }
  }

  // If columns are missing or other issue, the user needs to run SQL manually
  console.error("Could not ensure table exists. Please run this SQL in Supabase SQL Editor:");
  console.log(`
CREATE TABLE IF NOT EXISTS ${TABLE} (
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  color TEXT NOT NULL CHECK (color ~ '^#[0-9A-Fa-f]{6}$'),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (x, y)
);`);
  return false;
}

async function migrate() {
  console.log("Reading canvas.json...");
  if (!fs.existsSync(DATA_FILE)) {
    console.error("No canvas.json found. Nothing to migrate.");
    process.exit(1);
  }

  // Ensure table exists with correct schema
  const tableReady = await ensureTable();
  if (!tableReady) {
    console.log("\nPlease create the table manually in Supabase SQL Editor and re-run this script.");
    process.exit(1);
  }

  const raw = fs.readFileSync(DATA_FILE, "utf-8");
  const obj = JSON.parse(raw);
  const entries = Object.entries(obj);
  console.log(`Found ${entries.length} pixels in canvas.json`);

  console.log("Filtering valid pixels...");
  const validPixels = [];
  let removed = 0;

  for (const [key, color] of entries) {
    if (typeof color !== "string" || !/^#[0-9A-Fa-f]{6}$/.test(color)) {
      removed++;
      continue;
    }

    const normalized = color.toLowerCase();
    if (normalized === ERASER_COLOR) {
      removed++;
      continue;
    }

    const parts = key.split(",");
    const x = parseInt(parts[0], 10);
    const y = parseInt(parts[1], 10);

    if (!Number.isInteger(x) || !Number.isInteger(y)) {
      removed++;
      continue;
    }

    validPixels.push({ x, y, color: normalized });
  }

  console.log(`Valid pixels: ${validPixels.length}, removed: ${removed}`);

  if (validPixels.length === 0) {
    console.log("No valid pixels to migrate.");
    return;
  }

  console.log("Checking and clearing existing data in Supabase...");
  // Check if table has data
  const { data: existingData, error: selectError } = await supabase
    .from(TABLE)
    .select("x,y,color")
    .limit(1000);

  if (selectError) {
    console.error("Failed to check existing data:", selectError.message);
    process.exit(1);
  }

  if (existingData && existingData.length > 0) {
    console.log(`Found ${existingData.length} existing pixels. Upserting will update/replace them.`);
  } else {
    console.log("Table is empty, starting fresh insert...");
  }

  console.log("Uploading pixels to Supabase...");
  const BATCH_SIZE = 1000;
  let uploaded = 0;

  for (let i = 0; i < validPixels.length; i += BATCH_SIZE) {
    const batch = validPixels.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from(TABLE)
      .upsert(batch, { onConflict: ["x", "y"] });

    if (error) {
      console.error(`Batch ${i / BATCH_SIZE + 1} failed:`, error.message);
    } else {
      uploaded += batch.length;
      console.log(`Progress: ${uploaded}/${validPixels.length}`);
    }
  }

  console.log(`Migration complete. ${uploaded} pixels uploaded.`);
}

migrate().catch(e => {
  console.error("Migration failed:", e.message);
  process.exit(1);
});
