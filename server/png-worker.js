const sharp = require("sharp");
const ChunkCodec = require("../shared/chunk-codec");

// 28 colors (index 0-27) → RGB
const CODE_TO_RGB = [
  { r: 0x17, g: 0x51, b: 0x45 }, { r: 0x2e, g: 0x80, b: 0x65 },
  { r: 0x51, g: 0xb3, b: 0x41 }, { r: 0x9b, g: 0xd5, b: 0x47 },
  { r: 0xff, g: 0xf9, b: 0x71 }, { r: 0xff, g: 0x7f, b: 0x4f },
  { r: 0xff, g: 0x4f, b: 0x4f }, { r: 0xee, g: 0x30, b: 0x46 },
  { r: 0xdf, g: 0x42, b: 0x6e }, { r: 0xff, g: 0x88, b: 0xdd },
  { r: 0xa6, g: 0x26, b: 0x54 }, { r: 0x62, g: 0x1b, b: 0x52 },
  { r: 0x2f, g: 0x15, b: 0x4d }, { r: 0x00, g: 0x00, b: 0x00 },
  { r: 0x33, g: 0x33, b: 0x33 }, { r: 0x27, g: 0x25, b: 0x73 },
  { r: 0x48, g: 0x76, b: 0xbb }, { r: 0x7f, g: 0xd3, b: 0xe6 },
  { r: 0xc7, g: 0xf7, b: 0xf2 }, { r: 0xbb, g: 0xbb, b: 0xbb },
  { r: 0x66, g: 0x66, b: 0x66 }, { r: 0xfd, g: 0xcb, b: 0xb0 },
  { r: 0xd2, g: 0x9c, b: 0x8a }, { r: 0x9e, g: 0x4d, b: 0x4d },
  { r: 0x71, g: 0x28, b: 0x35 }, { r: 0x5d, g: 0x18, b: 0x35 },
  { r: 0x35, g: 0x08, b: 0x2a }, { r: 0xff, g: 0xbc, b: 0x60 },
];

const CHUNK_SIZE = ChunkCodec.CHUNK_SIZE; // 256
const BUCKET = "chunk-pngs";
const MAX_CHUNKS_PER_TICK = 20;
const DELTA_TTL_DAYS = 7;

function bufferToUint8Array(data) {
  if (!data) return new Uint8Array(0);
  if (typeof data === "string") {
    if (data.startsWith("\\x")) return Uint8Array.from(Buffer.from(data.slice(2), "hex"));
    return Uint8Array.from(Buffer.from(data, "hex"));
  }
  if (Buffer.isBuffer(data)) return new Uint8Array(data);
  if (data instanceof Uint8Array) return data;
  return new Uint8Array(data);
}

function storagePath(cx, cy) {
  return `${cx}_${cy}.png`;
}

/**
 * 256x256 PNGを生成（白背景）
 * @param {Array<{x:number,y:number,colorId:number}>} pixels - ローカル座標 (0..255)
 * @param {number} size
 * @returns {Promise<Buffer>} PNG buffer
 */
async function renderChunkToPng(pixels, size = CHUNK_SIZE) {
  const buf = Buffer.alloc(size * size * 4);
  // 白背景で初期化
  for (let i = 0; i < size * size; i++) {
    const o = i * 4;
    buf[o] = 255; buf[o + 1] = 255; buf[o + 2] = 255; buf[o + 3] = 255;
  }
  for (const p of pixels) {
    if (p.x < 0 || p.x >= size || p.y < 0 || p.y >= size) continue;
    const rgb = CODE_TO_RGB[p.colorId];
    if (!rgb) continue;
    const o = (p.y * size + p.x) * 4;
    buf[o] = rgb.r; buf[o + 1] = rgb.g; buf[o + 2] = rgb.b; buf[o + 3] = 255;
  }
  return sharp(buf, { raw: { width: size, height: size, channels: 4 } })
    .png({ compressionLevel: 6, palette: false })
    .toBuffer();
}

/**
 * Supabase StorageへPNGをアップロード（upsert）
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {number} cx
 * @param {number} cy
 * @param {Buffer} pngBuf
 * @returns {Promise<{path:string, etag:string}>}
 */
async function uploadPngToStorage(supabase, cx, cy, pngBuf) {
  const path = storagePath(cx, cy);
  const { error } = await supabase.storage.from(BUCKET).upload(path, pngBuf, {
    contentType: "image/png",
    upsert: true,
    cacheControl: "3600",
  });
  if (error) throw new Error(`storage upload ${path} failed: ${error.message}`);
  // ETagは簡易にbase64の先頭やlengthで代用、DB側でmd5を保持する場合は別途
  return { path, etag: `${pngBuf.length}-${Date.now()}` };
}

/**
 * 更新があったチャンクのみをPNG化（Cron本体）
 * - 条件: png_generated_at IS NULL OR updated_at > png_generated_at
 * - Storageへupsert後、canvas_chunksのpng_*を更新
 * - 古いdeltaは削除（TTL）
 * @param {import('pg').Pool} pgPool
 * @param {import('@supabase/supabase-js').SupabaseClient|null} supabase - Storage uploadに使用、nullならDBのみ更新（png_data保存しない運用ならスキップ）
 * @param {object} opts
 * @returns {Promise<{processed:number, skipped:number, errors:number}>}
 */
async function processDirtyChunks(pgPool, supabase, opts = {}) {
  const limit = opts.limit || MAX_CHUNKS_PER_TICK;
  if (!pgPool) {
    console.log("[png-worker] no pgPool, skip");
    return { processed: 0, skipped: 0, errors: 0 };
  }

  let client;
  try {
    client = await pgPool.connect();
  } catch (e) {
    console.error("[png-worker] pg connect failed:", e.message);
    return { processed: 0, skipped: 0, errors: 1 };
  }

  try {
    // 1) dirty chunk抽出（変更がないチャンクはスキップ）
    const { rows } = await client.query(
      `SELECT chunk_x, chunk_y, data, updated_at, png_generated_at
       FROM canvas_chunks
       WHERE png_generated_at IS NULL OR updated_at > png_generated_at
       ORDER BY updated_at ASC
       LIMIT $1`,
      [limit]
    );

    if (rows.length === 0) {
      return { processed: 0, skipped: 0, errors: 0 };
    }

    console.log(`[png-worker] found ${rows.length} dirty chunks`);

    let processed = 0;
    let errors = 0;

    for (const row of rows) {
      const cx = row.chunk_x;
      const cy = row.chunk_y;
      try {
        const pixels = ChunkCodec.decode(bufferToUint8Array(row.data));
        const pngBuf = await renderChunkToPng(pixels, CHUNK_SIZE);

        // Storageへアップロード（supabaseが無ければスキップしてDBのみ更新）
        let storagePathVal = storagePath(cx, cy);
        let etag = `W/"${cx}-${cy}-${pngBuf.length}-${Date.now()}"`;
        if (supabase) {
          try {
            const up = await uploadPngToStorage(supabase, cx, cy, pngBuf);
            storagePathVal = up.path;
            etag = up.etag;
          } catch (e) {
            console.error(`[png-worker] upload failed ${cx},${cy}:`, e.message);
            // Storage失敗は致命とせず再試行させるため continue（png_generated_atは更新しない）
            errors++;
            continue;
          }
        } else {
          console.warn(`[png-worker] supabase not configured, skip storage upload for ${cx},${cy}`);
        }

        // DBのメタ更新
        await client.query(
          `UPDATE canvas_chunks
           SET png_generated_at = NOW(), png_etag = $1, png_storage_path = $2
           WHERE chunk_x = $3 AND chunk_y = $4`,
          [etag, storagePathVal, cx, cy]
        );

        // 古いdelta削除（TTL超え）
        await client.query(
          `DELETE FROM canvas_pixel_deltas
           WHERE chunk_x = $1 AND chunk_y = $2 AND created_at < NOW() - INTERVAL '${DELTA_TTL_DAYS} days'`,
          [cx, cy]
        );

        processed++;
      } catch (e) {
        console.error(`[png-worker] chunk ${cx},${cy} failed:`, e.message);
        errors++;
      }
    }

    console.log(`[png-worker] tick done: processed=${processed} errors=${errors} totalDirty=${rows.length}`);
    return { processed, skipped: 0, errors };
  } finally {
    client.release();
  }
}

module.exports = {
  CHUNK_SIZE,
  BUCKET,
  MAX_CHUNKS_PER_TICK,
  CODE_TO_RGB,
  storagePath,
  bufferToUint8Array,
  renderChunkToPng,
  uploadPngToStorage,
  processDirtyChunks,
};
