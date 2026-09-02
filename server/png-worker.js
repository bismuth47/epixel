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
    // 同刻停滞対策: updated_at == png_generated_at のµs一致でPNGが古いまま停滞するのを防ぐため >= に変更
    // 正常時は png_generated_at > updated_at なので >= でも steady state は dirtyにならない（1回余分に再生成されるのみ）
    const { rows } = await client.query(
      `SELECT chunk_x, chunk_y, data, updated_at, png_generated_at
       FROM canvas_chunks
       WHERE png_generated_at IS NULL OR updated_at >= png_generated_at
       ORDER BY updated_at ASC
       LIMIT $1`,
      [limit]
    );

    if (rows.length === 0) {
      // dirtyが無いときでも、過去のバグで残った stale delta（png_generated_at以前）を掃除
      // 安全性強化: < に変更（= のdeltaは次回ポーリングで取得可能なため残す、PNGに含まれる保証がない）
      try {
        const cleaned = await client.query(
          `DELETE FROM canvas_pixel_deltas d
           USING canvas_chunks c
           WHERE d.chunk_x = c.chunk_x AND d.chunk_y = c.chunk_y
             AND c.png_generated_at IS NOT NULL
             AND d.created_at < c.png_generated_at`
        );
        if (cleaned.rowCount > 0) console.log(`[png-worker] stale delta cleanup: ${cleaned.rowCount} rows`);
      } catch (e) {
        console.warn("[png-worker] stale cleanup failed:", e.message);
      }
      return { processed: 0, skipped: 0, errors: 0 };
    }

    console.log(`[png-worker] found ${rows.length} dirty chunks`);

    let processed = 0;
    let skipped = 0;
    let errors = 0;

    for (const row of rows) {
      const cx = row.chunk_x;
      const cy = row.chunk_y;
      const oldUpdatedAt = row.updated_at;
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

        // DBのメタ更新（RETURNINGで確定時刻を取得してdelta削除に利用）
        // レース対策: 同時書き込みでupdated_atが進んでいたらpng_generated_atを進めない
        // → そのdeltaは次tickでPNGに含めてから削除する（タイムラグ埋めの欠落防止）
        // 安全性強化: トランザクション境界を明確化するため clock_timestamp() を使用し、DELETEは < に変更
        const upd = await client.query(
          `UPDATE canvas_chunks
           SET png_generated_at = clock_timestamp(), png_etag = $1, png_storage_path = $2
           WHERE chunk_x = $3 AND chunk_y = $4 AND updated_at = $5
           RETURNING png_generated_at`,
          [etag, storagePathVal, cx, cy, oldUpdatedAt]
        );
        if (upd.rowCount === 0) {
          console.warn(`[png-worker] skip ${cx},${cy}: concurrent update detected (updated_at changed), retry next tick`);
          skipped++;
          continue;
        }
        const pngAt = upd.rows[0]?.png_generated_at;

        // PNGに吸収されたdeltaを削除（png_generated_at未満のみ削除）
        // 変更: <= から < に変更。= のdeltaは µs一致でPNGに含まれていない可能性があるため残し、
        // クライアントの >= 取得（server.js:901）で次回ポーリングで回収させる
        if (pngAt) {
          await client.query(
            `DELETE FROM canvas_pixel_deltas
             WHERE chunk_x = $1 AND chunk_y = $2 AND created_at < $3`,
            [cx, cy, pngAt]
          );
        }
        // 7日以上前の孤立deltaも念のため掃除（TTL）
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

    // 今回処理した分以外にも残っている stale delta をまとめて掃除（dirtyでないchunkの過去残留分）
    // < に変更（= は次回ポーリングで回収）
    try {
      const cleaned = await client.query(
        `DELETE FROM canvas_pixel_deltas d
         USING canvas_chunks c
         WHERE d.chunk_x = c.chunk_x AND d.chunk_y = c.chunk_y
           AND c.png_generated_at IS NOT NULL
           AND d.created_at < c.png_generated_at`
      );
      if (cleaned.rowCount > 0) console.log(`[png-worker] stale delta cleanup: ${cleaned.rowCount} rows`);
    } catch (e) {
      console.warn("[png-worker] stale cleanup failed:", e.message);
    }

    console.log(`[png-worker] tick done: processed=${processed} skipped=${skipped} errors=${errors} totalDirty=${rows.length}`);
    return { processed, skipped, errors };
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
