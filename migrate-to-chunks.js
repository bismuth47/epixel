const { Pool } = require('pg');
const ChunkCodec = require('./shared/chunk-codec');

const pool = new Pool({
  host: process.env.DB_HOST || 'aws-0-ap-northeast-1.pooler.supabase.com',
  port: 6543,
  database: 'postgres',
  user: 'postgres.yavyvxzcmvshakcekpnh',
  password: ';v./6S/khRbS',
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  query_timeout: 120000,
});

const CHUNK_SIZE = 256;

async function main() {
  const client = await pool.connect();
  try {
    // Step 1: Create canvas_chunks table
    await client.query(`
      CREATE TABLE IF NOT EXISTS canvas_chunks (
        chunk_x    SMALLINT    NOT NULL,
        chunk_y    SMALLINT    NOT NULL,
        data       BYTEA       NOT NULL,
        pixel_count SMALLINT    NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (chunk_x, chunk_y)
      );
    `);
    console.log('✅ Table canvas_chunks created');

    // Step 2: Read all pixels from canvas_pixels
    console.log('📖 Reading all pixels from canvas_pixels...');
    const { rows } = await client.query('SELECT x, y, color FROM canvas_pixels');
    console.log(`   Read ${rows.length} pixels`);

    // Step 3: Group by chunk
    const chunks = new Map(); // "cx,cy" -> Pixel[]
    for (const row of rows) {
      const cx = Math.floor(row.x / CHUNK_SIZE);
      const cy = Math.floor(row.y / CHUNK_SIZE);
      const key = cx + ',' + cy;
      if (!chunks.has(key)) chunks.set(key, []);
      chunks.get(key).push({
        x: row.x - cx * CHUNK_SIZE,   // local x
        y: row.y - cy * CHUNK_SIZE,   // local y
        colorId: row.color,
      });
    }
    console.log(`📦 Found ${chunks.size} unique chunks`);

    // Step 4: Encode and insert chunks
    let totalBytes = 0;
    let chunkCount = 0;
    for (const [key, pixels] of chunks) {
      const [cx, cy] = key.split(',').map(Number);
      const encoded = ChunkCodec.encode(pixels);
      
      await client.query(
        'INSERT INTO canvas_chunks (chunk_x, chunk_y, data, pixel_count) VALUES ($1, $2, $3, $4)',
        [cx, cy, encoded, pixels.length]
      );
      totalBytes += encoded.length;
      chunkCount++;
    }
    console.log(`✅ Inserted ${chunkCount} chunks, ${totalBytes} bytes total`);
    console.log(`📊 Average: ${(rows.length / chunkCount).toFixed(1)} pixels/chunk, ${(totalBytes / chunkCount).toFixed(1)} bytes/chunk`);
    console.log(`💾 Storage efficiency: ${totalBytes} bytes vs ${rows.length * 3} bytes (raw code) vs ${rows.length * 9} bytes (hex string)`);

    // Step 5: Verify
    const verifyRes = await client.query('SELECT count(*), sum(pixel_count) as total_pixels FROM canvas_chunks');
    console.log(`✅ Verification: ${verifyRes.rows[0].count} chunks, ${verifyRes.rows[0].total_pixels} total pixels`);

    // Step 6: Show format distribution
    const distRes = await client.query('SELECT pixel_count, avg(length(data)) as avg_size FROM canvas_chunks GROUP BY pixel_count ORDER BY pixel_count');
    
    // Show size distribution by format
    const formats = { SPARSE: 0, RLE: 0, DENSE: 0 };
    const fmtSizes = { SPARSE: 0, RLE: 0, DENSE: 0 };
    for (const [key, pixels] of chunks) {
      const enc = ChunkCodec.encode(pixels);
      const fmt = ChunkCodec.getFormatName(enc[0] & 3);
      formats[fmt]++;
      fmtSizes[fmt] += enc.length;
    }
    console.log('📊 Format distribution:');
    for (const [fmt, count] of Object.entries(formats)) {
      if (count > 0) {
        console.log(`   ${fmt}: ${count} chunks, ${(fmtSizes[fmt] / count).toFixed(1)} avg bytes/chunk`);
      }
    }

  } catch (err) {
    console.error('❌ Migration failed:', err.message);
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
