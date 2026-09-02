#!/usr/bin/env node
// migrate-png-and-delta.js
// 実行: node migrate-png-and-delta.js
// 環境変数: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_DB_PASSWORD

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_DB_PASSWORD = process.env.SUPABASE_DB_PASSWORD;

async function getPgPool() {
  if (!SUPABASE_URL || !SUPABASE_DB_PASSWORD) return null;
  const m = SUPABASE_URL.match(/https:\/\/([^.]+)\.supabase\.co/);
  if (!m) return null;
  const projectRef = m[1];
  return new Pool({
    host: 'aws-0-ap-northeast-1.pooler.supabase.com',
    port: 6543,
    database: 'postgres',
    user: `postgres.${projectRef}`,
    password: SUPABASE_DB_PASSWORD,
    ssl: { rejectUnauthorized: false },
  });
}

async function run() {
  const sqlPath = path.join(__dirname, 'sql', '001_png_and_delta.sql');
  const sql = fs.readFileSync(sqlPath, 'utf-8');

  console.log('[migrate] applying 001_png_and_delta.sql ...');

  const pool = await getPgPool();
  if (pool) {
    const client = await pool.connect();
    try {
      // statement_timeout 長め
      await client.query("SET statement_timeout = '120000'");
      await client.query(sql);
      console.log('[migrate] pg pool: SQL applied');
    } catch (e) {
      console.error('[migrate] pg pool failed:', e.message);
      throw e;
    } finally {
      client.release();
      await pool.end();
    }
  } else {
    console.log('[migrate] pg pool not configured, skipping pg path');
  }

  // Storage bucket 作成（Supabase JS経由の冪等作成）
  const key = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
  if (SUPABASE_URL && key) {
    const supabase = createClient(SUPABASE_URL, key);
    console.log('[migrate] ensuring storage bucket chunk-pngs ...');
    const { data: buckets, error: listErr } = await supabase.storage.listBuckets();
    if (listErr) {
      console.warn('[migrate] listBuckets failed:', listErr.message);
    } else {
      const exists = buckets && buckets.some(b => b.name === 'chunk-pngs' || b.id === 'chunk-pngs');
      if (exists) {
        console.log('[migrate] bucket chunk-pngs already exists');
      } else {
        const { error: createErr } = await supabase.storage.createBucket('chunk-pngs', { public: true });
        if (createErr) {
          console.warn('[migrate] createBucket failed (may need SQL):', createErr.message);
        } else {
          console.log('[migrate] bucket chunk-pngs created');
        }
      }
    }
    // verify
    const { data: verifyBuckets } = await supabase.storage.listBuckets();
    console.log('[migrate] buckets:', (verifyBuckets||[]).map(b=>b.name).join(', '));
  } else {
    console.log('[migrate] supabase client not configured, skip bucket check');
  }

  console.log('[migrate] done');
}

run().catch(e => { console.error(e); process.exit(1); });
