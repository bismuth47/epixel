-- 001: PNG cache (Supabase Storage) + Delta tracking
-- 適用: psql または Supabase SQL Editor で実行
-- 想定DB: PostgreSQL (Supabase)

-- 1) canvas_chunks に PNG 生成管理カラムを追加
ALTER TABLE canvas_chunks
  ADD COLUMN IF NOT EXISTS png_generated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS png_etag TEXT,
  ADD COLUMN IF NOT EXISTS png_storage_path TEXT;

-- 2) 差分テーブル（PNG生成後に吸収されるまでの追記ログ）
CREATE TABLE IF NOT EXISTS canvas_pixel_deltas (
  id BIGSERIAL PRIMARY KEY,
  chunk_x SMALLINT NOT NULL,
  chunk_y SMALLINT NOT NULL,
  x INT NOT NULL,
  y INT NOT NULL,
  color SMALLINT NOT NULL, -- 0-27 or 255(eraser)
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_delta_color CHECK (color BETWEEN 0 AND 27 OR color = 255)
);

-- 3) インデックス
-- Cron: 更新があり PNG 未生成/古いチャンク抽出
CREATE INDEX IF NOT EXISTS idx_chunks_png_dirty
  ON canvas_chunks (updated_at, png_generated_at)
  WHERE png_generated_at IS NULL OR updated_at > png_generated_at;

-- クライアント差分取得: chunk単位 + 時刻範囲
CREATE INDEX IF NOT EXISTS idx_deltas_chunk_time
  ON canvas_pixel_deltas (chunk_x, chunk_y, created_at);

-- 全体掃除・監視用
CREATE INDEX IF NOT EXISTS idx_deltas_created_at
  ON canvas_pixel_deltas (created_at);

-- 4) Storage bucket（Supabase Storage）
-- Supabase ダッシュボード or 下記SQLで作成（storage schema が存在する場合）
INSERT INTO storage.buckets (id, name, public)
VALUES ('chunk-pngs', 'chunk-pngs', true)
ON CONFLICT (id) DO NOTHING;

-- 5) Storage ポリシー（public read, service_role write）
-- 既存ポリシーがある場合はスキップされるように DO ブロックで制御
DO $$
BEGIN
  -- public read
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname='storage' AND tablename='objects' AND policyname='chunk-pngs public read'
  ) THEN
    CREATE POLICY "chunk-pngs public read"
      ON storage.objects FOR SELECT
      USING (bucket_id = 'chunk-pngs');
  END IF;
  -- service_role は storage サービスロールで自動許可されるため追加ポリシー不要だが、
  -- 念のため authenticated からの write を service_role のみにする運用推奨。
  -- Supabase のデフォルトでは service_role は RLS バイパス。
END $$;
