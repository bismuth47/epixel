/**
 * docs/png-delta-client-example.js
 * 仕様: ①PNGベース取得 → ②since差分取得 → ③オーバーレイ描画
 * 前提: サーバーが 60s Cronで dirtyのみStorageへPNGを生成（server/png-worker.js）
 *      /api/chunks/:cx/:cy/png  と  /api/delta?cx=&cy=&since=  が稼働
 *
 * このファイルは Vanilla JS / React / TypeScript いずれからも流用可能なロジック例。
 * 既存 public/index.html の fetchTile() を置換するイメージ。
 */

// 既存のChunkCodecを再利用（ブラウザでは <script src="/shared/chunk-codec.js"> で読み込み）
const TILE = 256; // ChunkCodec.CHUNK_SIZE と一致

/**
 * 単一チャンクを PNG + Delta で取得して Canvas へ描画
 * @param {number} cx - chunk x
 * @param {number} cy - chunk y
 * @param {CanvasRenderingContext2D} ctx - 描画先コンテキスト（メインcanvasのctx）
 * @param {number} pixelSize - world pixel size (例: 20)
 * @param {Map<string, Map<string,string>>} tilePixels - 既存の tilePixels 互換Map（差分反映用）
 * @param {(x:number,y:number,color:string)=>void} setPixel - 既存setPixelヘルパ
 */
async function fetchChunkPngWithDelta(cx, cy, ctx, pixelSize, tilePixels, setPixel) {
  const key = `${cx},${cy}`;
  const CODE_TO_COLOR = [
    "#175145","#2e8065","#51b341","#9bd547","#fff971","#ff7f4f",
    "#ff4f4f","#ee3046","#df426e","#ff88dd","#a62654","#621b52",
    "#2f154d","#000000","#333333","#272573","#4876bb","#7fd3e6",
    "#c7f7f2","#bbbbbb","#666666","#fdcbb0","#d29c8a",
    "#9e4d4d","#712835","#5d1835","#35082a","#ffbc60"
  ];

  // ① ベースPNG取得
  const pngUrl = `/api/chunks/${cx}/${cy}/png`;
  let pngTimestamp = null;
  let baseBitmap = null;

  const pngRes = await fetch(pngUrl, { cache: "default" });
  if (pngRes.status === 404) {
    // 空チャンク: 何も描画せず delta のみ後で処理
    pngTimestamp = new Date(0).toISOString();
  } else if (pngRes.ok) {
    // ETag / Last-Modified を差分取得の since に使う
    pngTimestamp = pngRes.headers.get("Last-Modified") || pngRes.headers.get("ETag") || new Date().toISOString();
    // 304 の場合は Service Worker / HTTPキャッシュが既に持っている PNG を使う想定
    // ここでは 200 の場合のみ bitmap 化
    if (pngRes.status === 200) {
      const blob = await pngRes.blob();
      baseBitmap = await createImageBitmap(blob);
      // ベースを一括描画（1 drawImage で 256x256 を配置）
      const worldX = cx * TILE * pixelSize;
      const worldY = cy * TILE * pixelSize;
      ctx.drawImage(baseBitmap, worldX, worldY, TILE * pixelSize, TILE * pixelSize);
    }
  } else {
    console.warn(`[png] fetch failed ${key}:`, pngRes.status);
    return;
  }

  // ② 差分取得（PNG生成時刻以降）
  //    pngTimestamp が ETag 由来の場合、サーバー側で ETag→時刻変換されるが、
  //    正確には Last-Modified を使う。ETagの場合はフォールバックで 0 からの差分を取得しても冪等。
  let since = pngTimestamp;
  try {
    // ETag は日付でないため parse 失敗→ 0 にフォールバック
    const d = new Date(pngTimestamp);
    if (isNaN(d.getTime())) since = new Date(0).toISOString();
    else since = d.toISOString();
  } catch (_) {
    since = new Date(0).toISOString();
  }

  const deltaRes = await fetch(`/api/delta?cx=${cx}&cy=${cy}&since=${encodeURIComponent(since)}`, { cache: "no-store" });
  if (!deltaRes.ok) {
    console.warn(`[delta] fetch failed ${key}:`, deltaRes.status);
    return;
  }
  const { deltas } = await deltaRes.json();
  if (!deltas || deltas.length === 0) return;

  // ③ オーバーレイ: 差分ピクセルだけ上書き
  //    ベースPNGの上に fillRect で 1px ずつ上書き（pixelSize 倍で描画）
  for (const d of deltas) {
    const { x, y, color } = d; // global coords, color 0-27 or 255(eraser)
    if (color === 255) {
      // eraser: 白で上書き or 背景色でクリア
      ctx.fillStyle = "#ffffff";
      // setPixel 側で削除も反映
      if (setPixel) {
        // deletePixel 相当を呼ぶ場合は外部から渡された関数を使う
        // ここでは簡易に tilePixels から削除
      }
    } else {
      ctx.fillStyle = CODE_TO_COLOR[color];
      if (setPixel) setPixel(x, y, CODE_TO_COLOR[color]);
    }
    // global → world 座標変換は不要、既にctxがtranslate/scale済みの前提
    // もし ctx が world 座標系なら:
    ctx.fillRect(x * pixelSize, y * pixelSize, pixelSize, pixelSize);
  }
}

// React hooks 例
/*
function usePngChunk(cx, cy) {
  const canvasRef = useRef(null);
  useEffect(() => {
    const ctx = canvasRef.current.getContext('2d');
    fetchChunkPngWithDelta(cx, cy, ctx, 20, tilePixels, setPixel);
  }, [cx, cy]);
  return canvasRef;
}
*/

// 視点移動時のビューポート取得例
async function fetchViewportPng(viewportChunks, ctx, pixelSize) {
  for (const { cx, cy } of viewportChunks) {
    await fetchChunkPngWithDelta(cx, cy, ctx, pixelSize);
  }
}
