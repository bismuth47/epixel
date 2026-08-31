(function (global, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = factory();
  } else {
    global.ChunkCodec = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // ── Constants ──────────────────────────────────────────────────
  var CHUNK_SIZE = 256;
  var PIXELS_PER_CHUNK = CHUNK_SIZE * CHUNK_SIZE; // 65536
  var BITMASK_SIZE = PIXELS_PER_CHUNK / 8; // 8192 bytes

  var FMT_SPARSE = 0;
  var FMT_RLE = 1;
  var FMT_DENSE = 2;

  // Use Dense format only when the chunk has enough pixels to justify
  // the 8KB bitmask overhead.  Break-even vs Sparse is ~3448 pixels.
  var DENSE_MIN_PIXELS = 3500;

  // ── Helpers ────────────────────────────────────────────────────

  function getIndex(x, y) {
    return y * CHUNK_SIZE + x;
  }

  function getXY(index) {
    return { x: index % CHUNK_SIZE, y: (index / CHUNK_SIZE) | 0 };
  }

  function sortPixels(pixels) {
    return pixels.slice().sort(function (a, b) {
      return getIndex(a.x, a.y) - getIndex(b.x, b.y);
    });
  }

  // ── RLE ────────────────────────────────────────────────────────

  function computeRuns(sortedPixels) {
    var runs = [];
    if (sortedPixels.length === 0) return runs;

    var curColor = sortedPixels[0].colorId;
    var curStart = getIndex(sortedPixels[0].x, sortedPixels[0].y);
    var curLen = 1;

    for (var i = 1; i < sortedPixels.length; i++) {
      var idx = getIndex(sortedPixels[i].x, sortedPixels[i].y);
      if (sortedPixels[i].colorId === curColor && idx === curStart + curLen) {
        curLen++;
      } else {
        runs.push({ colorId: curColor, start: curStart, length: curLen });
        curColor = sortedPixels[i].colorId;
        curStart = idx;
        curLen = 1;
      }
    }
    runs.push({ colorId: curColor, start: curStart, length: curLen });
    return runs;
  }

  function estimateRleSize(sortedPixels) {
    var runs = computeRuns(sortedPixels);
    return 3 + runs.length * 5;
  }

  function estimateDenseSize(pixelCount) {
    return 1 + BITMASK_SIZE + Math.ceil((pixelCount * 5) / 8);
  }

  // ── Format 0: Sparse ───────────────────────────────────────────

  function encodeSparse(sortedPixels) {
    var count = sortedPixels.length;
    var buf = new Uint8Array(3 + count * 3);
    buf[0] = FMT_SPARSE;
    buf[1] = (count >>> 8) & 0xff;
    buf[2] = count & 0xff;
    var offset = 3;
    for (var i = 0; i < count; i++) {
      var p = sortedPixels[i];
      var index = getIndex(p.x, p.y);
      buf[offset++] = (index >>> 8) & 0xff;
      buf[offset++] = index & 0xff;
      buf[offset++] = p.colorId;
    }
    return buf;
  }

  function decodeSparse(buf) {
    var count = (buf[1] << 8) | buf[2];
    var pixels = new Array(count);
    var offset = 3;
    for (var i = 0; i < count; i++) {
      var index = (buf[offset] << 8) | buf[offset + 1];
      var colorId = buf[offset + 2];
      var xy = getXY(index);
      pixels[i] = { x: xy.x, y: xy.y, colorId: colorId };
      offset += 3;
    }
    return pixels;
  }

  // ── Format 1: RLE ──────────────────────────────────────────────

  function encodeRle(runs) {
    // Split runs that exceed uint16 max (65535) length
    var expanded = [];
    for (var i = 0; i < runs.length; i++) {
      var r = runs[i];
      while (r.length > 65535) {
        expanded.push({ colorId: r.colorId, start: r.start, length: 65535 });
        r = { colorId: r.colorId, start: r.start + 65535, length: r.length - 65535 };
      }
      expanded.push(r);
    }

    var count = expanded.length;
    if (count > 65535) {
      throw new Error('Too many RLE runs for chunk: ' + count);
    }

    var buf = new Uint8Array(3 + count * 5);
    buf[0] = FMT_RLE;
    buf[1] = (count >>> 8) & 0xff;
    buf[2] = count & 0xff;
    var offset = 3;
    for (var i = 0; i < count; i++) {
      var r = expanded[i];
      buf[offset++] = r.colorId;
      buf[offset++] = (r.start >>> 8) & 0xff;
      buf[offset++] = r.start & 0xff;
      buf[offset++] = (r.length >>> 8) & 0xff;
      buf[offset++] = r.length & 0xff;
    }
    return buf;
  }

  function decodeRle(buf) {
    var count = (buf[1] << 8) | buf[2];
    var pixels = [];
    var offset = 3;
    for (var i = 0; i < count; i++) {
      var colorId = buf[offset];
      var start = (buf[offset + 1] << 8) | buf[offset + 2];
      var length = (buf[offset + 3] << 8) | buf[offset + 4];
      offset += 5;
      for (var j = 0; j < length; j++) {
        var xy = getXY(start + j);
        pixels.push({ x: xy.x, y: xy.y, colorId: colorId });
      }
    }
    return pixels;
  }

  // ── Format 2: Dense (bitmask + 5-bit packed colors) ───────────

  function encodeDense(sortedPixels) {
    var colorByteCount = Math.ceil((sortedPixels.length * 5) / 8);
    var buf = new Uint8Array(1 + BITMASK_SIZE + colorByteCount);
    buf[0] = FMT_DENSE;

    // Build bitmask: 1 bit per pixel, MSB first within each byte
    for (var i = 0; i < sortedPixels.length; i++) {
      var p = sortedPixels[i];
      var index = getIndex(p.x, p.y);
      var byteIdx = index >>> 3;   // index / 8
      var bitIdx = 7 - (index & 7); // MSB first
      buf[1 + byteIdx] |= (1 << bitIdx);
    }

    // Pack 5-bit color IDs sequentially (MSB first within each byte)
    var colorBase = 1 + BITMASK_SIZE;
    var bitPos = 0;
    for (var i = 0; i < sortedPixels.length; i++) {
      var colorId = sortedPixels[i].colorId;
      for (var b = 0; b < 5; b++) {
        var bit = (colorId >>> (4 - b)) & 1;
        var absBit = bitPos + b;
        var byteIdx = colorBase + (absBit >>> 3);
        var bitIdx = 7 - (absBit & 7);
        if (bit) buf[byteIdx] |= (1 << bitIdx);
      }
      bitPos += 5;
    }

    return buf;
  }

  function decodeDense(buf) {
    var pixels = [];
    var colorBase = 1 + BITMASK_SIZE;
    var colorBitPos = 0;

    for (var byteIdx = 0; byteIdx < BITMASK_SIZE; byteIdx++) {
      var byte = buf[1 + byteIdx];
      for (var bit = 7; bit >= 0; bit--) {
        if (byte & (1 << bit)) {
          var index = byteIdx * 8 + (7 - bit);
          var xy = getXY(index);

          // Read 5 bits from color data stream
          var colorId = 0;
          for (var i = 0; i < 5; i++) {
            var absBit = colorBitPos + i;
            var cByteIdx = colorBase + (absBit >>> 3);
            var cBitIdx = 7 - (absBit & 7);
            colorId = (colorId << 1) | ((buf[cByteIdx] >>> cBitIdx) & 1);
          }
          colorBitPos += 5;

          pixels.push({ x: xy.x, y: xy.y, colorId: colorId });
        }
      }
    }
    return pixels;
  }

  // ── Public API ─────────────────────────────────────────────────

  function getFormatName(fmt) {
    switch (fmt) {
      case FMT_SPARSE: return 'SPARSE';
      case FMT_RLE: return 'RLE';
      case FMT_DENSE: return 'DENSE';
      default: return 'UNKNOWN';
    }
  }

  /**
   * Encode pixels into the most compact binary representation.
   * Automatically selects Sparse, RLE, or Dense format based on data.
   *
   * @param {Array<{x:number,y:number,colorId:number}>} pixels
   * @returns {Uint8Array}
   */
  function encode(pixels) {
    if (!pixels || pixels.length === 0) {
      return new Uint8Array([FMT_SPARSE, 0, 0]);
    }

     var sorted = sortPixels(pixels);

    // Deduplicate by index (keep last — latest color wins for overlapping pixels)
    var unique = [];
    var lastIndex = -1;
    for (var i = 0; i < sorted.length; i++) {
      var idx = getIndex(sorted[i].x, sorted[i].y);
      if (idx !== lastIndex) {
        unique.push(sorted[i]);
        lastIndex = idx;
      } else {
        unique[unique.length - 1] = sorted[i];
      }
    }

    // Estimate all formats
    var sparseSize = 3 + unique.length * 3;
    var rleSize = estimateRleSize(unique);
    var denseSize = estimateDenseSize(unique.length);

    // Select smallest (Dense only for high density)
    if (denseSize <= sparseSize && denseSize <= rleSize && unique.length > DENSE_MIN_PIXELS) {
      return encodeDense(unique);
    }
    if (rleSize <= sparseSize) {
      return encodeRle(computeRuns(unique));
    }
    return encodeSparse(unique);
  }

  /**
   * Decode a binary buffer back into pixels.
   *
   * @param {Uint8Array} buf
   * @returns {Array<{x:number,y:number,colorId:number}>}
   */
  function decode(buf) {
    var fmt = buf[0] & 0x03;
    switch (fmt) {
      case FMT_SPARSE: return decodeSparse(buf);
      case FMT_RLE: return decodeRle(buf);
      case FMT_DENSE: return decodeDense(buf);
      default: throw new Error('Unknown chunk format: ' + fmt);
    }
  }

  /**
   * Returns { format, size, pixelCount } for debugging.
   */
  function analyze(pixels) {
    if (!pixels || pixels.length === 0) {
      return { format: 'SPARSE', size: 3, pixelCount: 0 };
    }

    var sorted = sortPixels(pixels);
    var sparseSize = 3 + sorted.length * 3;
    var rleSize = estimateRleSize(sorted);
    var denseSize = estimateDenseSize(sorted.length);

    var bestFormat = 'SPARSE';
    var bestSize = sparseSize;

    if (rleSize < bestSize) {
      bestFormat = 'RLE';
      bestSize = rleSize;
    }
    if (denseSize <= bestSize && sorted.length > DENSE_MIN_PIXELS) {
      bestFormat = 'DENSE';
      bestSize = denseSize;
    }

    return { format: bestFormat, size: bestSize, pixelCount: sorted.length };
  }

  return {
    CHUNK_SIZE: CHUNK_SIZE,
    PIXELS_PER_CHUNK: PIXELS_PER_CHUNK,
    BITMASK_SIZE: BITMASK_SIZE,
    FMT_SPARSE: FMT_SPARSE,
    FMT_RLE: FMT_RLE,
    FMT_DENSE: FMT_DENSE,
    DENSE_MIN_PIXELS: DENSE_MIN_PIXELS,
    getIndex: getIndex,
    getXY: getXY,
    getFormatName: getFormatName,
    encode: encode,
    decode: decode,
    analyze: analyze,
  };
});
