package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;

public final class CmpSanity {
    private CmpSanity() {}

    /* =========================================================
     * Checks e helpers básicos
     * ========================================================= */
    public static void requireState(boolean cond, String msg) {
        if (!cond) throw new IllegalStateException(msg);
    }
    public static void requireArgument(boolean cond, String msg) {
        if (!cond) throw new IllegalArgumentException(msg);
    }

    public static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    public static int bitsToBytesCeil(long bits) {
        if (bits <= 0) return 0;
        long b = (bits + 7) >>> 3;
        return (b > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) b;
    }

    public static long bytesToBits(long bytes) {
        return (bytes <= 0) ? 0L : (bytes << 3);
    }

    /* =========================================================
     * Huffman: validações canônicas (Kraft), unicidade, histogramas
     * ========================================================= */

    /** Verifica o critério de Kraft para comprimentos 1..32 (0 é ignorado). */
    public static boolean kraftOk(int[] lengths) {
        if (lengths == null || lengths.length == 0) return false;
        int[] cnt = new int[33];
        int maxL = 0, nz = 0;
        for (int L : lengths) {
            if (L < 0 || L > 32) return false;
            if (L == 0) continue;
            cnt[L]++; nz++; if (L > maxL) maxL = L;
        }
        if (nz < 1) return false;
        int slots = 1;
        for (int L = 1; L <= maxL; L++) {
            slots = (slots << 1) - cnt[L];
            if (slots < 0) return false;
        }
        return true;
    }

    public static boolean kraftOk(byte[] lengths) {
        if (lengths == null) return false;
        int[] tmp = new int[lengths.length];
        for (int i = 0; i < lengths.length; i++) tmp[i] = lengths[i] & 0xFF;
        return kraftOk(tmp);
    }

    /** Símbolos 0..255 e todos distintos. */
    public static boolean allUnique(int[] symbols) {
        if (symbols == null) return false;
        HashSet<Integer> seen = new HashSet<>(symbols.length * 2);
        for (int s : symbols) {
            if (s < 0 || s > 255) return false;
            if (!seen.add(s)) return false;
        }
        return true;
    }

    /** Histograma de comprimentos (1..maxL). */
    public static int[] histogram(int[] lens, int maxL) {
        int[] h = new int[Math.max(1, maxL + 1)];
        if (lens == null) return h;
        for (int L : lens) if (L >= 0 && L < h.length) h[L]++;
        return h;
    }

    /* =========================================================
     * Hexdump (debug)
     * ========================================================= */
    public static String hexdump(ByteBuffer buf, int offset, int len) {
        if (buf == null) return "<null>";
        ByteBuffer b = buf.duplicate();
        offset = clamp(offset, 0, b.limit());
        len = clamp(len, 0, b.limit() - offset);
        b.position(offset);
        b.limit(offset + len);
        StringBuilder sb = new StringBuilder(len * 3 + (len / 16) * 8);
        int col = 0;
        int addr = offset;
        while (b.hasRemaining()) {
            int v = b.get() & 0xFF;
            if (col == 0) sb.append(String.format("%08X: ", addr));
            sb.append(String.format("%02X ", v));
            addr++; col++;
            if (col == 16) { sb.append('\n'); col = 0; }
        }
        if (col != 0) sb.append('\n');
        return sb.toString();
    }

    /* =========================================================
     * Prefix-probe (diagnóstico leve do bitstream)
     * ========================================================= */

    /** Constrói vetores first/last para códigos canônicos a partir dos comprimentos. */
    public static void buildFirstLast(int[] lengths, int[] firstCode, int[] lastCode) {
        int maxL = 0;
        int[] cnt = new int[33];
        for (int L : lengths) { if (L > 0) { cnt[L]++; if (L > maxL) maxL = L; } }
        int code = 0;
        for (int L = 1; L <= maxL; L++) {
            code = (code + cnt[L - 1]) << 1;
            firstCode[L] = code;
            lastCode[L]  = code + cnt[L] - 1;
        }
    }

    private static int peekMSB(byte[] data, long startBit, int n) {
        int out = 0;
        for (int i = 0; i < n; i++) {
            long bit = startBit + i;
            int b = data[(int) (bit >>> 3)] & 0xFF;
            int sh = 7 - (int) (bit & 7);
            out = (out << 1) | ((b >>> sh) & 1);
        }
        return out;
    }

    private static int peekLSB(byte[] data, long startBit, int n) {
        int out = 0;
        for (int i = 0; i < n; i++) {
            long bit = startBit + i;
            int b = data[(int) (bit >>> 3)] & 0xFF;
            int sh = (int) (bit & 7);
            out |= ((b >>> sh) & 1) << i;
        }
        return out;
    }

    private static boolean hitsPrefix(int prefix, int k, int[] first, int[] last) {
        int upto = Math.min(k, first.length - 1);
        for (int L = 1; L <= upto; L++) {
            int mask = (L == 32) ? -1 : ((1 << L) - 1);
            int val  = prefix & mask;
            int lo = first[L], hi = last[L];
            if (hi >= lo && val >= lo && val <= hi) return true;
        }
        return false;
    }

    /**
     * Teste rápido: verifica se os primeiros bytes de {@code payload} podem ser
     * prefixo de algum código canônico definido por {@code codeLengths}.
     * Retorna true em caso de "cheiro" positivo (diagnóstico, não prova).
     */
    public static boolean prefixLooksLike(ByteBuffer payload, int[] codeLengths, int sampleBytes) {
        if (payload == null || codeLengths == null || codeLengths.length == 0) return false;
        ByteBuffer p = payload.duplicate();
        int take = Math.min(Math.max(8, sampleBytes), Math.min(128, p.remaining()));
        if (take <= 0) return false;

        byte[] data = new byte[take];
        p.get(data);

        int maxL = 0;
        for (int L : codeLengths) if (L > maxL) maxL = L;
        int k = Math.min(24, Math.max(2, maxL));

        int[] first = new int[33], last = new int[33];
        buildFirstLast(codeLengths, first, last);

        for (int bitShift = 0; bitShift < 8; bitShift++) {
            long start = bitShift;
            if ((long) data.length * 8L - start < k) break;
            int msb = peekMSB(data, start, k);
            int lsb = peekLSB(data, start, k);
            if (hitsPrefix(msb, k, first, last)) return true;
            if (hitsPrefix(lsb, k, first, last)) return true;
        }
        return false;
    }

    /* =========================================================
     * Dumps de diagnóstico
     * ========================================================= */

    public static void dumpHuffmanSummary(SegmentRecord.HuffmanTable ht,
                                          long requiredBits,
                                          int availableBits,
                                          int payloadStartByte) {
        if (ht == null) {
            System.out.println("Huffman: <null>");
            return;
        }
        int n = ht.symbolCount;
        int[] lens = ht.codeLengths != null ? ht.codeLengths : new int[0];
        int maxL = 0, nz = 0;
        for (int L : lens) { if (L > 0) { nz++; if (L > maxL) maxL = L; } }

        int[] hist = histogram(lens, Math.max(32, maxL));
        System.out.printf("Huffman N=%d, maxLen=%d, nonZeroLens=%d, kraftOk=%s%n",
                n, maxL, nz, kraftOk(lens));

        StringBuilder sb = new StringBuilder("Lengths histogram:");
        for (int L = 1; L <= Math.max(1, maxL); L++) {
            if (hist[L] != 0) sb.append(' ').append(L).append(':').append(hist[L]);
        }
        System.out.println(sb.toString());

        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, payloadStartByte);
    }

    public static void dumpSegmentSummary(SegmentRecord rec, ByteOrder order) {
        if (rec == null) {
            System.out.println("Segment: <null>");
            return;
        }
        SegmentRecord.Metadata md = rec.metadata;
        SegmentRecord.HuffmanTable ht = rec.huffman;

        long requiredBits = (md != null) ? md.sumBits() : 0L;
        int availableBits  = (rec.payloadSlice != null) ? rec.payloadSlice.remaining() * 8 : 0;

        if (md != null) {
            System.out.printf("Segment metadata: minDelta=%.6f maxDelta=%.6f totalBits=%d%n",
                    md.minDelta, md.maxDelta, requiredBits);
        } else {
            System.out.println("Segment metadata: <null>");
        }

        dumpHuffmanSummary(ht, requiredBits, availableBits, ht != null ? ht.payloadStart : -1);

        boolean prefixOk = (ht != null) && prefixLooksLike(rec.payloadSlice, ht.codeLengths, 64);
        String bo = (order == null) ? "unknown" :
                (order == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN");
        System.out.println("prefixProbe.anyHit=" + prefixOk + ", byteOrder=" + bo);
    }
}
