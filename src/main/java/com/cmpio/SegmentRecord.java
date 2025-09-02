package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class SegmentRecord {

    public static final int RECORD_SIZE   = 8192;
    public static final int METADATA_SIZE = 264;  // 4+4 + (64*2) + (64*2)
    public static final int HUFF_OFF      = 264;  // posição “esperada” de N (uint16)

    public final Metadata metadata;
    public final HuffmanTable huffman;
    public final ByteBuffer payloadSlice; // fatia do ARQUIVO a partir do payload
    public final int payloadStart;        // offset relativo ao INÍCIO DO RECORD

    /* ================= Tipos ================= */

    public static final class Metadata {
        public final float minDelta;
        public final float maxDelta;
        public final int[] quant;      // uint16[64]
        public final int[] blockBits;  // uint16[64]
        public Metadata(float minDelta, float maxDelta, int[] quant, int[] blockBits) {
            this.minDelta = minDelta; this.maxDelta = maxDelta;
            this.quant = quant; this.blockBits = blockBits;
        }
        public long sumBits() { long s=0; for (int v: blockBits) s += (v & 0xFFFF); return s; }
    }

    public static final class HuffmanTable {
        public final int   symbolCount;    // N
        public final int[] codeLengths;    // N (0..15; 0 = símbolo ausente)
        public final int[] symbols;        // N (0..255)
        public final int   payloadStart;   // relativo ao início do record
        public HuffmanTable(int n, int[] lens, int[] syms, int payloadStart) {
            this.symbolCount = n; this.codeLengths = lens; this.symbols = syms; this.payloadStart = payloadStart;
        }
    }

    private SegmentRecord(Metadata md, HuffmanTable ht, ByteBuffer payloadSlice, int payloadStart) {
        this.metadata = md; this.huffman = ht; this.payloadSlice = payloadSlice; this.payloadStart = payloadStart;
    }

    /* =============== API PRINCIPAL =============== */

    public static SegmentRecord parse(ByteBuffer fileBuf, int recStart, ByteOrder order) {
        ByteBuffer rec = slice(fileBuf, recStart, RECORD_SIZE).order(order);

        // 1) METADATA (0..263)
        float minDelta = rec.getFloat(0);
        float maxDelta = rec.getFloat(4);

        int[] quant = new int[64];
        for (int i=0, off=8; i<64; i++, off+=2) quant[i] = rec.getShort(off) & 0xFFFF;

        int[] blockBits = new int[64];
        for (int i=0, off=136; i<64; i++, off+=2) blockBits[i] = rec.getShort(off) & 0xFFFF;

        Metadata md = new Metadata(minDelta, maxDelta, quant, blockBits);
        long requiredBits = md.sumBits();

        // 2) HUFFMAN (robusto: tenta offsets perto de 264 e, se precisar, varre mais amplo)
        Candidate best = findHuffman(rec, fileBuf, recStart);
        if (best == null) {
            throw new IllegalStateException("Huffman table não reconhecida: nenhum vetor cabe como comprimentos 0..15 + Kraft.");
        }

        ByteBuffer payload = slice(fileBuf, recStart + best.payloadOff, RECORD_SIZE - best.payloadOff)
                .order(ByteOrder.BIG_ENDIAN);

        HuffmanTable ht = new HuffmanTable(best.N, best.lens, best.syms, best.payloadOff);

        // Diag amigável
        int availableBits = payload.remaining() * 8;
        System.out.printf(
                "Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=%d (shift %+d, N-%s), layout=%s, lensEnc=%s, payloadStart=%d, requiredBits=%d, availableBits=%d%n",
                minDelta, maxDelta, best.N, best.base, best.base - HUFF_OFF,
                best.nEndian == ByteOrder.BIG_ENDIAN ? "BE" : "LE",
                best.layout, best.lensEnc.desc, best.payloadOff, requiredBits, availableBits);

        return new SegmentRecord(md, ht, payload, best.payloadOff);
    }

    /* =============== Localizador robusto da tabela =============== */

    private enum Layout { LEN_SYM, SYM_LEN }
    private enum LenEnc {
        BYTES("bytes"),
        NIBBLES_HI_LO("nibbles(hi->lo)"),
        NIBBLES_LO_HI("nibbles(lo->hi)");
        final String desc; LenEnc(String d){this.desc=d;}
    }

    private static final class Candidate {
        final int base; final ByteOrder nEndian; final int N;
        final int[] lens; final int[] syms;
        final int payloadOff; final Layout layout; final LenEnc lensEnc;
        final int score;
        Candidate(int base, ByteOrder nEndian, int N, int[] lens, int[] syms,
                  int payloadOff, Layout layout, LenEnc enc, int score) {
            this.base=base; this.nEndian=nEndian; this.N=N; this.lens=lens; this.syms=syms;
            this.payloadOff=payloadOff; this.layout=layout; this.lensEnc=enc; this.score=score;
        }
    }

    private static Candidate findHuffman(ByteBuffer rec, ByteBuffer fileBuf, int recStart) {
        // Fase 1: região próxima do esperado
        Candidate c = tryRange(rec, fileBuf, recStart, HUFF_OFF-4, HUFF_OFF+4);
        if (c != null) return c;
        // Fase 2: varredura conservadora pelo record
        return tryRange(rec, fileBuf, recStart, 240, 4096);
    }

    private static Candidate tryRange(ByteBuffer rec, ByteBuffer fileBuf, int recStart, int from, int to) {
        Candidate best = null;

        for (int base = Math.max(0, from); base <= Math.min(RECORD_SIZE-2, to); base++) {
            for (ByteOrder nbo : new ByteOrder[]{ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN}) {
                int N = readU16(rec, base, nbo);
                if (N < 1 || N > 256) continue;

                // Tentar quatro layouts: LEN+SYM e SYM+LEN, com lens em BYTES/NIBBLES (e 2 ordens de nibbles)
                for (Layout lay : new Layout[]{Layout.LEN_SYM, Layout.SYM_LEN}) {
                    for (LenEnc enc : new LenEnc[]{LenEnc.BYTES, LenEnc.NIBBLES_HI_LO, LenEnc.NIBBLES_LO_HI}) {
                        Candidate cand = tryLayout(rec, fileBuf, recStart, base, N, lay, enc);
                        if (cand == null) continue;
                        // escolhe melhor por: prefixHit, proximidade de 264, número de comprimentos !=0
                        if (best == null || cand.score > best.score) best = cand;
                    }
                }
            }
        }
        return best;
    }

    /** Testa um layout específico a partir de 'base' (onde está o N). */
    private static Candidate tryLayout(ByteBuffer rec, ByteBuffer fileBuf, int recStart,
                                       int base, int N, Layout layout, LenEnc enc) {

        // Tamanhos dos vetores conforme “enc”
        int lenBytes = (enc == LenEnc.BYTES) ? N : ((N + 1) >> 1); // ceil(N/2)
        int symBytes = N;

        // Offsets dos dois vetores
        int v1Off = base + 2;
        int v2Off = v1Off + (layout == Layout.LEN_SYM ? lenBytes : symBytes);
        int payloadOff = v2Off + (layout == Layout.LEN_SYM ? symBytes : lenBytes);

        if (payloadOff > RECORD_SIZE) return null;

        // Extrai lens e syms conforme o layout/codificação
        int[] lens, syms;

        if (layout == Layout.LEN_SYM) {
            lens = readLens(rec, v1Off, N, enc);
            if (lens == null) return null;
            syms = readU8(rec, v2Off, N);
        } else { // SYM_LEN
            syms = readU8(rec, v1Off, N);
            lens = readLens(rec, v2Off, N, enc);
            if (lens == null) return null;
        }

        // Validações:
        if (!looksLen(lens)) return null;
        for (int s : syms) if (s < 0 || s > 255) return null;
        if (!CmpSanity.kraftOk(lens)) return null;

        // Desempate por prefix probe
        boolean prefixHit = false;
        {
            ByteBuffer tentative = slice(fileBuf, recStart + payloadOff, RECORD_SIZE - payloadOff);
            prefixHit = CmpSanity.prefixLooksLike(tentative, lens, 64);
        }

        int dist = Math.abs(base - HUFF_OFF);
        int nz = countNonZero(lens);
        int score = (prefixHit ? 100000 : 0) - dist + nz;

        return new Candidate(base, ByteOrder.BIG_ENDIAN /*N já incorporado no base, score não precisa de endianness*/,
                N, lens, syms, payloadOff, layout, enc, score);
    }

    /* ================= Helpers ================= */

    private static int countNonZero(int[] v){int c=0; for(int x: v) if(x!=0)c++; return c;}

    /** Vetor “crível” de comprimentos: 0..15, ≥1 não-zero. Kraft é checada separadamente. */
    private static boolean looksLen(int[] v) {
        if (v == null || v.length == 0) return false;
        int nz = 0;
        for (int x : v) { if (x < 0 || x > 15) return false; if (x != 0) nz++; }
        return nz > 0;
    }

    private static int readU16(ByteBuffer buf, int off, ByteOrder bo) {
        int b0 = buf.get(off)   & 0xFF;
        int b1 = buf.get(off+1) & 0xFF;
        return (bo == ByteOrder.BIG_ENDIAN) ? ((b0 << 8)|b1) : ((b1 << 8)|b0);
    }

    private static int[] readU8(ByteBuffer buf, int off, int n) {
        int[] out = new int[n];
        for (int i=0;i<n;i++) out[i] = buf.get(off+i) & 0xFF;
        return out;
    }

    /** Lê comprimentos com base em enc (bytes ou nibbles). */
    private static int[] readLens(ByteBuffer buf, int off, int N, LenEnc enc) {
        int[] lens = new int[N];
        if (enc == LenEnc.BYTES) {
            for (int i=0;i<N;i++) lens[i] = buf.get(off+i) & 0xFF;
            return lens;
        }

        // NIBBLES
        int needBytes = (N + 1) >> 1;
        int idx = 0;
        for (int b=0; b<needBytes; b++) {
            int v = buf.get(off + b) & 0xFF;
            int hi = (v >>> 4) & 0xF;
            int lo = v & 0xF;

            if (enc == LenEnc.NIBBLES_HI_LO) {
                if (idx < N) lens[idx++] = hi;
                if (idx < N) lens[idx++] = lo;
            } else { // NIBBLES_LO_HI
                if (idx < N) lens[idx++] = lo;
                if (idx < N) lens[idx++] = hi;
            }
        }
        return lens;
    }

    private static ByteBuffer slice(ByteBuffer src, int pos, int len) {
        ByteBuffer d = src.duplicate();
        d.position(pos); d.limit(pos + len);
        return d.slice();
    }

    /** Utilitário: hexdump local (debug). */
    public static String dumpAround(ByteBuffer fileBuf, int recStart, int center, int span) {
        int from = Math.max(0, center - span), to = Math.min(RECORD_SIZE, center + span);
        ByteBuffer rec = slice(fileBuf, recStart + from, to - from);
        return CmpSanity.hexdump(rec, 0, rec.remaining());
    }
}
