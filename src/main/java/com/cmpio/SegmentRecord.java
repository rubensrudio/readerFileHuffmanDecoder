package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Parser de um Segment Record (8192 bytes) com heurísticas robustas:
 * - Localiza/valida a tabela Huffman (layout SYM_LEN + nibbles hi->lo)
 * - Evita falsos-positivos (N muito pequeno, payloadStart baixo, prefix-probe falhando)
 * - Determina payloadStart, payloadSlice e totalBits (com janela plausível)
 */
public final class SegmentRecord {

    public static final int RECORD_SIZE = 8192;

    // Parâmetros de heurística
    private static final int SCAN_START = 256;        // início da varredura de candidatos
    private static final int SCAN_END   = 1024;       // fim (exclusivo)
    private static final int MIN_PAYLOAD_START = 512; // evita “cabos” no começo do record
    private static final int MAX_PREVIEW_BYTES  = 256;// bytes para prefix-probe (dentro do record)
    private static final int MAX_STITCHED_RECORDS = 8;// teto para requiredBits plausível (8 records)

    /** Metadados mínimos necessários para stage 2. */
    public static final class Metadata {
        public double minDelta;
        public double maxDelta;
        public long   totalBits;
        @Override public String toString() {
            return String.format("Metadata{minDelta=%.6f maxDelta=%.6f totalBits=%d}", minDelta, maxDelta, totalBits);
        }
    }

    /** Tabela de Huffman (símbolos + comprimentos 0..15). */
    public static final class Huffman {
        public int[] symbols;
        public int[] lens;
        public int   maxLen;
        public int   nonZeroLens;
        public boolean kraftOk;
        @Override public String toString() {
            return "Huffman{N=" + symbols.length +
                    ", maxlen=" + maxLen +
                    ", nonZeroLens=" + nonZeroLens +
                    ", kraftOk=" + kraftOk + "}";
        }
    }

    // Exposição ao Stage2
    public Metadata   md;
    public Huffman    huffman;
    public int        payloadStart;
    public ByteBuffer payloadSlice;

    private SegmentRecord() {}

    public static SegmentRecord parse(ByteBuffer fileBuf, int recStart, ByteOrder order) {
        if (fileBuf == null) throw new IllegalStateException("SegmentRecord.parse: fileBuf é null.");
        if (recStart < 0 || recStart + RECORD_SIZE > fileBuf.capacity()) {
            throw new IllegalArgumentException("recStart fora do arquivo: " + recStart);
        }
        Objects.requireNonNull(order, "byte order null");

        ByteBuffer rec = slice(fileBuf, recStart, RECORD_SIZE).order(order);

        SegmentRecord out = new SegmentRecord();
        out.md = new Metadata();

        // leitura opcional de min/max (apenas informativo)
        try {
            double d0 = rec.getDouble(0), d1 = rec.getDouble(8);
            if (!Double.isNaN(d0)) out.md.minDelta = d0;
            if (!Double.isNaN(d1)) out.md.maxDelta = d1;
        } catch (Throwable ignore) {}

        // ===== 1) Prospecção de candidatos de Huffman =====
        HuffmanFound best = null;
        for (int base = SCAN_START; base < SCAN_END; base++) {
            for (int N = 2; N <= 64; N++) { // expandido até 64 (tabelas “reais” costumam estar ~32..48)
                int bytesLens = (N + 1) >> 1;
                int end = base + N + bytesLens;
                if (end > RECORD_SIZE) break;

                int[] syms = new int[N];
                for (int i = 0; i < N; i++) syms[i] = rec.get(base + i) & 0xFF;

                int[] lens = new int[N];
                for (int i = 0; i < bytesLens; i++) {
                    int b = rec.get(base + N + i) & 0xFF;
                    int hi = (b >>> 4) & 0xF, lo = b & 0xF;
                    int idx = i << 1;
                    if (idx     < N) lens[idx]     = hi;
                    if (idx + 1 < N) lens[idx + 1] = lo;
                }

                if (!lensInRange(lens)) continue;
                if (!allUnique(syms))   continue;

                int nonZero = 0, maxLen = 0;
                for (int L : lens) if (L > 0) { nonZero++; if (L > maxLen) maxLen = L; }
                if (nonZero < 2) continue;
                if (!kraftOK(lens)) continue;

                int payloadStart = align16(end);

                // ========= Scoring inicial (filtro rápido) =========
                int score = 0;
                if (N >= 3) score += 3;                          // preferir tabelas reais
                if (payloadStart >= MIN_PAYLOAD_START) score += 3;
                if (payloadStart % 16 == 0) score += 1;
                score += Math.min(maxLen, 8);                    // maxLen razoável ajuda
                score += nonZero;                                // mais códigos ativos
                if (N >= 32 && N <= 48) score += 4;              // preferência suave para zona onde N≈39 cai

                // ========= Prefix-probe forte (confirmação) =========
                if (payloadStart >= RECORD_SIZE) continue;
                int availBytes = Math.min(MAX_PREVIEW_BYTES, RECORD_SIZE - payloadStart);
                if (availBytes <= 0) continue;
                ByteBuffer pay = slice(rec, payloadStart, availBytes);

                if (!tinyPrefixProbeStrong(syms, lens, pay)) continue; // descarta se não bater

                HuffmanFound cand = new HuffmanFound();
                cand.base = base;
                cand.N = N;
                cand.syms = syms;
                cand.lens = lens;
                cand.maxLen = maxLen;
                cand.nonZero = nonZero;
                cand.payloadStart = payloadStart;
                cand.score = score;

                if (best == null || cand.score > best.score ||
                        (cand.score == best.score && cand.payloadStart < best.payloadStart)) {
                    best = cand;
                }
            }
        }
        if (best == null) {
            throw new IllegalStateException("Huffman table não reconhecida: nenhum candidato passou no prefix-probe + sanidade.");
        }

        // ===== 2) Consolidar Huffman e payload =====
        out.huffman = new Huffman();
        out.huffman.symbols = best.syms;
        out.huffman.lens    = best.lens;
        out.huffman.maxLen  = best.maxLen;
        out.huffman.nonZeroLens = best.nonZero;
        out.huffman.kraftOk = true;

        out.payloadStart = best.payloadStart;
        out.payloadSlice = slice(rec, out.payloadStart, RECORD_SIZE - out.payloadStart);

        int payloadBytes = out.payloadSlice.remaining();
        long availableBits = (long) payloadBytes * 8L;

        // ===== 3) totalBits (padrão com janela plausível + caps) =====
        long hardCapBits = (long) RECORD_SIZE * 8L * MAX_STITCHED_RECORDS; // p.ex. 8 records
        long softCapBits = 200_000L;                                       // heurística para este dataset
        long cap = Math.min(hardCapBits, softCapBits);

        long requiredBits = findReasonableBitsInt(
                rec,
                Math.max(0, out.payloadStart - 512),
                Math.min(512, out.payloadStart),
                order,
                availableBits,
                cap
        );

        if (requiredBits <= availableBits || requiredBits > cap) {
            long fallback = availableBits + (5500L << 3); // ~5.5 KiB adicionais
            requiredBits = Math.min(fallback, cap);
        }
        out.md.totalBits = requiredBits;

        // ===== 4) Log amigável =====
        System.out.printf("Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=%d, layout=SYM_LEN, lensEnc=nibbles(hi->lo), payloadStart=%d%n",
                out.md.minDelta, out.md.maxDelta, out.huffman.symbols.length, best.base, out.payloadStart);
        System.out.printf("Huffman %s%n", out.huffman);
        System.out.printf("Lengths histogram: %s%n", histLens(out.huffman.lens));
        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                out.md.totalBits, availableBits, out.payloadStart);

        return out;
    }

    // ====================== Helpers ======================

    private static boolean lensInRange(int[] lens) {
        for (int L : lens) if (L < 0 || L > 15) return false;
        return true;
    }
    private static boolean allUnique(int[] a) {
        for (int i = 0; i < a.length; i++) for (int j = i + 1; j < a.length; j++) if (a[i] == a[j]) return false;
        return true;
    }
    private static boolean kraftOK(int[] lens) {
        int max = 0; for (int L : lens) if (L > max) max = L;
        if (max <= 0) return false;
        long sum = 0, full = 1L << max;
        for (int L : lens) if (L > 0) { sum += 1L << (max - L); if (sum > full) return false; }
        return sum <= full;
    }
    private static int align16(int x) { int m = x & 15; return (m == 0) ? x : (x + (16 - m)); }
    private static String histLens(int[] lens) {
        int[] h = new int[16]; for (int L : lens) if (L >= 0 && L < 16) h[L]++;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < h.length; i++) if (h[i] != 0) { if (sb.length()!=0) sb.append(' '); sb.append(i).append(':').append(h[i]); }
        return sb.toString();
    }

    /** Varre uma janela antes do payload à procura de um int “totalBits” plausível. */
    private static long findReasonableBitsInt(ByteBuffer rec,
                                              int start, int len,
                                              ByteOrder order,
                                              long availableBits,
                                              long maxPlausibleBits) {
        if (len <= 0) return -1;
        int end = Math.min(rec.capacity(), start + len);
        ByteOrder old = rec.order();
        rec.order(order);
        try {
            for (int pos = start; pos + 4 <= end; pos++) {
                long vv = rec.getInt(pos) & 0xFFFFFFFFL;
                if (vv > availableBits && vv <= maxPlausibleBits) return vv;
            }
        } catch (Throwable ignore) {
        } finally {
            rec.order(old);
        }
        return -1;
    }

    /** Slice seguro. */
    static ByteBuffer slice(ByteBuffer src, int pos, int len) {
        if (src == null) throw new IllegalStateException("slice: src é null.");
        if (pos < 0 || len < 0 || pos + len > src.capacity())
            throw new IllegalArgumentException(String.format("slice fora: pos=%d len=%d cap=%d", pos, len, src.capacity()));
        ByteBuffer d = src.duplicate(); d.position(pos); d.limit(pos + len); return d.slice();
    }

    /** Estrutura temporária do melhor candidato. */
    private static final class HuffmanFound {
        int base, N, maxLen, nonZero, payloadStart, score;
        int[] syms, lens;
    }

    // =================== Prefix-probe forte ===================
    // Testa se algum prefixo do payload (dentro do próprio record) bate com a tabela
    // para qualquer combinação MSB/LSB × invert × shifts 0..3.
    private static boolean tinyPrefixProbeStrong(int[] syms, int[] lens, ByteBuffer payload) {
        byte[] data = new byte[payload.remaining()];
        payload.duplicate().get(data);
        Canon canon = Canon.fromSymbolsAndLengths(syms, lens);

        final boolean[] orders = { true, false };   // LSB/MSB
        final boolean[] invs   = { false, true };

        for (boolean lsb : orders) {
            DecodeNode root = canon.buildDecodeTree(lsb);
            for (boolean inv : invs) {
                for (int shift = 0; shift <= 3; shift++) {
                    if (probeWith(root, data, lsb, inv, shift)) return true;
                }
            }
        }
        return false;
    }

    private static boolean probeWith(DecodeNode root, byte[] data,
                                     boolean lsb, boolean inv, int shift) {
        BitReader br = new BitReader(data, Math.min((long) data.length * 8, 2048),
                lsb, inv, shift);
        int decoded = 0, distinct = 0;
        int[] seen = new int[256];
        Arrays.fill(seen, 0);

        for (int i = 0; i < 64; i++) {
            int s = decodeOne(root, br);
            if (s < 0) return false;
            decoded++;
            if (seen[s] == 0) { seen[s] = 1; distinct++; }
        }
        return (decoded >= 32 && distinct >= 4);
    }

    // === Implementação mínima de Canon/árvore/bitreader (sem depender do Stage2Analyzer) ===

    private static final class Canon {
        static final class Entry { final int sym,len; int codeMSB; Entry(int s,int l){sym=s;len=l;} }
        final Entry[] entries;
        Canon(Entry[] es){ entries=es; }
        static Canon fromSymbolsAndLengths(int[] syms, int[] lens) {
            int n = syms.length;
            Entry[] es = new Entry[n];
            int k=0;
            for (int i=0;i<n;i++) if (lens[i]>0) es[k++] = new Entry(syms[i], lens[i]);
            es = Arrays.copyOf(es, k);
            Arrays.sort(es, (a,b)-> a.len!=b.len ? Integer.compare(a.len,b.len) : Integer.compare(a.sym,b.sym));
            int code=0, prev=es.length>0?es[0].len:0;
            for (Entry e: es){ if (e.len>prev){ code <<= (e.len-prev); prev=e.len; } e.codeMSB = code; code++; }
            return new Canon(es);
        }
        DecodeNode buildDecodeTree(boolean lsb) {
            DecodeNode root = new DecodeNode();
            for (Entry e: entries){
                int bits = lsb ? bitReverse(e.codeMSB, e.len) : e.codeMSB;
                DecodeNode n=root;
                for (int i=e.len-1;i>=0;i--){
                    int b=(bits>>>i)&1;
                    DecodeNode nx = (b==0)? n.zero : n.one;
                    if (nx==null){ nx=new DecodeNode(); if (b==0) n.zero=nx; else n.one=nx; }
                    n=nx;
                }
                n.isLeaf=true; n.symbol=e.sym;
            }
            return root;
        }
        static int bitReverse(int v,int w){ int r=0; for(int i=0;i<w;i++){ r=(r<<1)|(v&1); v>>>=1; } return r; }
    }
    private static final class DecodeNode { boolean isLeaf; int symbol; DecodeNode zero,one; }
    private static int decodeOne(DecodeNode root, BitReader br){
        DecodeNode n=root;
        while(true){
            if (n==null) return -1;
            if (n.isLeaf) return n.symbol;
            int b=br.readBit(); if (b<0) return -1;
            n=(b==0)? n.zero : n.one;
        }
    }
    private static final class BitReader {
        final byte[] data; final long limitBits; final boolean lsb; final boolean inv; final int shift; long pos=0;
        BitReader(byte[] d,long lim,boolean lsb,boolean inv,int sh){this.data=d;this.limitBits=lim;this.lsb=lsb;this.inv=inv;this.shift=sh&7;}
        int readBit(){
            long p=pos+shift; if (p>=limitBits) return -1;
            int bi=(int)(p>>>3), off=(int)(p&7), v=data[bi]&0xFF;
            int bit = lsb ? ((v>>>off)&1) : ((v>>>(7-off))&1);
            if (inv) bit^=1; pos++; return bit;
        }
    }
}
