package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Parser de um Segment Record (8192 bytes) com heurísticas robustas.
 * Exposição compatível com AnalyzeSegment:
 *  - recStart
 *  - md: minDelta, maxDelta, totalBits, payloadStartByte, payloadBytes
 *  - huffman: N, base, layout, lensEncoding, symbols, lens, kraftOk
 */
public final class SegmentRecord {

    public static final int RECORD_SIZE = 8192;

    // Heurística de busca
    private static final int SCAN_START = 256;
    private static final int SCAN_END   = 1024;
    private static final int MIN_PAYLOAD_START = 512;
    private static final int MAX_PREVIEW_BYTES = 256;
    private static final int MAX_STITCHED_RECORDS = 8;

    // ======= Tipos expostos =======

    /** Metadados do record. */
    public static final class Metadata {
        public double minDelta;
        public double maxDelta;

        /** total de bits a consumir no bitstream (após montagem multi-record). */
        public long   totalBits;

        /** offset (em bytes) do payload dentro do record (0..8192). */
        public int    payloadStartByte;

        /** bytes disponíveis no payload deste record (8192 - payloadStartByte). */
        public int    payloadBytes;

        @Override public String toString() {
            return String.format("Metadata{minDelta=%.6f maxDelta=%.6f totalBits=%d payloadStart=%d payloadBytes=%d}",
                    minDelta, maxDelta, totalBits, payloadStartByte, payloadBytes);
        }
    }

    /** Tabela Huffman detectada. */
    public static final class HuffmanInfo {
        public int    N;                // quantidade de símbolos da tabela
        public int    base;             // posição (offset) dentro do record onde a tabela começa
        public String layout;           // "SYM_LEN"
        public String lensEncoding;     // "nibbles(hi->lo)"
        public int[]  symbols;          // 0..255
        public int[]  lens;             // 0..15, mesmo comprimento de symbols
        public boolean kraftOk;         // resultado do teste de Kraft

        @Override public String toString() {
            return "Huffman{N=" + N +
                    ", kraftOk=" + kraftOk +
                    ", layout=" + layout +
                    ", lensEnc=" + lensEncoding + "}";
        }
    }

    // ======= Campos expostos ao restante do pipeline =======
    public final int recStart;      // offset do registro (8192 bytes) no arquivo
    public final Metadata md;       // metadados
    public final HuffmanInfo huffman; // tabela huffman
    public final ByteBuffer payloadSlice; // janela deste record (somente bytes locais, sem costura)

    // ======= Construção =======
    private SegmentRecord(int recStart, Metadata md, HuffmanInfo h, ByteBuffer payloadSlice) {
        this.recStart = recStart;
        this.md = md;
        this.huffman = h;
        this.payloadSlice = payloadSlice;
    }

    /**
     * Faz o parse de um registro (8192 bytes) a partir do arquivo mapeado.
     * @param fileBuf  buffer do arquivo inteiro (mapeado)
     * @param recStart offset do início do registro (8192 bytes)
     * @param order    endianness do arquivo
     */
    public static SegmentRecord parse(ByteBuffer fileBuf, int recStart, ByteOrder order) {
        if (fileBuf == null) throw new IllegalStateException("SegmentRecord.parse: fileBuf é null.");
        if (recStart < 0 || recStart + RECORD_SIZE > fileBuf.capacity()) {
            throw new IllegalArgumentException("recStart fora do arquivo: " + recStart);
        }
        Objects.requireNonNull(order, "byte order null");

        ByteBuffer rec = slice(fileBuf, recStart, RECORD_SIZE).order(order);

        // 0) min/max (opcional; muitos arquivos têm 0.0/0.0)
        Metadata md = new Metadata();
        try {
            double d0 = rec.getDouble(0), d1 = rec.getDouble(8);
            if (!Double.isNaN(d0)) md.minDelta = d0;
            if (!Double.isNaN(d1)) md.maxDelta = d1;
        } catch (Throwable ignore) {}

        // 1) Scaneia candidatos de tabela Huffman (SYM_LEN + nibbles hi->lo)
        HuffmanFound best = null;
        for (int base = SCAN_START; base < SCAN_END; base++) {
            for (int N = 2; N <= 64; N++) {
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
                if (!kraftOK(lens))     continue;

                int nonZero = 0, maxLen = 0;
                for (int L : lens) if (L > 0) { nonZero++; if (L > maxLen) maxLen = L; }
                if (nonZero < 2) continue;

                int payloadStart = align16(end);
                if (payloadStart < MIN_PAYLOAD_START || payloadStart >= RECORD_SIZE) continue;

                // score
                int score = 0;
                if (N >= 3) score += 3;
                if (payloadStart % 16 == 0) score += 1;
                score += Math.min(maxLen, 8);
                score += nonZero;
                if (N >= 32 && N <= 48) score += 4;

                // prefix-probe forte no próprio record (até 256 bytes)
                int availBytes = Math.min(MAX_PREVIEW_BYTES, RECORD_SIZE - payloadStart);
                if (availBytes <= 0) continue;
                ByteBuffer pay = slice(rec, payloadStart, availBytes);
                if (!tinyPrefixProbeStrong(syms, lens, pay)) continue;

                HuffmanFound cand = new HuffmanFound();
                cand.base = base;
                cand.N = N;
                cand.syms = syms;
                cand.lens = lens;
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

        // 2) Consolidar huffman + payload
        HuffmanInfo h = new HuffmanInfo();
        h.N = best.N;
        h.base = best.base;
        h.layout = "SYM_LEN";
        h.lensEncoding = "nibbles(hi->lo)";
        h.symbols = best.syms;
        h.lens    = best.lens;
        h.kraftOk = true;

        md.payloadStartByte = best.payloadStart;
        md.payloadBytes = RECORD_SIZE - md.payloadStartByte;

        ByteBuffer payloadSlice = slice(rec, md.payloadStartByte, md.payloadBytes);

        // 3) totalBits: tenta achar int plausível antes do payload; se falhar, fallback + caps
        long availableBits = (long) md.payloadBytes * 8L;
        long hardCapBits = (long) RECORD_SIZE * 8L * MAX_STITCHED_RECORDS; // ex.: 8 records
        long softCapBits = 200_000L;                                       // heurístico
        long cap = Math.min(hardCapBits, softCapBits);

        long requiredBits = findReasonableBitsInt(
                rec,
                Math.max(0, md.payloadStartByte - 512),
                Math.min(512, md.payloadStartByte),
                order,
                availableBits,
                cap
        );

        if (requiredBits <= availableBits || requiredBits > cap) {
            long fallback = availableBits + (5500L << 3); // ~5.5 KiB extra
            requiredBits = Math.min(fallback, cap);
        }
        md.totalBits = requiredBits;

        // 4) monta objeto final
        return new SegmentRecord(recStart, md, h, payloadSlice);
    }

    // ====== Helpers ======

    private static boolean lensInRange(int[] lens) {
        for (int L : lens) if (L < 0 || L > 15) return false;
        return true;
    }
    private static boolean allUnique(int[] a) {
        for (int i = 0; i < a.length; i++)
            for (int j = i + 1; j < a.length; j++)
                if (a[i] == a[j]) return false;
        return true;
    }
    private static boolean kraftOK(int[] lens) {
        int max = 0; for (int L : lens) if (L > max) max = L;
        if (max <= 0) return false;
        long sum = 0, full = 1L << max;
        for (int L : lens) if (L > 0) {
            sum += 1L << (max - L);
            if (sum > full) return false;
        }
        return sum <= full;
    }
    private static int align16(int x) { int m = x & 15; return (m == 0) ? x : (x + (16 - m)); }

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
        ByteBuffer d = src.duplicate();
        d.position(pos);
        d.limit(pos + len);
        return d.slice();
    }

    // ======= Estruturas auxiliares de detecção =======
    private static final class HuffmanFound {
        int base, N, payloadStart, score;
        int[] syms, lens;
    }

    // ======= Prefix-probe forte (sem depender de Stage2/3 externos) =======
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
            if (s >= 0 && s < 256 && seen[s] == 0) { seen[s] = 1; distinct++; }
        }
        return (decoded >= 32 && distinct >= 4);
    }

    // === Implementação mínima para probe ===
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
            Arrays.sort(es, (a,b)-> a.len!=b.len ? Integer.compare(a.len,b.len)
                    : Integer.compare(a.sym,b.sym));
            int code=0, prev=es.length>0?es[0].len:0;
            for (Entry e: es){
                if (e.len>prev){ code <<= (e.len-prev); prev=e.len; }
                e.codeMSB = code;
                code++;
            }
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
