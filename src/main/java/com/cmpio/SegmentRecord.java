package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Registro de segmento CMP:
 *   [0..263]  - Metadata (264 bytes)
 *   [264..]   - Tabela local (detecção automática):
 *               • N(uint16) + [len(uint8)]^N + [symbol(uint8|uint16)]^N
 *               • ou N(uint16) + [symbol(uint8|uint16)]^N + [len(uint8)]^N
 *   [..]      - Bitstream concatenado dos 64 blocos (MSB-first, sem padding)
 */
public final class SegmentRecord {

    /* ============================ tipos ============================ */

    public static final class Metadata {
        public float minDelta, maxDelta;
        public final int[] quantDeltas    = new int[64]; // uint16
        public final int[] blockSizesBits = new int[64]; // uint16
        public long totalBits() { long s=0; for (int v: blockSizesBits) s += (v & 0xFFFFL); return s; }
    }

    public static final class HuffmanTable {
        public int    symbolCount;     // N (1..256)
        public byte[] codeLengths;     // N bytes
        public int[]  symbols16;       // N valores 0..65535 (se vier 8-bit, mapeia 0..255)
        public int    symbolWidthBytes;// 1 ou 2 (diagnóstico)
        public int byteSize() { return 2 + symbolCount + symbolWidthBytes * symbolCount; }
    }

    public static final class Probe {
        public final int payloadStart;    // 264 + tamTabela
        public final long requiredBits;   // Σ blockSizes
        public final ByteOrder metaOrder;
        public final int symbolWidthBytes;
        public final boolean symbolsFirst;
        Probe(int ps, long bits, ByteOrder ord, int w, boolean sf) {
            this.payloadStart = ps; this.requiredBits = bits; this.metaOrder = ord;
            this.symbolWidthBytes = w; this.symbolsFirst = sf;
        }
    }

    /* ============================ campos ============================ */

    public final Metadata     metadata;
    public final HuffmanTable huffman;
    public final ByteBuffer   compressedStreamSlice;
    public final ByteOrder    metadataByteOrder;

    private static final int METADATA_SIZE = 264;

    private SegmentRecord(Metadata md, HuffmanTable ht, ByteBuffer payload, ByteOrder mdOrder) {
        this.metadata = md; this.huffman = ht; this.compressedStreamSlice = payload; this.metadataByteOrder = mdOrder;
    }

    /* ============================ API ============================ */

    public static SegmentRecord parse(ByteBuffer record, ByteOrder fileOrder) {
        // tenta nas duas ordens de endianness para o cabeçalho local (metadata+tabela)
        ByteOrder alt = (fileOrder == ByteOrder.BIG_ENDIAN) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;

        ParseAttempt a = tryParseFlexible(record, fileOrder);
        if (a.valid) return a.build();

        ParseAttempt b = tryParseFlexible(record, alt);
        if (b.valid) return b.build();

        throw new IllegalStateException("Segment metadata/table inconsistent with provided record buffer.");
    }

    /** Probe para REC_len==0 (registro variável). Lê só cabeçalho+ tabela e calcula payloadStart/Σbits. */
    public static Probe probe(ByteBuffer atLeast8192, ByteOrder fileOrder) {
        ByteOrder alt = (fileOrder == ByteOrder.BIG_ENDIAN) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;

        Probe p = tryProbeFlexible(atLeast8192, fileOrder);
        if (p != null) return p;

        Probe q = tryProbeFlexible(atLeast8192, alt);
        if (q != null) return q;

        throw new IllegalStateException("Cannot probe segment: metadata/Huffman header unreadable.");
    }

    /* ============================ helpers ============================ */

    private static Metadata readMetadata(ByteBuffer rec, ByteOrder ord) {
        ByteBuffer b = rec.duplicate().order(ord);
        b.position(0);

        Metadata md = new Metadata();
        md.minDelta = b.getFloat();
        md.maxDelta = b.getFloat();
        for (int i = 0; i < 64; i++) md.quantDeltas[i]    = Short.toUnsignedInt(b.getShort());
        for (int i = 0; i < 64; i++) md.blockSizesBits[i] = Short.toUnsignedInt(b.getShort());
        return md;
    }

    private static final class ParseAttempt {
        final Metadata md; final HuffmanTable ht; final ByteBuffer payload; final ByteOrder mdOrder; final boolean valid;
        ParseAttempt(Metadata md, HuffmanTable ht, ByteBuffer payload, ByteOrder mdOrder, boolean valid) {
            this.md = md; this.ht = ht; this.payload = payload; this.mdOrder = mdOrder; this.valid = valid;
        }
        SegmentRecord build() { return new SegmentRecord(md, ht, payload, mdOrder); }
    }

    /** Lê tabela em qualquer uma das 4 combinações: [len|sym] × [sym8|sym16]; valida e escolhe a que fecha. */
    private static ParseAttempt tryParseFlexible(ByteBuffer rec, ByteOrder ord) {
        try {
            Metadata md = readMetadata(rec, ord);
            long requiredBits = md.totalBits();

            // Tenta combinações em ordem mais comum: [symbols16][lengths], [lengths][symbols16], [symbols8][lengths], [lengths][symbols8]
            boolean[] symbolsFirstOpts = { true, false };
            int[]     symWidthOpts     = { 2, 1 };

            for (boolean symbolsFirst : symbolsFirstOpts) {
                for (int symW : symWidthOpts) {
                    HuffmanTable ht = readHuffmanFlexible(rec, METADATA_SIZE, ord, symbolsFirst, symW);
                    if (ht == null) continue;

                    int payloadStart = METADATA_SIZE + ht.byteSize();
                    if (payloadStart > rec.limit()) continue;

                    // comprimentos têm que ser plausíveis (<=32 e existir pelo menos um >0)
                    int maxLen = 0, nz = 0;
                    for (byte Lb : ht.codeLengths) {
                        int L = Lb & 0xFF;
                        if (L > maxLen) maxLen = L;
                        if (L > 0) nz++;
                    }
                    if (nz == 0 || maxLen > 32) continue;

                    long availableBits = (long) (rec.limit() - payloadStart) * 8L;
                    if (requiredBits > availableBits) continue;

                    ByteBuffer payload = rec.duplicate();
                    payload.position(payloadStart);
                    payload = payload.slice();

                    return new ParseAttempt(md, ht, payload, ord, true);
                }
            }
            return new ParseAttempt(null, null, null, ord, false);
        } catch (Throwable t) {
            return new ParseAttempt(null, null, null, ord, false);
        }
    }

    private static HuffmanTable readHuffmanFlexible(ByteBuffer rec, int base, ByteOrder ord,
                                                    boolean symbolsFirst, int symbolWidthBytes) {
        try {
            ByteBuffer b = rec.duplicate().order(ord);
            b.position(base);

            int N = Short.toUnsignedInt(b.getShort());
            if (N < 1 || N > 256) return null;

            int lenPos, symPos, after;
            if (symbolsFirst) {
                symPos = base + 2;
                lenPos = symPos + symbolWidthBytes * N;
            } else {
                lenPos = base + 2;
                symPos = lenPos + N;
            }
            after = symPos + symbolWidthBytes * N;

            if (after > rec.limit()) return null;

            // Lê comprimentos (sempre N bytes)
            ByteBuffer bl = rec.duplicate().order(ord);
            bl.position(lenPos);
            byte[] lengths = new byte[N];
            bl.get(lengths);

            // Lê símbolos (8 ou 16 bits)
            int[] sym16 = new int[N];
            ByteBuffer bs = rec.duplicate().order(ord);
            bs.position(symPos);
            if (symbolWidthBytes == 1) {
                for (int i = 0; i < N; i++) sym16[i] = Byte.toUnsignedInt(bs.get());
            } else if (symbolWidthBytes == 2) {
                for (int i = 0; i < N; i++) sym16[i] = Short.toUnsignedInt(bs.getShort());
            } else return null;

            HuffmanTable ht = new HuffmanTable();
            ht.symbolCount = N;
            ht.codeLengths = lengths;
            ht.symbols16   = sym16;
            ht.symbolWidthBytes = symbolWidthBytes;
            return ht;
        } catch (Throwable t) {
            return null;
        }
    }

    /** Probe que tenta detectar ordem e largura e retorna o payloadStart correto. */
    private static Probe tryProbeFlexible(ByteBuffer rec, ByteOrder ord) {
        try {
            Metadata md = readMetadata(rec, ord);
            long requiredBits = md.totalBits();

            boolean[] symbolsFirstOpts = { true, false };
            int[]     symWidthOpts     = { 2, 1 };

            for (boolean symbolsFirst : symbolsFirstOpts) {
                for (int symW : symWidthOpts) {
                    HuffmanTable ht = readHuffmanFlexible(rec, METADATA_SIZE, ord, symbolsFirst, symW);
                    if (ht == null) continue;

                    // comprimentos plausíveis?
                    int maxLen = 0, nz = 0;
                    for (byte Lb : ht.codeLengths) {
                        int L = Lb & 0xFF;
                        if (L > maxLen) maxLen = L;
                        if (L > 0) nz++;
                    }
                    if (nz == 0 || maxLen > 32) continue;

                    int payloadStart = METADATA_SIZE + ht.byteSize();
                    if (payloadStart > rec.limit()) continue; // precisa caber no buffer do probe (>=8192)

                    return new Probe(payloadStart, requiredBits, ord, symW, symbolsFirst);
                }
            }
            return null;
        } catch (Throwable t) {
            return null;
        }
    }
}
