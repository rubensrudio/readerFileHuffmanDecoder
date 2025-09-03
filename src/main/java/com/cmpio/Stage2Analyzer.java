package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Stage 2: monta o bitstream multi-record (PayloadAssembler) e
 * auto-prova combinações de leitura de bits (LSB/MSB × invert × shift 0..7)
 * para validar a tabela de Huffman detectada no SegmentRecord.
 *
 * Ajustes de nomes:
 *  - requiredBits = rec.md.totalBits     (campo, sem "()")
 *  - code lengths = rec.huffman.lens     (não "lengths")
 */
public final class Stage2Analyzer {

    private Stage2Analyzer() {}

    public static void analyze(ByteBuffer file,
                               int recStart,
                               ByteOrder order,
                               SegmentRecord rec) {

        Objects.requireNonNull(file, "file buffer null");
        Objects.requireNonNull(rec,  "segment record null");

        // ===== Metadados =====
        if (rec.md == null) {
            throw new IllegalStateException("SegmentRecord.md é null (esperado totalBits, etc.)");
        }
        final long requiredBits = rec.md.totalBits; // << ajuste: campo, não método
        if (requiredBits <= 0) {
            throw new IllegalStateException("totalBits <= 0 no metadata do segmento.");
        }

        if (rec.huffman == null) {
            throw new IllegalStateException("SegmentRecord.huffman é null (tabela não reconhecida?).");
        }

        // símbolos e comprimentos
        final int[] symbols  = ensureIntArray(rec.huffman.symbols, "huffman.symbols");
        final int[] codeLens = ensureIntArray(rec.huffman.lens,    "huffman.lens"); // << ajuste

        final int payloadBytes = (rec.payloadSlice != null) ? rec.payloadSlice.remaining() : 0;
        final long availableBits = (long) payloadBytes * 8L;
        final int requiredBytes  = (int) ((requiredBits + 7) >>> 3);

        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, rec.payloadStart);

        if (requiredBits > availableBits) {
            System.out.printf("Aviso: requiredBits=%d > availableBits=%d (+%d bytes). ",
                    requiredBits, availableBits, (requiredBytes - payloadBytes));
        }

        // ===== Montagem do bitstream (estratégia A: próximos records a partir do byte 0) =====
        PayloadAssembler.Assembled asmA =
                PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);
        System.out.printf("Bitstream multi-record montado (%d bytes).%n", asmA.bytes.length);

        // ===== Canonicalização da Huffman =====
        Canon canon = Canon.fromSymbolsAndLengths(symbols, codeLens);

        // ===== Auto-probe (LSB/MSB × invert × shift 0..7) =====
        ProbeResult win = autoProbe(asmA.bytes, requiredBits, canon);
        if (win != null) {
            System.out.printf(">> Auto-probe OK: %s. Exemplo de símbolos:%n", win);
            int[] sample = decodeSample(asmA.bytes, requiredBits, canon, win);
            printSample(sample);
            return;
        }

        System.out.println(">> Preview falhou em todas as combinações. " +
                "Geralmente indica bitShift/bitSense não cobertos ou montagem de payload diferente.");
    }

    // =========================
    // Auto-probe / Preview
    // =========================

    private static ProbeResult autoProbe(byte[] data, long bitLimit, Canon canon) {
        final boolean[] bitOrders = { true, false };    // true=LSB-first, false=MSB-first
        final boolean[] inverts   = { false, true   };  // false=normal, true=invert
        final int MAX_SHIFT = 7;

        for (boolean lsb : bitOrders) {
            for (boolean inv : inverts) {
                for (int shift = 0; shift <= MAX_SHIFT; shift++) {
                    if (tryPreview(data, bitLimit, canon, lsb, inv, shift)) {
                        return new ProbeResult(lsb, inv, shift);
                    }
                }
            }
        }
        return null;
    }

    private static boolean tryPreview(byte[] data,
                                      long bitLimit,
                                      Canon canon,
                                      boolean lsbFirst,
                                      boolean invert,
                                      int bitShift) {
        try {
            BitReader br = new BitReader(data, bitLimit, lsbFirst, invert, bitShift);
            DecodeNode root = canon.buildDecodeTree(lsbFirst);

            final int MAX_SYMS = 64;
            for (int i = 0; i < MAX_SYMS; i++) {
                int sym = decodeOne(root, br);
                if (sym < 0) return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static int[] decodeSample(byte[] data, long bitLimit, Canon canon, ProbeResult pr) {
        BitReader br = new BitReader(data, bitLimit, pr.lsbFirst, pr.invert, pr.bitShift);
        DecodeNode root = canon.buildDecodeTree(pr.lsbFirst);

        final int MAX = 64;
        int[] out = new int[MAX];
        int i = 0;
        for (; i < MAX; i++) {
            int sym = decodeOne(root, br);
            if (sym < 0) break;
            out[i] = sym;
        }
        return (i < MAX) ? Arrays.copyOf(out, i) : out;
    }

    private static void printSample(int[] a) {
        if (a == null || a.length == 0) {
            System.out.println("  (amostra vazia)");
            return;
        }
        StringBuilder sb = new StringBuilder("  ");
        for (int i = 0; i < a.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(a[i]);
        }
        sb.append('\n');
        System.out.print(sb.toString());
    }

    // =========================
    // Decodificação por árvore
    // =========================

    private static int decodeOne(DecodeNode root, BitReader br) {
        DecodeNode n = root;
        while (true) {
            if (n == null) return -1;
            if (n.isLeaf) return n.symbol;
            int b = br.readBit();
            if (b < 0) return -1;
            n = (b == 0 ? n.zero : n.one);
        }
    }

    private static final class DecodeNode {
        boolean isLeaf;
        int symbol;
        DecodeNode zero, one;
    }

    // =========================
    // Leitura de bits
    // =========================

    private static final class BitReader {
        private final byte[] data;
        private final long limitBits;
        private final boolean lsbFirst;
        private final boolean invert;
        private final int startShift;
        private long bitPos;

        BitReader(byte[] data, long limitBits, boolean lsbFirst, boolean invert, int startShift) {
            this.data = Objects.requireNonNull(data);
            this.limitBits = limitBits;
            this.lsbFirst = lsbFirst;
            this.invert = invert;
            this.startShift = startShift & 7;
            this.bitPos = 0;
        }

        int readBit() {
            long pos = bitPos + startShift;
            if (pos >= limitBits) return -1;

            int byteIdx = (int) (pos >>> 3);
            int bitOff  = (int) (pos & 7);

            int v = data[byteIdx] & 0xFF;
            int bit = lsbFirst ? ((v >>> bitOff) & 1) : ((v >>> (7 - bitOff)) & 1);
            if (invert) bit ^= 1;

            bitPos++;
            return bit;
        }
    }

    // =========================
    // Canonical Huffman
    // =========================

    private static final class Canon {
        static final class Entry {
            final int symbol;
            final int len;
            int codeMSB;
            Entry(int symbol, int len) { this.symbol = symbol; this.len = len; }
        }

        final List<Entry> entries;
        final int minLen, maxLen;

        private Canon(List<Entry> entries, int minLen, int maxLen) {
            this.entries = entries;
            this.minLen = minLen;
            this.maxLen = maxLen;
        }

        static Canon fromSymbolsAndLengths(int[] symbols, int[] lens) {
            if (symbols.length != lens.length) {
                throw new IllegalArgumentException("symbols.length != lens.length");
            }
            List<Entry> list = new ArrayList<>();
            int minL = Integer.MAX_VALUE, maxL = 0;
            for (int i = 0; i < symbols.length; i++) {
                int L = lens[i];
                if (L <= 0) continue;
                list.add(new Entry(symbols[i], L));
                if (L < minL) minL = L;
                if (L > maxL) maxL = L;
            }
            if (list.isEmpty()) throw new IllegalArgumentException("Nenhuma entrada com L>0.");

            // ordena por (len, symbol)
            list.sort(Comparator.<Entry>comparingInt(e -> e.len).thenComparingInt(e -> e.symbol));

            // códigos canônicos MSB-first
            int code = 0, prevLen = list.get(0).len;
            for (Entry e : list) {
                if (e.len > prevLen) {
                    code <<= (e.len - prevLen);
                    prevLen = e.len;
                }
                e.codeMSB = code;
                code++;
            }
            return new Canon(list, minL, maxL);
        }

        DecodeNode buildDecodeTree(boolean lsbFirst) {
            DecodeNode root = new DecodeNode();
            for (Entry e : entries) {
                int bits = e.codeMSB;
                if (lsbFirst) bits = bitReverse(e.codeMSB, e.len);
                DecodeNode n = root;
                for (int i = e.len - 1; i >= 0; i--) {
                    int b = (bits >>> i) & 1;
                    DecodeNode next = (b == 0) ? n.zero : n.one;
                    if (next == null) {
                        next = new DecodeNode();
                        if (b == 0) n.zero = next; else n.one = next;
                    }
                    n = next;
                }
                n.isLeaf = true;
                n.symbol = e.symbol;
            }
            return root;
        }

        private static int bitReverse(int v, int width) {
            int r = 0;
            for (int i = 0; i < width; i++) {
                r = (r << 1) | (v & 1);
                v >>>= 1;
            }
            return r;
        }
    }

    // =========================
    // Utils
    // =========================

    private static int[] ensureIntArray(int[] arr, String name) {
        if (arr == null) throw new IllegalStateException(name + " é null.");
        return arr;
    }

    private static final class ProbeResult {
        final boolean lsbFirst;
        final boolean invert;
        final int bitShift;
        ProbeResult(boolean lsbFirst, boolean invert, int bitShift) {
            this.lsbFirst = lsbFirst;
            this.invert = invert;
            this.bitShift = bitShift;
        }
        @Override public String toString() {
            return String.format("bits=%s, invert=%s, shift=%d",
                    (lsbFirst ? "LSB" : "MSB"), invert ? "true" : "false", bitShift);
        }
    }
}
