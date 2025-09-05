package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Locale;

/**
 * Stage 2: valida a detecção de Huffman/payloadStart e monta o bitstream multi-record.
 * Esta versão usa os campos atuais de SegmentRecord:
 *   - rec.md.payloadStartByte, rec.md.payloadBytes, rec.md.totalBits
 *   - rec.huffman.symbols / lens
 */
public final class Stage2Analyzer {

    private Stage2Analyzer() {}

    public static void run(ByteBuffer file, int recStart, ByteOrder order, SegmentRecord rec) {
        Locale.setDefault(Locale.ROOT);

        // Resumo do record/tabela
        int maxLen = 0, nonZero = 0;
        for (int L : rec.huffman.lens) { if (L>0) { nonZero++; if (L>maxLen) maxLen=L; } }

        System.out.printf("Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=%d, layout=%s, "
                        + "lensEnc=%s, payloadStart=%d%n",
                rec.md.minDelta, rec.md.maxDelta,
                rec.huffman.N, rec.huffman.base, rec.huffman.layout,
                rec.huffman.lensEncoding, rec.md.payloadStartByte);

        // histograma de comprimentos
        int[] hist = new int[maxLen + 1];
        for (int L : rec.huffman.lens) if (L >= 0 && L < hist.length) hist[L]++;
        System.out.printf("Huffman Huffman{N=%d, maxlen=%d, nonZeroLens=%d, kraftOk=%s}%n",
                rec.huffman.N, maxLen, nonZero, rec.huffman.kraftOk);
        System.out.print("Lengths histogram:");
        for (int L=1; L<hist.length; L++) if (hist[L] > 0) System.out.print(" " + L + ":" + hist[L]);
        System.out.println();

        long requiredBits = rec.md.totalBits;
        long availableBits = (long) rec.md.payloadBytes * 8L;
        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, rec.md.payloadStartByte);

        // Montagem multi-record
        PayloadAssembler.Assembled as =
                PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

        long haveBits = (long) as.bytes.length * 8L;
        System.out.printf("Assembler: bytes=%d (bits=%d) %s%n",
                as.bytes.length, haveBits, haveBits >= requiredBits ? "OK" : "INSUFICIENTE");

        // Auto-probe leve para confirmar leitura de bits
        Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, as.bytes, Math.min(haveBits, requiredBits));
        System.out.printf(">> Auto-probe OK: bits=%s, invert=%s, shift=%d.%n",
                pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);
    }

    // ===== auto-probe simples =====
    private static Probe autoProbe(int[] symbols, int[] lens, byte[] stream, long limitBits) {
        boolean[] orders = { true, false }; // lsb?
        boolean[] invs   = { false, true };
        Probe best = null;
        for (boolean lsb : orders) {
            for (boolean inv : invs) {
                for (int shift = 0; shift <= 3; shift++) {
                    int score = scoreConfig(symbols, lens, stream, limitBits, lsb, inv, shift);
                    if (best == null || score > best.score) best = new Probe(lsb, inv, shift, score);
                }
            }
        }
        if (best == null) best = new Probe(false,false,0,0);
        return best;
    }

    private static int scoreConfig(int[] symbols, int[] lens, byte[] stream, long limitBits,
                                   boolean lsb, boolean invert, int shift) {
        HuffmanStreamDecoder d = HuffmanStreamDecoder.fromCanonical(
                symbols, lens, stream, Math.min(limitBits, 4096), lsb, invert, shift);
        int ok = 0, distinct = 0;
        int[] seen = new int[256];
        Arrays.fill(seen, 0);
        for (int i = 0; i < 128; i++) {
            int s = d.next();
            if (s < 0) break;
            ok++;
            if (s>=0 && s<256 && seen[s]==0) { seen[s]=1; distinct++; }
        }
        return ok + 2*distinct;
    }

    private static final class Probe {
        final boolean lsb; final boolean invert; final int shift; final int score;
        Probe(boolean lsb, boolean invert, int shift, int score) {
            this.lsb=lsb; this.invert=invert; this.shift=shift; this.score=score;
        }
    }
}
