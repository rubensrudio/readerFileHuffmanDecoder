package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Stage 3: decodifica o bitstream completo em tokens e imprime
 * amostra + histograma + estatísticas de tokenização.
 *
 * Este util le:
 *   - SegmentRecord (tabela Huffman + payloadStart + totalBits)
 *   - Monta o bitstream multi-record via PayloadAssembler
 *   - Auto-probe (MSB/LSB × invert × shift 0..3)
 *   - Decodifica tudo com HuffmanStreamDecoder
 *
 * API principal:
 *   Stage3SymbolDump.run(file, recStart, order, rec);
 */
public final class Stage3SymbolDump {

    private Stage3SymbolDump(){}

    public static void run(ByteBuffer file, int recStart, ByteOrder order, SegmentRecord rec) {
        // 1) Monta bitstream multi-record até totalBits
        long requiredBits = rec.md.totalBits;
        PayloadAssembler.Assembled assembled =
                PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

        byte[] stream = assembled.bytes;
        long availableBits = Math.min(requiredBits, (long) stream.length * 8L);

        // 2) Auto-probe em cima do stream costurado
        Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, stream, availableBits);

        // 3) Decodifica tudo
        HuffmanStreamDecoder dec = HuffmanStreamDecoder.fromCanonical(
                rec.huffman.symbols, rec.huffman.lens,
                stream, requiredBits,
                pick.lsb, pick.invert, pick.shift);

        final int MAX_SAMPLE = 256;
        int[] sample = new int[Math.min(MAX_SAMPLE, (int) (requiredBits / 2))]; // tamanho arbitrário de amostra
        int   sampleCount = 0;

        int[] hist = new int[256]; // símbolos presumidos 0..255
        long  totalSyms = 0;

        while (true) {
            int s = dec.next();
            if (s < 0) break;
            totalSyms++;
            if (s >= 0 && s < hist.length) hist[s]++;
            if (sampleCount < sample.length) sample[sampleCount++] = s;
        }

        // 4) Relatório
        System.out.printf("=== Stage 3 ===%n");
        System.out.printf("Auto-probe escolhido: bits-%s, invert=%s, shift=%d%n",
                pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);
        System.out.printf("Decodificados: %d símbolos (%.2f bits/símbolo em média)%n",
                totalSyms, totalSyms == 0 ? 0.0 : (double) requiredBits / (double) totalSyms);

        // amostra
        System.out.print("Amostra: ");
        for (int i = 0; i < sampleCount; i++) {
            if (i > 0) System.out.print(", ");
            System.out.print(sample[i]);
        }
        System.out.println();

        // top-N do histograma
        int topN = 16;
        int[] idx = argsortDesc(hist);
        System.out.println("Top " + topN + " símbolos:");
        for (int i = 0, shown = 0; i < idx.length && shown < topN; i++) {
            if (hist[idx[i]] == 0) break;
            System.out.printf("  %3d: %d%n", idx[i], hist[idx[i]]);
            shown++;
        }

        // sugestões simples para tokenização
        // (ajuda a decidir próximos passos — RLE, marcadores, etc.)
        int zero = (0 < hist.length) ? hist[0] : 0;
        int one  = (1 < hist.length) ? hist[1] : 0;
        System.out.printf("Heurísticas: zero=%d, one=%d, únicos>0=%d%n",
                zero, one, countPos(hist));
        System.out.println("Se um pequeno conjunto domina (ex.: 0/1/63/95/195/249), provável tokenização com marcadores fixos.");
    }

    // ---------- Auto-probe leve no bitstream inteiro ----------
    private static Probe autoProbe(int[] symbols, int[] lens, byte[] stream, long limitBits) {
        // testamos MSB/LSB × invert × shift 0..3
        boolean[] orders = { true, false }; // lsb?
        boolean[] invs   = { false, true };
        Probe best = null;

        for (boolean lsb : orders) {
            for (boolean inv : invs) {
                for (int shift = 0; shift <= 3; shift++) {
                    int score = scoreConfig(symbols, lens, stream, limitBits, lsb, inv, shift);
                    if (best == null || score > best.score) {
                        best = new Probe(lsb, inv, shift, score);
                    }
                }
            }
        }
        if (best == null) best = new Probe(false, false, 0, 0);
        return best;
    }

    // decodifica um prefixo curto e mede quantos símbolos válidos saem
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
            if (s >= 0 && s < 256 && seen[s] == 0) { seen[s] = 1; distinct++; }
        }
        // favorece combos que decodificam >64 símbolos e tem diversidade
        return ok + distinct * 2;
    }

    private static int[] argsortDesc(int[] v) {
        Integer[] idx = new Integer[v.length];
        for (int i=0;i<v.length;i++) idx[i] = i;
        Arrays.sort(idx, (a,b) -> Integer.compare(v[b], v[a]));
        int[] out = new int[v.length];
        for (int i=0;i<v.length;i++) out[i] = idx[i];
        return out;
    }

    private static int countPos(int[] v) {
        int c=0; for (int x : v) if (x>0) c++; return c;
    }

    // ---------- POJO ----------
    private static final class Probe {
        final boolean lsb; final boolean invert; final int shift; final int score;
        Probe(boolean lsb, boolean invert, int shift, int score){
            this.lsb=lsb; this.invert=invert; this.shift=shift; this.score=score;
        }
    }
}
