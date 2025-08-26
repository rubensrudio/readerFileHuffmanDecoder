package com.cmpio;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Stage 2: usa HuffmanDecoder16 para contar tokens por bloco
 *  e checar consistência bits-consumidos vs Block_Sizes.
 */
public final class Stage2Analyzer {

    public static final class Result {
        public final int[] tokensPerBlock = new int[64];
        public final boolean[] folded     = new boolean[64];

        public int blocksAnalyzed;
        public int blocksFolded;
        public int blocksNonZero;

        public int    minTokens = Integer.MAX_VALUE;
        public int    maxTokens = Integer.MIN_VALUE;
        public double avgTokens;

        public long requiredBits;
        public long availableBits;
        public String mode;  // info para diagnóstico

        @Override public String toString() {
            return "Stage2Analyzer.Result{" +
                    "blocksAnalyzed=" + blocksAnalyzed +
                    ", blocksFolded=" + blocksFolded +
                    ", blocksNonZero=" + blocksNonZero +
                    ", minTokens=" + minTokens +
                    ", maxTokens=" + maxTokens +
                    ", avgTokens=" + String.format("%.2f", avgTokens) +
                    ", requiredBits=" + requiredBits +
                    ", availableBits=" + availableBits +
                    ", mode=" + mode +
                    '}';
        }
    }

    /** Analisa um registro de segmento já parseado. */
    public static Result analyze(SegmentRecord rec) {
        if (rec == null) throw new IllegalArgumentException("rec == null");
        if (rec.huffman == null) throw new IllegalArgumentException("rec.huffman == null");

        // 1) Reconstrói a tabela canônica (16-bit alphabet)
        final int N = rec.huffman.symbolCount;
        if (N <= 0 || N > 256) {
            throw new IllegalStateException("Bad Huffman symbolCount N=" + N);
        }
        // codeLengths: N bytes -> int[]
        final int[] lengths = new int[N];
        for (int i = 0; i < N; i++) lengths[i] = Byte.toUnsignedInt(rec.huffman.codeLengths[i]);

        // symbols16: N uint16 -> int[]
        if (rec.huffman.symbols16 == null || rec.huffman.symbols16.length != N) {
            throw new IllegalStateException("SegmentRecord.HuffmanTable must expose symbols16[uint16] with N elements");
        }
        final int[] symbols16 = Arrays.copyOf(rec.huffman.symbols16, N);

        final HuffmanDecoder16 dec = HuffmanDecoder16.fromCanonical(lengths, symbols16);

        // 2) Extrai payload (bitstream concatenado dos blocos)
        ByteBuffer payload = rec.compressedStreamSlice.duplicate();
        payload.clear();
        byte[] data = new byte[payload.remaining()];
        payload.get(data);

        // 3) Sanity: soma dos bits requeridos pelos 64 blocos
        long reqBits = 0;
        for (int i = 0; i < 64; i++) reqBits += (rec.metadata.blockSizesBits[i] & 0xFFFFL);
        long availBits = (long) data.length * 8L;

        Result r = new Result();
        r.requiredBits  = reqBits;
        r.availableBits = availBits;
        r.mode = "HuffmanDecoder16(msb-first, canonical (len,sym))";

        if (reqBits > availBits) {
            throw new IllegalStateException("Segment payload truncated: required " + reqBits +
                    " bits, available " + availBits + " bits");
        }

        // 4) Decodifica bloco a bloco
        BitReader stream = new BitReader(data, 0, (int) reqBits, true); // MSB-first
        int nonZero = 0, folded = 0;
        int minTok = Integer.MAX_VALUE, maxTok = Integer.MIN_VALUE; long sumTok = 0;

        for (int b = 0; b < 64; b++) {
            int bits = rec.metadata.blockSizesBits[b] & 0xFFFF;
            if (bits == 0) {
                r.tokensPerBlock[b] = 0;
                r.folded[b] = true;
                folded++;
                continue;
            }
            nonZero++;

            BitReader slice = stream.sliceBits(bits);
            int tokens = 0;
            int consumed = 0;

            // No formato "1 símbolo por coeficiente" esperamos 512 símbolos.
            // Ainda assim, contamos de forma geral.
            for (int i = 0; i < 512; i++) {
                int sym = dec.decode(slice); // 0..65535
                // se você quiser recuperar o coeficiente quantizado: (short)(sym - 32768)
                tokens++;
                consumed += dec.lastLength();
                if (slice.remainingBits() == 0) break; // protege contra blocks menores (não usual)
                if (tokens > 200_000) throw new IllegalStateException("Unreasonable token count");
            }

            // Consistência: bits consumidos = tamanho do bloco
            if (consumed != bits) {
                // Se sobrou bit, drena os últimos símbolos até zerar; se faltar, é erro
                while (slice.remainingBits() > 0 && tokens < 200_000) {
                    dec.decode(slice);
                    tokens++;
                    consumed += dec.lastLength();
                }
                if (consumed != bits) {
                    throw new IllegalStateException("Block " + b +
                            ": consumed " + consumed + " bits, expected " + bits);
                }
            }

            r.tokensPerBlock[b] = tokens;
            if (tokens < minTok) minTok = tokens;
            if (tokens > maxTok) maxTok = tokens;
            sumTok += tokens;
        }

        r.blocksAnalyzed = 64;
        r.blocksFolded   = folded;
        r.blocksNonZero  = nonZero;
        if (nonZero == 0) {
            r.minTokens = r.maxTokens = 0; r.avgTokens = 0.0;
        } else {
            r.minTokens = minTok; r.maxTokens = maxTok; r.avgTokens = sumTok / (double) nonZero;
        }

        return r;
    }

    /** Heurística: se ≥80% dos blocos não dobrados tiverem 512 tokens, é 1 símbolo/coeficiente. */
    public static boolean looksLikeOneSymbolPerCoefficient(Result r) {
        if (r.blocksNonZero == 0) return true;
        int eq512 = 0;
        for (int b = 0; b < 64; b++) {
            if (!r.folded[b] && r.tokensPerBlock[b] == 512) eq512++;
        }
        return eq512 * 1.0 / r.blocksNonZero >= 0.8;
    }
}
