package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Stage 2 – valida a tabela local, monta o bitstream multi-record e faz um preview de decodificação.
 * Configurado para usar LSB, invert=false, shift=0 (modo validado no seu arquivo).
 */
public final class Stage2Analyzer {

    private Stage2Analyzer() {}

    /**
     * Analisa o segmento já parseado, faz a montagem multi-record e tenta decodificar alguns símbolos.
     *
     * @param fileBuf      buffer do arquivo inteiro
     * @param recStart     offset no arquivo do record que contém o segmento selecionado
     * @param order        ordem de bytes para leitura do METADATA (BIG_ENDIAN no seu caso)
     * @param rec          SegmentRecord resultante de SegmentRecord.parse(fileBuf, recStart, order)
     */
    public static void analyze(ByteBuffer fileBuf, int recStart, ByteOrder order, SegmentRecord rec) {
        // 1) Info de metadata e huffman
        final long requiredBits = rec.metadata.sumBits();
        final int  availableBits = rec.payloadSlice.remaining() * 8;

        System.out.printf("Metadata: minDelta=%.6f  maxDelta=%.6f  totalBits=%d  (payloadBytes=%d)%n",
                rec.metadata.minDelta, rec.metadata.maxDelta, requiredBits,
                rec.payloadSlice.remaining());

        final int N = rec.huffman.symbolCount;
        final int maxLen = max(rec.huffman.codeLengths);
        final int nonZero = countNonZero(rec.huffman.codeLengths);
        System.out.printf("Huffman N=%d, maxlen=%d, nonZeroLens=%d, kraftOk=%s%n",
                N, maxLen, nonZero, CmpSanity.kraftOk(rec.huffman.codeLengths));

        int[] hist = lengthsHistogram(rec.huffman.codeLengths);
        System.out.printf("Lengths histogram: %s%n", histogramToString(hist));

        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, rec.huffman.payloadStart);

        // 2) Monta bitstream multi-record se necessário
        PayloadAssembler.Assembled asm = PayloadAssembler.assemble(
                fileBuf, recStart, order, rec, requiredBits);

        int asmBits = asm.bytes.length * 8;
        if (asmBits < requiredBits) {
            System.out.printf("Aviso: ainda faltam %d bits após assemble (bytes=%d).%n",
                    (requiredBits - asmBits), asm.bytes.length);
        } else if (availableBits < requiredBits) {
            System.out.printf("Aviso: requiredBits=%d > availableBits=%d (+%d bytes). Bitstream multi-record montado (%d bytes).%n",
                    requiredBits, availableBits, ((requiredBits - availableBits) + 7) >>> 3, asm.bytes.length);
        }

        // 3) Preview de decodificação
        //    Modo já confirmado anteriormente: LSB, invert=false, shift=0
        try {
            PreviewResult pr = previewDecodeLSB(asm.bytes, (int)requiredBits, rec.huffman, 16);
            if (pr.ok) {
                System.out.printf(">> Preview OK: %d símbolos decodificados com LSB (invert=false, shift=0)%n", pr.decoded);
                System.out.print("   amostra: ");
                for (int i = 0; i < pr.decoded; i++) {
                    System.out.print(pr.sample[i]);
                    if (i + 1 < pr.decoded) System.out.print(", ");
                }
                System.out.println();
            } else {
                System.out.println(">> Preview falhou mesmo em LSB/invert=false/shift=0 (isso não era esperado).");
            }
        } catch (Exception ex) {
            System.out.println(">> Preview exception: " + ex.getMessage());
        }
    }

    /* ===================== PREVIEW – Huffman LSB ===================== */

    private static final class Node {
        int sym = -1;
        Node z; // bit 0
        Node o; // bit 1
    }

    private static final class PreviewResult {
        boolean ok;
        int decoded;
        int[] sample;
    }

    private static PreviewResult previewDecodeLSB(byte[] bytes, int maxBits, SegmentRecord.HuffmanTable ht, int wantSymbols) {
        // 1) Constrói códigos canônicos
        CanonicalCodes codes = buildCanonicalCodes(ht.codeLengths, ht.symbols);

        // 2) Para LSB-first, precisamos inserir os códigos REVERSOS na trie (e depois ler bits na ordem LSB)
        Node root = new Node();
        for (int i = 0; i < codes.count; i++) {
            int L = codes.lengths[i];
            if (L == 0) continue;
            int code = codes.codes[i];
            int rev = reverseBits(code, L);
            int sym = codes.symbols[i];
            Node n = root;
            for (int b = L - 1; b >= 0; b--) {
                int bit = (rev >>> b) & 1; // caminhar MSB->LSB do código já reverso
                n = (bit == 0) ? (n.z == null ? (n.z = new Node()) : n.z)
                        : (n.o == null ? (n.o = new Node()) : n.o);
            }
            n.sym = sym;
        }

        // 3) Lê bits LSB-first (sem inversão, shift=0) e caminha na trie
        BitReaderLSB r = new BitReaderLSB(bytes);
        PreviewResult pr = new PreviewResult();
        pr.sample = new int[Math.min(wantSymbols, 64)];
        int produced = 0;
        while (produced < pr.sample.length && r.bitPos < maxBits) {
            Node n = root;
            while (n.sym < 0) {
                int bit = r.readBit();
                if (bit < 0) { pr.ok = false; pr.decoded = produced; return pr; }
                n = (bit == 0) ? n.z : n.o;
                if (n == null) { pr.ok = false; pr.decoded = produced; return pr; }
            }
            pr.sample[produced++] = n.sym;
        }
        pr.ok = produced > 0;
        pr.decoded = produced;
        return pr;
    }

    private static final class BitReaderLSB {
        final byte[] a;
        int bitPos = 0; // posição em bits desde o início do array
        BitReaderLSB(byte[] a) { this.a = a; }
        int readBit() {
            int byteIndex = bitPos >>> 3;
            if (byteIndex >= a.length) return -1;
            int bitIndex = bitPos & 7;      // 0..7
            int v = a[byteIndex] & 0xFF;
            int bit = (v >>> bitIndex) & 1; // LSB-first
            bitPos++;
            return bit;
        }
    }

    private static final class CanonicalCodes {
        final int count;
        final int[] codes;    // código canônico (MSB) por i
        final int[] lengths;  // comprimento por i
        final int[] symbols;  // símbolo por i
        CanonicalCodes(int c, int[] codes, int[] lengths, int[] symbols){
            this.count=c; this.codes=codes; this.lengths=lengths; this.symbols=symbols;
        }
    }

    /** Constrói códigos canônicos padrão (ordem: por L crescente, e dentro do mesmo L por ordem de 'symbols'). */
    private static CanonicalCodes buildCanonicalCodes(int[] lens, int[] syms) {
        int n = lens.length;
        // Ordena (L, símbolo) mas preserva o mapeamento
        int[][] pairs = new int[n][3]; // [L, sym, idx]
        int count = 0;
        for (int i=0;i<n;i++) {
            int L = lens[i];
            if (L == 0) continue;
            pairs[count][0] = L;
            pairs[count][1] = syms[i] & 0xFF;
            pairs[count][2] = i;
            count++;
        }
        Arrays.sort(pairs, 0, count, (a,b) -> {
            int d = Integer.compare(a[0], b[0]);
            return (d != 0) ? d : Integer.compare(a[1], b[1]);
        });

        int[] codes   = new int[count];
        int[] lengths = new int[count];
        int[] symbols = new int[count];

        int code = 0;
        int prevLen = (count > 0 ? pairs[0][0] : 0);

        for (int i=0;i<count;i++) {
            int L = pairs[i][0];
            int sym = pairs[i][1];

            if (i == 0) {
                code = 0;
                prevLen = L;
            } else {
                if (L == prevLen) {
                    code += 1;
                } else {
                    code = (code + 1) << (L - prevLen);
                    prevLen = L;
                }
            }

            codes[i]   = code;
            lengths[i] = L;
            symbols[i] = sym;
        }

        return new CanonicalCodes(count, codes, lengths, symbols);
    }

    private static int reverseBits(int v, int len) {
        int r = 0;
        for (int i=0;i<len;i++) {
            r = (r << 1) | (v & 1);
            v >>>= 1;
        }
        return r;
    }

    /* ===================== Utils ===================== */

    private static int max(int[] a){ int m=0; for(int x: a) if(x>m) m=x; return m; }
    private static int countNonZero(int[] a){ int c=0; for(int x: a) if(x!=0) c++; return c; }

    private static int[] lengthsHistogram(int[] lens) {
        int max = 0;
        for (int L : lens) if (L > max) max = L;
        int[] h = new int[Math.max(16, max+1)];
        for (int L : lens) h[L]++;
        return h;
    }

    private static String histogramToString(int[] h) {
        StringBuilder sb = new StringBuilder();
        for (int L=0; L<h.length; L++) {
            if (h[L] != 0) {
                if (sb.length() != 0) sb.append(' ');
                sb.append(L).append(':').append(h[L]);
            }
        }
        return sb.length()==0 ? "(vazio)" : sb.toString();
    }
}
