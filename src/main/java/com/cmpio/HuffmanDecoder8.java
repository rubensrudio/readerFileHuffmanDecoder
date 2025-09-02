package com.cmpio;

import java.util.Arrays;

/**
 * Decoder de Huffman canônico para alfabeto de 8 bits (0..255).
 * Suporta: tie-break configurável, codewords bit-reversed, e bit sense invertido.
 * Aceita comprimentos 0..32 (0 = símbolo ausente). Leitura via BitReader.
 */
public final class HuffmanDecoder8 {

    public enum TieBreak { BY_SYMBOL_VALUE, BY_INPUT_ORDER }
    private static final int MAX_BITS = 32;

    private static final class Node {
        int sym = -1;   // >=0 = terminal
        Node zero, one;
    }

    private final Node root = new Node();
    private final boolean invertBitSense;
    private int lastLen = 0;

    private HuffmanDecoder8(boolean invertBitSense) {
        this.invertBitSense = invertBitSense;
    }

    /** Comprimento (em bits) consumido pelo último decode. */
    public int lastLength() { return lastLen; }

    /**
     * Constrói a árvore canônica a partir dos (length,symbol).
     * Ordenação canônica: (length ASC, desempate configurável).
     *
     * @param codeLengths  comprimentos (0..32); 0 = símbolo ausente
     * @param symbols      símbolos (0..255)
     * @param tieBreak     desempate quando os comprimentos são iguais
     * @param reverseCodes se true, aplica bit-reverse no codeword antes de inserir
     * @param invertSense  se true, lê os bits invertendo 0↔1
     */
    public static HuffmanDecoder8 fromCanonical(
            int[] codeLengths, int[] symbols,
            TieBreak tieBreak, boolean reverseCodes, boolean invertSense) {

        if (codeLengths == null || symbols == null)
            throw new IllegalArgumentException("lengths/symbols must not be null");
        if (codeLengths.length != symbols.length)
            throw new IllegalArgumentException("lengths and symbols sizes must match");

        final int N = codeLengths.length;
        Integer[] idx = new Integer[N];
        int maxLen = 0, nonZero = 0;

        for (int i = 0; i < N; i++) {
            int L = codeLengths[i];
            if (L < 0 || L > MAX_BITS) throw new IllegalArgumentException("Invalid code length " + L);
            if (L > 0) { nonZero++; if (L > maxLen) maxLen = L; }
            int s = symbols[i];
            if (s < 0 || s > 255) throw new IllegalArgumentException("Symbol out of range: " + s);
            idx[i] = i;
        }
        if (nonZero == 0) throw new IllegalArgumentException("Empty Huffman table (all lengths are zero)");

        Arrays.sort(idx, (a, b) -> {
            int la = codeLengths[a], lb = codeLengths[b];
            if (la != lb) return Integer.compare(la, lb);
            return (tieBreak == TieBreak.BY_SYMBOL_VALUE)
                    ? Integer.compare(symbols[a], symbols[b])
                    : Integer.compare(a, b); // BY_INPUT_ORDER
        });

        // Histograma -> firstCode[len]
        int[] lenCount = new int[maxLen + 1];
        for (int L : codeLengths) if (L > 0) lenCount[L]++;

        int[] firstCode = new int[maxLen + 1];
        int code = 0;
        for (int L = 1; L <= maxLen; L++) {
            code = (code + lenCount[L - 1]) << 1;
            firstCode[L] = code;
        }

        // Atribui códigos
        int[] nextCode = Arrays.copyOf(firstCode, firstCode.length);
        HuffmanDecoder8 dec = new HuffmanDecoder8(invertSense);
        for (int k = 0; k < N; k++) {
            int i = idx[k];
            int L = codeLengths[i];
            if (L == 0) continue;
            int c = nextCode[L]++;
            if (reverseCodes) c = reverseBits(c, L);
            dec.insert(symbols[i], c, L);
        }
        return dec;
    }

    private static int reverseBits(int v, int len) {
        int r = 0;
        for (int i = 0; i < len; i++) { r = (r << 1) | (v & 1); v >>>= 1; }
        return r;
    }

    private void insert(int symbol, int code, int len) {
        Node n = root;
        for (int i = len - 1; i >= 0; i--) {
            int bit = (code >>> i) & 1;
            n = (bit == 0)
                    ? (n.zero == null ? (n.zero = new Node()) : n.zero)
                    : (n.one  == null ? (n.one  = new Node())  : n.one);
        }
        n.sym = symbol;
    }

    /** Decodifica um símbolo (0..255) lendo do BitReader. */
    public int decode(BitReader br) {
        Node n = root;
        int used = 0;
        while (n.sym < 0) {
            int bit = br.read1();
            if (bit < 0) throw new IllegalStateException("Unexpected end of bitstream");
            if (invertBitSense) bit ^= 1;
            n = (bit == 0) ? n.zero : n.one;
            if (n == null) throw new IllegalStateException("Invalid Huffman code");
            used++;
        }
        lastLen = used;
        return n.sym;
    }
}
