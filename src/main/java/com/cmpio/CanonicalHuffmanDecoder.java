package com.cmpio;

import java.util.Arrays;

/** Decoder de Huffman canônico com:
 *  - políticas de desempate,
 *  - opção de reverter os bits do codeword (bit-reversed),
 *  - validações de comprimentos.
 */
final class CanonicalHuffmanDecoder {
    private static final class Node {
        int sym = -1;
        Node zero, one;
    }

    private final Node root = new Node();

    /** Critérios de desempate quando o comprimento é igual. */
    enum TieBreak { BY_SYMBOL_VALUE, BY_INPUT_ORDER }

    /**
     * @param codeLengths comprimentos (uint8[N])
     * @param symbols     valores dos símbolos (uint8[N])
     * @param tieBreak    desempate entre símbolos de mesmo comprimento
     * @param reverseCodes se true, inverte os bits de cada codeword antes de inserir na árvore
     */
    CanonicalHuffmanDecoder(byte[] codeLengths, byte[] symbols,
                            TieBreak tieBreak, boolean reverseCodes) {
        final int N = codeLengths.length;
        if (symbols.length != N) throw new IllegalArgumentException("Lengths and symbols must match");

        // índices 0..N-1 ordenados por (comprimento, empate)
        Integer[] idx = new Integer[N];
        for (int i = 0; i < N; i++) idx[i] = i;

        Arrays.sort(idx, (a, b) -> {
            int la = codeLengths[a] & 0xFF, lb = codeLengths[b] & 0xFF;
            if (la != lb) return Integer.compare(la, lb);
            return (tieBreak == TieBreak.BY_SYMBOL_VALUE)
                    ? Integer.compare(symbols[a] & 0xFF, symbols[b] & 0xFF)
                    : Integer.compare(a, b);
        });

        // histograma → firstCode[len]
        int maxLen = 0;
        int[] lenCount = new int[257];
        for (int i = 0; i < N; i++) {
            int L = codeLengths[i] & 0xFF;
            if (L == 0) continue;               // símbolo não usado
            if (L < 1 || L > 32)
                throw new IllegalArgumentException("Invalid code length L=" + L + " (must be 1..32)");
            lenCount[L]++;
            if (L > maxLen) maxLen = L;
        }
        int[] firstCode = new int[maxLen + 2];
        int code = 0;
        for (int L = 1; L <= maxLen; L++) {
            code = (code + lenCount[L - 1]) << 1;
            firstCode[L] = code;
        }

        // atribui códigos canônicos; insere na árvore (com reversão opcional)
        int[] nextCode = Arrays.copyOf(firstCode, firstCode.length);
        for (int k = 0; k < N; k++) {
            int i = idx[k];
            int L = codeLengths[i] & 0xFF;
            if (L == 0) continue;
            int c = nextCode[L]++;
            if (reverseCodes) c = reverseBits(c, L);
            insert(symbols[i] & 0xFF, c, L);
        }
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

    /** Decodifica 1 símbolo. */
    int decode(BitReader br) {
        Node n = root;
        while (n.sym < 0) {
            int bit = br.read1();
            if (bit < 0) throw new IllegalStateException("Unexpected end of bitstream");
            n = (bit == 0) ? n.zero : n.one;
            if (n == null) throw new IllegalStateException("Invalid Huffman code");
        }
        return n.sym;
    }
}
