package com.cmpio;

import java.util.Arrays;

/**
 * Decoder de Huffman canônico para alfabeto de 16 bits.
 *
 * Uso típico:
 *   int[] lengths = ...; // N comprimentos (uint8 -> 0..255) para cada símbolo presente
 *   int[] symbols = ...; // N valores de símbolo (uint16 -> 0..65535)
 *   HuffmanDecoder16 dec = HuffmanDecoder16.fromCanonical(lengths, symbols);
 *   int s = dec.decode(bitReader); // retorna 0..65535
 *
 * Observações:
 * - Ordenação canônica: (length ASC, symbol ASC).
 * - Códigos atribuídos via histograma de comprimentos (firstCode/nextCode).
 * - Bits lidos MSB-first (o BitReader do projeto já faz isso).
 */
public final class HuffmanDecoder16 {

    /** Nó da trie de decodificação. */
    private static final class Node {
        int sym = -1;      // -1 = não terminal
        Node zero, one;    // filhos
    }

    private final Node root = new Node();
    private int lastLength = 0; // comprimento (em bits) do último símbolo decodificado

    private HuffmanDecoder16() { }

    /** @return comprimento (em bits) consumido pelo último {@link #decode(BitReader)}. */
    public int lastLength() { return lastLength; }

    /**
     * Constrói a árvore canônica a partir dos pares (length, symbol).
     * @param codeLengths comprimentos em bits (N posições). Valores válidos: 1..32; 0 = não usado.
     * @param symbols     valores de símbolo (N posições), 0..65535.
     */
    public static HuffmanDecoder16 fromCanonical(int[] codeLengths, int[] symbols) {
        if (codeLengths == null || symbols == null)
            throw new IllegalArgumentException("lengths/symbols must not be null");
        if (codeLengths.length != symbols.length)
            throw new IllegalArgumentException("lengths and symbols sizes must match");
        final int N = codeLengths.length;

        // Índices [0..N-1] para ordenação estável por (len, symbol)
        Integer[] idx = new Integer[N];
        for (int i = 0; i < N; i++) idx[i] = i;

        // Validação preliminar e cálculo de maxLen
        int maxLen = 0;
        for (int L : codeLengths) {
            if (L < 0 || L > 32) throw new IllegalArgumentException("Invalid code length L=" + L);
            if (L > maxLen) maxLen = L;
        }
        if (maxLen == 0) throw new IllegalArgumentException("Empty Huffman table (all lengths are zero)");

        Arrays.sort(idx, (a, b) -> {
            int la = codeLengths[a], lb = codeLengths[b];
            if (la != lb) return Integer.compare(la, lb);
            return Integer.compare(symbols[a], symbols[b]);
        });

        // Histograma de comprimentos -> firstCode[len]
        int[] lenCount = new int[maxLen + 1];
        for (int L : codeLengths) if (L > 0) lenCount[L]++;

        int[] firstCode = new int[maxLen + 1];
        int code = 0;
        for (int L = 1; L <= maxLen; L++) {
            code = (code + lenCount[L - 1]) << 1;
            firstCode[L] = code;
        }

        // Atribui códigos canônicos e cria a árvore
        int[] nextCode = Arrays.copyOf(firstCode, firstCode.length);
        HuffmanDecoder16 dec = new HuffmanDecoder16();
        for (int k = 0; k < N; k++) {
            int i = idx[k];
            int L = codeLengths[i];
            if (L == 0) continue; // símbolo não usado
            int c = nextCode[L]++;
            dec.insert(symbols[i], c, L);
        }
        return dec;
    }

    /** Insere (code,len) para o símbolo dado na trie. */
    private void insert(int symbol, int code, int len) {
        if (symbol < 0 || symbol > 0xFFFF)
            throw new IllegalArgumentException("Symbol out of range: " + symbol);
        Node n = root;
        for (int i = len - 1; i >= 0; i--) {
            int bit = (code >>> i) & 1;
            n = (bit == 0)
                    ? (n.zero == null ? (n.zero = new Node()) : n.zero)
                    : (n.one  == null ? (n.one  = new Node())  : n.one);
        }
        n.sym = symbol;
    }

    /**
     * Decodifica um símbolo (0..65535) lendo bits MSB-first do {@link BitReader}.
     * Lança IllegalStateException se consumir um prefixo inválido ou se faltar bit.
     */
    public int decode(BitReader br) {
        Node n = root;
        int consumed = 0;
        while (n.sym < 0) {
            int bit = br.read1();
            if (bit < 0) throw new IllegalStateException("Unexpected end of bitstream");
            n = (bit == 0) ? n.zero : n.one;
            consumed++;
            if (n == null) throw new IllegalStateException("Invalid Huffman code");
        }
        lastLength = consumed;
        return n.sym;
    }
}
