package com.cmpio;

import java.util.Arrays;

/**
 * Decodificador Huffman canônico com suporte a leitura de bits:
 *  - ordem MSB ou LSB
 *  - invert (flip) dos bits
 *  - bit-shift 0..7 (alinhamento inicial)
 *
 * Uso:
 *   HuffmanStreamDecoder d = HuffmanStreamDecoder.fromCanonical(
 *       symbols, codeLengths, bitstreamBytes, totalBits, lsb=false, invert=true, shift=0);
        *   int sym;
 *   while ((sym = d.next()) >= 0) { ... }
        */
public final class HuffmanStreamDecoder {

    // ---------- construção canônica ----------
    public static HuffmanStreamDecoder fromCanonical(
            int[] symbols, int[] codeLengths,
            byte[] bitstream, long totalBits,
            boolean lsb, boolean invert, int shift) {

        if (symbols.length != codeLengths.length)
            throw new IllegalArgumentException("symbols e codeLengths com tamanhos diferentes.");

        // monta códigos canônicos (ordena por (len, symbol))
        Entry[] entries = buildCanonical(symbols, codeLengths);

        // constrói árvore de decodificação no espaço de bits selecionado
        Node root = buildDecodeTree(entries, lsb);

        return new HuffmanStreamDecoder(root, bitstream, totalBits, lsb, invert, shift);
    }

    private static Entry[] buildCanonical(int[] symbols, int[] lens) {
        int n = symbols.length;
        Entry[] es = new Entry[n];
        int k = 0;
        for (int i = 0; i < n; i++) {
            int L = lens[i];
            if (L <= 0) continue; // ignora códigos de tamanho 0
            if (L > 15) throw new IllegalArgumentException("Comprimento > 15 não suportado: L=" + L);
            es[k++] = new Entry(symbols[i], L);
        }
        es = Arrays.copyOf(es, k);
        Arrays.sort(es, (a,b) -> a.len != b.len ? Integer.compare(a.len, b.len)
                : Integer.compare(a.sym, b.sym));
        // varre gerando os códigos MSB-first
        int code = 0;
        int prevLen = es.length > 0 ? es[0].len : 0;
        for (Entry e : es) {
            if (e.len > prevLen) {
                code <<= (e.len - prevLen);
                prevLen = e.len;
            }
            e.codeMSB = code;
            code++;
        }
        return es;
    }

    private static Node buildDecodeTree(Entry[] es, boolean lsb) {
        Node root = new Node();
        for (Entry e : es) {
            int bits = lsb ? bitReverse(e.codeMSB, e.len) : e.codeMSB;
            Node n = root;
            for (int i = e.len - 1; i >= 0; i--) {
                int b = (bits >>> i) & 1;
                Node nx = (b == 0) ? n.zero : n.one;
                if (nx == null) {
                    nx = new Node();
                    if (b == 0) n.zero = nx; else n.one = nx;
                }
                n = nx;
            }
            n.leaf = true;
            n.symbol = e.sym;
        }
        return root;
    }

    private static int bitReverse(int v, int w) {
        int r = 0;
        for (int i = 0; i < w; i++) { r = (r << 1) | (v & 1); v >>>= 1; }
        return r;
    }

    private static final class Entry {
        final int sym; final int len; int codeMSB;
        Entry(int s, int l){ sym=s; len=l; }
    }

    private static final class Node {
        boolean leaf; int symbol; Node zero, one;
    }

    // ---------- instância / leitura de bits ----------
    private final Node root;
    private final byte[] data;
    private final long limitBits;
    private final boolean lsb;
    private final boolean invert;
    private final int shift; // 0..7
    private long pos;        // posição em bits (já incluindo shift)

    private HuffmanStreamDecoder(Node root, byte[] data, long totalBits,
                                 boolean lsb, boolean invert, int shift) {
        this.root   = root;
        this.data   = data;
        this.limitBits = Math.min(totalBits, (long) data.length * 8L);
        this.lsb    = lsb;
        this.invert = invert;
        this.shift  = shift & 7;
        this.pos    = 0;
    }

    /** Lê o próximo símbolo; retorna -1 quando não há mais bits suficientes. */
    public int next() {
        Node n = root;
        while (true) {
            if (n == null) return -1;
            if (n.leaf) return n.symbol;
            int b = readBit();
            if (b < 0) return -1;
            n = (b == 0) ? n.zero : n.one;
        }
    }

    /** Lê 1 bit conforme configuração (MSB/LSB, invert, shift). */
    private int readBit() {
        long p = pos + shift;
        if (p >= limitBits) return -1;
        int byteIndex = (int) (p >>> 3);
        int bitOff    = (int) (p & 7);
        int v = data[byteIndex] & 0xFF;

        int bit = lsb ? ((v >>> bitOff) & 1) : ((v >>> (7 - bitOff)) & 1);
        if (invert) bit ^= 1;

        pos++;
        return bit;
    }

    /** Quantos bits restam (aprox.). */
    public long remainingBits() {
        long p = pos + shift;
        return p >= limitBits ? 0 : (limitBits - p);
    }
}
