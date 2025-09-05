package com.cmpio;

import java.util.Arrays;

/**
 * Decoder de Huffman canônico "streaming" com suporte a:
 *  - ordem de bits MSB-first ou LSB-first
 *  - inversão de bits (bit ^ 1)
 *  - deslocamento inicial (skip) de 'shift' bits para alinhamento
 *
 * API mínima para o Stage4TokenGrouper:
 *   - fromCanonical(...)    -> constrói o decoder
 *   - next()                -> lê próximo símbolo (>=0) ou -1 se acabou
 *
 * Observações:
 *  - Espera comprimentos 1..15 típicos; ignora entradas com L=0.
 *  - Implementa decodificação canônica padrão: para cada L, temos nextCode[L] e baseIndex[L],
 *    então durante a leitura acumulamos 'code' e, quando len=L, verificamos se
 *    code está no intervalo [nextCode[L], nextCode[L]+count[L]-1].
 */
public final class HuffmanStreamDecoder {

    // ===== estrutura do código canônico =====
    private final int maxLen;                // maior comprimento (<= 31 aqui, mas usamos <= 15)
    private final int[] countByLen;          // countByLen[L] = qtd símbolos com comprimento L
    private final int[] nextCode;            // nextCode[L] = menor código canônico no comprimento L
    private final int[] baseIndex;           // baseIndex[L] = índice inicial no array 'perm' para comprimento L
    private final int[] perm;                // símbolos ordenados por (len, ordem de aparição)
    // ===== leitura de bits =====
    private final byte[] data;
    private final long limitBits;            // nº máximo de bits válidos do payload
    private final boolean lsb;               // bit-order dentro do byte
    private final boolean invert;            // inverte bit
    private long bitPos;                     // posição global em bits (0..limitBits)
    private int curByte;                     // cache do byte atual (0..255)
    private int curByteIndex;                // índice do byte atual
    private int bitIndexInByte;              // posição do próximo bit no byte (0..7) conforme ordem lsb/msb

    private HuffmanStreamDecoder(
            int maxLen, int[] countByLen, int[] nextCode, int[] baseIndex, int[] perm,
            byte[] data, long limitBits, boolean lsb, boolean invert, int initialSkipBits
    ) {
        this.maxLen      = maxLen;
        this.countByLen  = countByLen;
        this.nextCode    = nextCode;
        this.baseIndex   = baseIndex;
        this.perm        = perm;
        this.data        = data;
        this.limitBits   = Math.min(limitBits, (long)data.length * 8L);
        this.lsb         = lsb;
        this.invert      = invert;

        this.bitPos = 0;
        this.curByteIndex = 0;
        this.curByte = (data.length > 0) ? (data[0] & 0xFF) : 0;

        // define posição do primeiro bit dentro do byte
        this.bitIndexInByte = lsb ? 0 : 7;

        // aplica skip inicial (shift) para alinhamento
        for (int i = 0; i < initialSkipBits; i++) readBit();
    }

    /**
     * Constrói a partir de tabela canônica (símbolos + comprimentos).
     * 'lens' é o comprimento de código (0..15); L=0 significa "não presente".
     */
    public static HuffmanStreamDecoder fromCanonical(
            int[] symbols, int[] lens,
            byte[] payload, long limitBits,
            boolean lsb, boolean invert, int shift
    ) {
        if (symbols == null || lens == null || symbols.length != lens.length) {
            throw new IllegalArgumentException("symbols/lens inválidos.");
        }

        // 1) filtra símbolos com L>0 e computa maxLen
        int n = symbols.length;
        int maxLen = 0;
        for (int i = 0; i < n; i++) {
            if (lens[i] < 0) throw new IllegalArgumentException("Comprimento negativo em i=" + i);
            if (lens[i] > maxLen) maxLen = lens[i];
        }
        if (maxLen == 0) {
            // nenhuma entrada
            return new HuffmanStreamDecoder(0, new int[1], new int[1], new int[1], new int[0],
                    payload, limitBits, lsb, invert, Math.max(0, shift));
        }

        // 2) countByLen
        int[] countByLen = new int[maxLen + 1];
        for (int i = 0; i < n; i++) {
            int L = lens[i];
            if (L > 0) countByLen[L]++;
        }

        // 3) perm: símbolos ordenados por (len asc, ordem de aparição)
        int total = 0;
        int[] baseIndex = new int[maxLen + 1];
        Arrays.fill(baseIndex, 0);
        for (int L = 1; L <= maxLen; L++) {
            baseIndex[L] = total;
            total += countByLen[L];
        }
        int[] nextIdx = Arrays.copyOf(baseIndex, baseIndex.length);
        int[] perm = new int[total];
        for (int i = 0; i < n; i++) {
            int L = lens[i];
            if (L > 0) {
                int dst = nextIdx[L]++;
                perm[dst] = symbols[i];
            }
        }

        // 4) nextCode[L] canônico
        int[] nextCode = new int[maxLen + 1];
        int code = 0;
        for (int L = 1; L <= maxLen; L++) {
            code = (code + countByLen[L - 1]) << 1;
            nextCode[L] = code;
        }

        // pronto
        return new HuffmanStreamDecoder(
                maxLen, countByLen, nextCode, baseIndex, perm,
                payload, limitBits, lsb, invert, Math.max(0, Math.min(7, shift))
        );
    }

    /**
     * Retorna próximo símbolo decodificado, ou -1 quando atinge o limite de bits
     * ou quando não há mais códigos válidos a serem lidos.
     */
    public int next() {
        if (maxLen == 0) return -1;

        int code = 0;
        for (int L = 1; L <= maxLen; L++) {
            int bit = readBit();
            if (bit < 0) return -1;
            code = (code << 1) | bit;

            int firstCode = nextCode[L];
            int cnt = countByLen[L];
            if (cnt > 0) {
                int lastCode = firstCode + cnt - 1;
                if (code >= firstCode && code <= lastCode) {
                    int off = code - firstCode;
                    int idx = baseIndex[L] + off;
                    if (idx >= 0 && idx < perm.length) {
                        return perm[idx];
                    }
                    return -1; // index fora: stream inválido
                }
            }
        }
        // nenhuma correspondência no maxLen => stream inválido ou acabou
        return -1;
    }

    // ===== leitura de 1 bit conforme config =====
    // Retorna 0/1 ou -1 se atingiu limitBits.
    private int readBit() {
        if (bitPos >= limitBits) return -1;
        if (data.length == 0) return -1;

        int bit;
        if (!lsb) {
            // MSB-first
            bit = (curByte >> bitIndexInByte) & 1;
            if (invert) bit ^= 1;

            advanceBitMSB();
        } else {
            // LSB-first
            bit = (curByte >> bitIndexInByte) & 1;
            if (invert) bit ^= 1;

            advanceBitLSB();
        }
        bitPos++;
        return bit;
    }

    private void advanceBitMSB() {
        bitIndexInByte--;
        if (bitIndexInByte < 0) {
            curByteIndex++;
            if (curByteIndex < data.length) {
                curByte = data[curByteIndex] & 0xFF;
            } else {
                curByte = 0;
            }
            bitIndexInByte = 7;
        }
    }

    private void advanceBitLSB() {
        bitIndexInByte++;
        if (bitIndexInByte > 7) {
            curByteIndex++;
            if (curByteIndex < data.length) {
                curByte = data[curByteIndex] & 0xFF;
            } else {
                curByte = 0;
            }
            bitIndexInByte = 0;
        }
    }
}
