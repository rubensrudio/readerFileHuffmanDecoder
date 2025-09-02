package com.cmpio;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Analisa um SegmentRecord:
 *  - valida a tabela de Huffman (Kraft)
 *  - checa capacidade (requiredBits x availableBits)
 *  - faz um "preview" de decodificação tolerante a erros (MSB/LSB, invert, startShift 0..7)
 *
 * Importante: se o bitstream ultrapassar 1 record (8192 B), esta classe
 * apenas avisa e NÃO tenta juntar múltiplos records (isso deve ser feito
 * em um nível acima do reader).
 */
public final class Stage2Analyzer {

    /** Quantos símbolos no preview de decodificação (diagnóstico). */
    private static final int PREVIEW_TOKENS = 32;

    /** Ponto de entrada único. */
    public void analyze(SegmentRecord rec) {
        // Dump resumido do segmento + tabela (seguro contra nulos)
        CmpSanity.dumpSegmentSummary(rec, null);

        if (rec == null || rec.huffman == null || rec.metadata == null) {
            System.out.println("Registro/tabela/metadata ausentes — encerrando análise.");
            return;
        }

        final int[] lens = rec.huffman.codeLengths;
        final int[] syms = rec.huffman.symbols;

        // Validações essenciais de Huffman
        if (lens == null || syms == null || lens.length != syms.length || lens.length == 0) {
            throw new IllegalStateException("Huffman table inválida (tamanhos inconsistentes).");
        }
        if (!CmpSanity.kraftOk(lens)) {
            throw new IllegalStateException("Huffman lengths falham a desigualdade de Kraft.");
        }
        // Símbolos precisam estar no intervalo de byte; duplicatas são tecnicamente permitidas
        // (vários códigos mapeando ao mesmo símbolo), então não exigimos unicidade aqui.
        for (int s : syms) {
            if (s < 0 || s > 255) {
                throw new IllegalStateException("Símbolo fora do intervalo 0..255: " + s);
            }
        }

        // Capacidade do payload deste record
        final long requiredBits  = rec.metadata.sumBits(); // <-- atualizado
        final int  availableBits = (rec.payloadSlice != null ? rec.payloadSlice.remaining() * 8 : 0);

        if (requiredBits > availableBits) {
            System.out.printf(
                    "Aviso: requiredBits=%d > availableBits=%d (%+.0f bytes). " +
                            "Provável bitstream multi-record — juntar bytes dos próximos records antes de decodificar.%n",
                    requiredBits, availableBits, Math.ceil((requiredBits - availableBits) / 8.0));
            // Mesmo assim, tentamos um preview curto só para alinhar modo de bits:
            previewDecode(rec);
            return;
        }

        // Preview normal (quando cabe no record)
        previewDecode(rec);
    }

    /* =======================================================================================
     * Preview de decodificação tolerante a erros
     * ======================================================================================= */
    private void previewDecode(SegmentRecord rec) {
        if (rec.payloadSlice == null || rec.payloadSlice.remaining() == 0) {
            System.out.println(">> Preview: payload vazio.");
            return;
        }
        final byte[] payload = toBytes(rec.payloadSlice);
        final CanonicalDecoder dec = new CanonicalDecoder(rec.huffman.codeLengths, rec.huffman.symbols);

        // Tentamos modos (ordem, invert, startShift) e aceitamos o primeiro que
        // conseguir decodificar um número mínimo de tokens sem erro.
        Mode best = null;
        int bestOk = -1;
        int[] bestSample = null;

        for (BitOrder ord : BitOrder.values()) {
            for (boolean invert : new boolean[]{false, true}) {
                for (int shift = 0; shift < 8; shift++) {
                    BitReader br = new BitReader(payload, shift, ord, invert);
                    int[] out = new int[PREVIEW_TOKENS];
                    int ok = 0;
                    for (int i = 0; i < PREVIEW_TOKENS; i++) {
                        int sym = dec.tryDecode(br);
                        if (sym < 0) break; // falhou neste modo
                        out[ok++] = sym;
                    }
                    if (ok > bestOk) {
                        bestOk = ok;
                        best = new Mode(ord, invert, shift);
                        bestSample = Arrays.copyOf(out, ok);
                    }
                    if (bestOk >= PREVIEW_TOKENS) break;
                }
                if (bestOk >= PREVIEW_TOKENS) break;
            }
            if (bestOk >= PREVIEW_TOKENS) break;
        }

        if (bestOk <= 0 || best == null) {
            System.out.println(">> Preview: não foi possível decodificar de forma consistente " +
                    "em nenhum modo (MSB/LSB, invert, shifts 0..7).");
            return;
        }

        System.out.printf(">> Preview OK: %d símbolos decodificados com %s (invert=%s, shift=%d)%n",
                bestOk, best.order, best.invert, best.shift);
        System.out.print("   amostra: ");
        for (int i = 0; i < Math.min(bestOk, 24); i++) {
            if (i > 0) System.out.print(", ");
            System.out.print(bestSample[i]);
        }
        if (bestOk > 24) System.out.print(", ...");
        System.out.println();
    }

    /* =======================================================================================
     * Infra: decodificador canônico e leitor de bits
     * ======================================================================================= */

    /** MSB-first ou LSB-first. */
    private enum BitOrder { MSB, LSB }

    /** Parametrização vencedora do preview. */
    private static final class Mode {
        final BitOrder order; final boolean invert; final int shift;
        Mode(BitOrder order, boolean invert, int shift) {
            this.order = order; this.invert = invert; this.shift = shift;
        }
    }

    /** Leitor de bits a partir de um array de bytes, com suporte a MSB/LSB, invert e shift inicial. */
    private static final class BitReader {
        final byte[] data;
        final BitOrder order;
        final boolean invert;
        long bitPos; // posição corrente em bits, a partir de 0

        BitReader(byte[] data, int startShift, BitOrder order, boolean invert) {
            this.data = data;
            this.order = order;
            this.invert = invert;
            this.bitPos = startShift; // permite deslocar o alinhamento inicial 0..7
        }

        /** Lê próximo bit (0/1) avançando 1. Retorna -1 se acabou. */
        int readBit() {
            if (bitPos >= (long) data.length * 8L) return -1;
            int bit;
            int idx = (int) (bitPos >>> 3);
            int ofs = (int) (bitPos & 7);
            int b = data[idx] & 0xFF;

            if (order == BitOrder.MSB) {
                bit = (b >>> (7 - ofs)) & 1;
            } else {
                bit = (b >>> ofs) & 1;
            }
            bitPos++;
            if (invert) bit ^= 1;
            return bit;
        }

        /** Lê n bits (n<=24), MSB-first no inteiro retornado, avançando n. Retorna -1 se não há bits. */
        int readBits(int n) {
            int v = 0;
            for (int i = 0; i < n; i++) {
                int bit = readBit();
                if (bit < 0) return -1;
                v = (v << 1) | bit;
            }
            return v;
        }

        /** Espia n bits (n<=24) sem consumir; -1 se não há bits. */
        int peekBits(int n) {
            long save = bitPos;
            int v = readBits(n);
            bitPos = save;
            return v;
        }
    }

    /**
     * Decodificador Huffman canônico para símbolos em 0..255.
     * Constrói vetores first/last e um vetor de símbolos "ordenado por comprimento".
     */
    private static final class CanonicalDecoder {
        final int maxL;
        final int[] firstCode;    // firstCode[L]
        final int[] lastCode;     // lastCode[L]
        final int[] firstIndex;   // índice inicial em sortedSymbols para comprimento L
        final int[] sortedSymbols;

        CanonicalDecoder(int[] codeLengths, int[] symbols) {
            if (codeLengths.length != symbols.length)
                throw new IllegalArgumentException("lens.length != symbols.length");

            // Contagem por comprimento
            int[] count = new int[33];
            int m = 0;
            for (int L : codeLengths) {
                if (L < 0 || L > 32) throw new IllegalArgumentException("Invalid length " + L);
                if (L > 0) { count[L]++; if (L > m) m = L; }
            }
            this.maxL = m;

            // firstCode/firstIndex por comprimento
            this.firstCode  = new int[33];
            this.lastCode   = new int[33];
            this.firstIndex = new int[33];

            int code = 0, index = 0;
            for (int L = 1; L <= maxL; L++) {
                code = (code + count[L - 1]) << 1;
                firstCode[L]  = code;
                firstIndex[L] = index;
                lastCode[L]   = code + count[L] - 1;
                index += count[L];
            }

            // Monta vetor de símbolos ordenado por comprimento (estável pelo índice original)
            this.sortedSymbols = new int[codeLengths.length];
            int[] cursor = Arrays.copyOf(firstIndex, firstIndex.length);
            for (int L = 1; L <= maxL; L++) {
                for (int i = 0; i < codeLengths.length; i++) {
                    if (codeLengths[i] == L) {
                        int pos = cursor[L]++;
                        sortedSymbols[pos] = symbols[i] & 0xFF;
                    }
                }
            }
        }

        /** Tenta decodificar um símbolo; retorna -1 se não bater com nenhum prefixo válido. */
        int tryDecode(BitReader br) {
            int acc = 0;
            for (int L = 1; L <= maxL; L++) {
                int bit = br.readBit();
                if (bit < 0) return -1;
                acc = (acc << 1) | bit;

                int lo = firstCode[L], hi = lastCode[L];
                if (hi >= lo && acc >= lo && acc <= hi) {
                    int off = acc - lo;
                    int idx = firstIndex[L] + off;
                    if (idx < 0 || idx >= sortedSymbols.length) return -1;
                    return sortedSymbols[idx];
                }
            }
            return -1;
        }
    }

    /* =======================================================================================
     * Utilitários locais
     * ======================================================================================= */
    private static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer b = buf.duplicate();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }
}
