package com.cmpio;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Utilitários de checagem/diagnóstico ("sanidade") para arquivos CMP.
 * - Imprime um resumo do header
 * - Escolhe um segmento para inspeção (o informado, ou o primeiro não-vazio)
 * - Faz inspeção leve do SegmentRecord (Huffman, payload, bits disponíveis/requeridos)
 *
 * Observação: CmpSanity NÃO decodifica o payload; isso fica a cargo do Stage2Analyzer.
 */
public final class CmpSanity {

    private CmpSanity() {}

    /** Imprime o cabeçalho resumido – igual ao que você vinha vendo nos logs. */
    public static void printHeaderSummary(CmpReader rd) {
        Objects.requireNonNull(rd, "CmpReader null");

        final Path p = rd.getBasePath();
        final ByteOrder order = rd.getByteOrder();

        System.out.println("=== CMP Header Summary ===");
        System.out.println("Arquivo: " + p);
        System.out.println("Byte order: " + (order == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));
        System.out.println("==============================================");

        System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                rd.getOtPos(), rd.getHdrPos(), rd.getRecPos0(), rd.getRecPos1());

        long nx = (long) (rd.getMax1() - rd.getMin1() + 1);
        long ny = (long) (rd.getMax2() - rd.getMin2() + 1);
        long nz = (long) (rd.getMax3() - rd.getMin3() + 1);
        long total = nx * ny * nz;

        System.out.printf("Dimensões: MIN_1..MAX_1=%d..%d   MIN_2..MAX_2=%d..%d   MIN_3..MAX_3=%d..%d   (total=%d)%n",
                rd.getMin1(), rd.getMax1(), rd.getMin2(), rd.getMax2(), rd.getMin3(), rd.getMax3(), total);
    }

    /**
     * Decide qual segmento analisar:
     * - se (s1,s2,s3) for nulo, usa o primeiro não-vazio encontrado na tabela de offsets;
     * - caso contrário, usa o segmento solicitado.
     * Retorna o trio (s1,s2,s3), ou null se não houver nenhum segmento com offset > 0.
     */
    public static int[] chooseSegmentOrFirstNonEmpty(CmpReader rd, int[] requested) {
        Objects.requireNonNull(rd, "CmpReader null");

        int[] chosen = requested;
        if (chosen == null) {
            int[] first = rd.findFirstNonEmpty();
            if (first == null) {
                System.out.println("Nenhum segmento não-vazio encontrado na tabela de offsets.");
                return null;
            }
            chosen = first;
            System.out.printf("Nenhum (seg1,seg2,seg3) informado. Usando o primeiro não-vazio: (%d,%d,%d)%n",
                    chosen[0], chosen[1], chosen[2]);
        } else {
            System.out.printf("Segmento solicitado: (%d,%d,%d)%n", chosen[0], chosen[1], chosen[2]);
        }
        return chosen;
    }

    /**
     * Faz uma inspeção leve do SegmentRecord:
     * - imprime metadados encontrados (min/max, totalBits)
     * - imprime características da Huffman (N, maxlen, Kraft, histograma de comprimentos)
     * - informa bytes/bits disponíveis no primeiro record e o início do payload
     * - alerta se requiredBits > availableBits (bitstream multi-record)
     */
    public static void quickInspect(CmpReader rd, int s1, int s2, int s3) {
        Objects.requireNonNull(rd, "CmpReader null");

        SegmentRecord rec = rd.readSegmentRecord(s1, s2, s3);

        // Cabeçalho do record
        System.out.printf("Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=?," +
                        " layout=SYM_LEN, lensEnc=nibbles(hi->lo), payloadStart=%d%n",
                rec.md.minDelta, rec.md.maxDelta,
                rec.huffman.symbols.length,
                rec.payloadStart);

        // Huffman info
        System.out.printf("Huffman N=%d, maxlen=%d, nonZeroLens=%d, kraftOk=%s%n",
                rec.huffman.symbols.length, rec.huffman.maxLen,
                rec.huffman.nonZeroLens, rec.huffman.kraftOk);

        System.out.printf("Lengths histogram: %s%n", lengthsHistogram(rec.huffman.lens));

        // Bits disponíveis x requeridos
        final int payloadBytes = (rec.payloadSlice != null) ? rec.payloadSlice.remaining() : 0;
        final long availableBits = (long) payloadBytes * 8L;
        final long requiredBits  = rec.md.totalBits;

        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, rec.payloadStart);

        if (requiredBits > availableBits) {
            int missingBytes = (int) (((requiredBits - availableBits) + 7) >>> 3);
            System.out.printf("Aviso: requiredBits=%d > availableBits=%d (+%d bytes). " +
                            "Provável bitstream multi-record — juntar bytes dos próximos records antes de decodificar.%n",
                    requiredBits, availableBits, missingBytes);
        }
    }

    // ======================
    // Helpers
    // ======================

    private static String lengthsHistogram(int[] lens) {
        int[] h = new int[16];
        for (int L : lens) {
            if (L >= 0 && L < 16) h[L]++;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < h.length; i++) {
            if (h[i] != 0) {
                if (sb.length() != 0) sb.append(' ');
                sb.append(i).append(':').append(h[i]);
            }
        }
        return sb.toString();
    }
}
