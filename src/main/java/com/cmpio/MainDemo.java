package com.cmpio;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Demo/CLI mínima para inspecionar um arquivo CMP e analisar um segmento.
 *
 * Uso:
 *   java com.cmpio.MainDemo <arquivo.cmp> [seg1 seg2 seg3]
 *
 * - Se (seg1,seg2,seg3) não forem informados, usa o primeiro segmento não-vazio.
 * - Imprime um resumo do cabeçalho e do segmento.
 * - Executa o Stage2Analyzer (preview de decodificação tolerante).
 */
public final class MainDemo {

    public static void main(String[] args) {
        final Path base = Path.of("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");

        Integer seg1 = null, seg2 = null, seg3 = null;
        if (args.length >= 4) {
            try {
                seg1 = Integer.parseInt(args[1]);
                seg2 = Integer.parseInt(args[2]);
                seg3 = Integer.parseInt(args[3]);
            } catch (NumberFormatException nfe) {
                System.err.println("Parâmetros de segmento inválidos. Esperado: [seg1 seg2 seg3] inteiros.");
                System.exit(2);
            }
        }

        try (CmpReader reader = new CmpReader(base)) {
            // Abre e detecta endianness / offsets principais
            reader.open();

            // Resumo do cabeçalho
            dumpHeaderSummary(reader);

            // Decide o segmento a analisar
            int s1, s2, s3;
            if (seg1 != null && seg2 != null && seg3 != null) {
                s1 = seg1; s2 = seg2; s3 = seg3;
            } else {
                int[] first = reader.findFirstNonEmpty();
                if (first == null) {
                    System.out.println("Nenhum segmento não-vazio encontrado neste arquivo.");
                    return;
                }
                s1 = first[0]; s2 = first[1]; s3 = first[2];
                System.out.printf("Nenhum (seg1,seg2,seg3) informado. Usando o primeiro não-vazio: (%d,%d,%d)%n",
                        s1, s2, s3);
            }

            // Lê o registro do segmento
            SegmentRecord rec = reader.readSegmentRecord(s1, s2, s3);

            // Overview do segmento (metadata + huffman + capacidade)
            dumpSegmentOverview(reader, s1, s2, s3, rec);

            // Stage 2 (preview tolerante de Huffman)
            new Stage2Analyzer().analyze(rec);

        } catch (Exception ex) {
            System.err.println("Erro: " + ex.getMessage());
            // Descomente a linha abaixo para ver o stacktrace completo em debug:
            // ex.printStackTrace();
            System.exit(1);
        }
    }

    /* =======================================================================================
     * Impressões
     * ======================================================================================= */

    private static void dumpHeaderSummary(CmpReader r) {
        System.out.println("=== CMP Header Summary ===");
        System.out.println("Arquivo: " + r.getBasePath());
        System.out.println("Byte order: " + (r.getByteOrder() == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));
        System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                r.getOtPos(), r.getHdrPos(), r.getRecPos0(), r.getRecPos1());
        System.out.printf("Dimensões: MIN_1..MAX_1=%d..%d  MIN_2..MAX_2=%d..%d  MIN_3..MAX_3=%d..%d  (total=%d)%n",
                r.getMin1(), r.getMax1(), r.getMin2(), r.getMax2(), r.getMin3(), r.getMax3(), r.getSegmentsCount());
        System.out.println("====================================");
    }

    private static void dumpSegmentOverview(CmpReader r, int s1, int s2, int s3, SegmentRecord rec) {
        System.out.printf("Segmento selecionado: (%d,%d,%d)%n", s1, s2, s3);

        if (rec == null) {
            System.out.println("Segmento: <null>");
            return;
        }

        // Metadata + capacidade
        if (rec.metadata != null) {
            long requiredBits = rec.metadata.sumBits();
            int payloadBytes = rec.payloadSlice != null ? rec.payloadSlice.remaining() : 0;
            System.out.printf("Metadata: minDelta=%.6f  maxDelta=%.6f  totalBits=%d  (payloadBytes=%d)%n",
                    rec.metadata.minDelta, rec.metadata.maxDelta, requiredBits, payloadBytes);
        } else {
            System.out.println("Metadata: <null>");
        }

        // Huffman (usa utilitário que já imprime histograma, Kraft, etc.)
        CmpSanity.dumpSegmentSummary(rec, r.getByteOrder());
    }
}
