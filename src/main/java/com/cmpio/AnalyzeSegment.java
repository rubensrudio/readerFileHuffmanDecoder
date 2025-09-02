package com.cmpio;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Entry-point de análise rápida de um CMP:
 *  - abre o arquivo e detecta byte order/posições
 *  - imprime sumário do cabeçalho
 *  - escolhe um segmento (via args ou primeiro não-vazio)
 *  - faz o parse do SegmentRecord e mostra overview
 *  - dispara o Stage2Analyzer (preview tolerante)
 *
 * Uso:
 *   java com.cmpio.AnalyzeSegment <arquivo.cmp> [seg1 seg2 seg3]
 *
 * Se seg1/seg2/seg3 não forem informados, usa o primeiro segmento não-vazio.
 */
public final class AnalyzeSegment {

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
            reader.open();
            dumpHeaderSummary(reader);

            int s1, s2, s3;
            if (seg1 != null && seg2 != null && seg3 != null) {
                s1 = seg1; s2 = seg2; s3 = seg3;
            } else {
                int[] first = reader.findFirstNonEmpty();
                if (first == null) {
                    System.out.println("Nenhum segmento não-vazio encontrado.");
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

            // Stage 2 (preview de decodificação tolerante)
            new Stage2Analyzer().analyze(rec);

        } catch (Exception ex) {
            System.err.println("Falha na análise: " + ex.getMessage());
            // Descomente para stacktrace completo em debug:
            // ex.printStackTrace();
            System.exit(1);
        }
    }

    /* =======================================================================================
     * Impressões / Overviews
     * ======================================================================================= */

    private static void dumpHeaderSummary(CmpReader r) {
        System.out.println("=== CMP Header Summary ===");
        System.out.println("Arquivo: " + r.getBasePath().toString());
        System.out.println("Byte order: " + (r.getByteOrder() == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));

        System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                r.getOtPos(), r.getHdrPos(), r.getRecPos0(), r.getRecPos1());

        System.out.printf("Dimensões: MIN_1..MAX_1=%d..%d  MIN_2..MAX_2=%d..%d  MIN_3..MAX_3=%d..%d  (total=%d)%n",
                r.getMin1(), r.getMax1(), r.getMin2(), r.getMax2(), r.getMin3(), r.getMax3(), r.getSegmentsCount());
        System.out.println("====================================");
    }

    /**
     * Imprime um resumo seguro do segmento. Não lança NPE: valida todos os nulos.
     */
    private static void dumpSegmentOverview(CmpReader r, int s1, int s2, int s3, SegmentRecord rec) {
        if (rec == null) {
            System.out.printf("Segmento (%d,%d,%d): <null>%n", s1, s2, s3);
            return;
        }

        System.out.printf("Segmento selecionado: (%d,%d,%d)%n", s1, s2, s3);

        // Metadata
        SegmentRecord.Metadata md = rec.metadata;
        if (md != null) {
            long requiredBits = md.sumBits();
            int payloadBytes = rec.payloadSlice != null ? rec.payloadSlice.remaining() : 0;
            System.out.printf("Metadata: minDelta=%.6f  maxDelta=%.6f  totalBits=%d  (payloadBytes=%d)%n",
                    md.minDelta, md.maxDelta, requiredBits, payloadBytes);
        } else {
            System.out.println("Metadata: <null>");
        }

        // Huffman + capacidade (usa utilitário com validações internas)
        if (rec.huffman != null) {
            int availableBits = rec.payloadSlice != null ? rec.payloadSlice.remaining() * 8 : 0;
            CmpSanity.dumpHuffmanSummary(rec.huffman,
                    md != null ? md.sumBits() : 0L,
                    availableBits,
                    rec.huffman.payloadStart);
        } else {
            System.out.println("Huffman: <null>");
        }
    }
}
