package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Executa a análise de um único segmento.
 * Por padrão, usa o primeiro record do arquivo (o segmento (0,0,0) no seu caso).
 * Se desejar, você pode estender para aceitar seg1/seg2/seg3 por linha de comando
 * e mapear esses índices para o record correto.
 */
public final class AnalyzeSegment {

    private AnalyzeSegment() {}

    public static void main(String[] args) throws Exception {
        Path cmpPath = Paths.get("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");
        try (CmpReader r = new CmpReader(cmpPath)) {
            r.open();

            System.out.println("=== CMP Header Summary ===");
            System.out.println("Arquivo: " + r.getBasePath());
            System.out.println("Byte order: " + r.getByteOrder());
            System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                    r.getOtPos(), r.getHdrPos(), r.getRecPos0(), r.getRecPos1());
            System.out.println("==============================================");

            int[] first = r.findFirstNonEmpty();
            if (first == null) {
                System.out.println("Nenhum (seg1,seg2,seg3) com offset>0.");
                return;
            }
            int s1 = first[0], s2 = first[1], s3 = first[2];
            System.out.printf("Usando o primeiro não-vazio: (%d,%d,%d)%n", s1, s2, s3);

            SegmentRecord rec = r.readSegmentRecord(s1, s2, s3);
            ByteOrder order = r.getByteOrder();
            ByteBuffer file = r.getFileBuffer();

            // Resumo do record/huffman
            int nonZero = 0, maxLen = 0;
            for (int L : rec.huffman.lens) { if (L>0) { nonZero++; if (L>maxLen) maxLen=L; } }
            System.out.printf("Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=%d, layout=%s, lensEnc=%s, payloadStart=%d%n",
                    rec.md.minDelta, rec.md.maxDelta, rec.huffman.N, rec.huffman.base,
                    rec.huffman.layout, rec.huffman.lensEncoding, rec.md.payloadStartByte);
            System.out.printf("Huffman Huffman{N=%d, maxLen=%d, nonZeroLens=%d, kraftOk=%s}%n",
                    rec.huffman.N, maxLen, nonZero, rec.huffman.kraftOk);

            int[] hist = new int[maxLen+1];
            for (int L : rec.huffman.lens) if (L>=0 && L<hist.length) hist[L]++;
            System.out.print("Lengths histogram:"); for (int L=1; L<hist.length; L++) if (hist[L]>0) System.out.print(" " + L + ":" + hist[L]); System.out.println();

            long requiredBits = rec.md.totalBits;
            long availableBits = (long) rec.md.payloadBytes * 8L;
            System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                    requiredBits, availableBits, rec.md.payloadStartByte);

            // ===== Stage 4 v2 =====
            System.out.println("=== Stage 4 ===");
            boolean splitOnly251 = true;
            double padMergeThreshold = 0.70;
            Stage4TokenGrouper.run(file, rec.recStart, order, rec, splitOnly251, padMergeThreshold);

            // ===== Stage 4.3 =====
            System.out.println("=== Stage 4.3 ===");
            Path outSchemaDir = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage4_schema")
                    : cmpPath.getParent().resolve("cmp_stage4_schema");
            Stage4SchemaExtractor.run(file, rec.recStart, order, rec, outSchemaDir, padMergeThreshold);

            // ===== Stage 5 =====
            System.out.println("=== Stage 5 ===");
            Path outStage5 = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage5_out")
                    : cmpPath.getParent().resolve("cmp_stage5_out");
            Stage5Reconstructor.run(file, rec.recStart, order, rec, outStage5);
        }
    }
}
