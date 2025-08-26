package com.cmpio;

import java.nio.file.Path;

public final class AnalyzeSegment {
    public static void main(String[] args) throws Exception {
        Path p = Path.of("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");
        try (CmpReader r = CmpReader.open(p)) {
            int s1, s2, s3;
            if (args.length >= 4) {
                s1 = Integer.parseInt(args[1]);
                s2 = Integer.parseInt(args[2]);
                s3 = Integer.parseInt(args[3]);
            } else {
                int[] first = r.findFirstNonEmpty();
                if (first == null) {
                    System.out.println("Nenhum segmento não-vazio encontrado.");
                    return;
                }
                s1 = first[0]; s2 = first[1]; s3 = first[2];
            }

            SegmentRecord rec = r.readSegmentRecord(s1, s2, s3);
            if (rec == null) {
                System.out.printf("Segmento (%d,%d,%d) está vazio.%n", s1, s2, s3);
                return;
            }

            Stage2Analyzer.Result res = Stage2Analyzer.analyze(rec);
            System.out.printf("Stage 2 - segmento (%d,%d,%d):%n", s1, s2, s3);
            System.out.println(res);

            boolean onePerCoef = Stage2Analyzer.looksLikeOneSymbolPerCoefficient(res);
            System.out.println("Classificação heurística: " +
                    (onePerCoef ? "ONE_SYMBOL_PER_COEFFICIENT" : "TOKENIZED"));
        }
    }
}
