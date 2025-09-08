package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executa análise completa do segmento + Stage 7 e Stage 8,
 * sem depender de tipos internos do Stage5Reconstructor.
 */
public final class AnalyzeSegment {

    private AnalyzeSegment() {}

    /** Regex para identificar arquivos cycle_XX_tokens.csv */
    private static final Pattern TOKENS_CYCLE_RE = Pattern.compile("^cycle_(\\d{2})_tokens\\.csv$");

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
            System.out.print("Lengths histogram:");
            for (int L=1; L<hist.length; L++) if (hist[L]>0) System.out.print(" " + L + ":" + hist[L]);
            System.out.println();

            long requiredBits = rec.md.totalBits;
            long availableBits = (long) rec.md.payloadBytes * 8L;
            System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                    requiredBits, availableBits, rec.md.payloadStartByte);

            // ===== Stage 4 =====
            System.out.println("=== Stage 4 ===");
            boolean splitOnly251 = true;
            double padMergeThreshold = 0.70;
            // Não capturamos retorno (run não retorna Result)
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

            // ===== Stage 7 =====
            System.out.println("=== Stage 7 ===");
            Path outStage7 = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage7_out")
                    : cmpPath.getParent().resolve("cmp_stage7_out");
            Files.createDirectories(outStage7);

            List<Path> tokenCsvs = listCycleTokenFiles(outStage5);
            tokenCsvs.sort(Comparator.comparing(Path::toString));
            for (Path tokensCsv : tokenCsvs) {
                int cyc = extractCycleIndex(tokensCsv.getFileName().toString());
                Path cycDir = outStage7.resolve(String.format("cycle_%02d", cyc));
                Files.createDirectories(cycDir);

                // escolha do modo
                Stage7Decoder.RunMode mode = Stage7Decoder.RunMode.REPEAT_LAST;
                Stage7Decoder.Result r7 = Stage7Decoder.quickRunCsv(tokensCsv, cycDir, mode);
                System.out.println(r7);
            }

            // ===== Stage 8 =====
            System.out.println("=== Stage 8 ===");
            Path outStage8 = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage8_out")
                    : cmpPath.getParent().resolve("cmp_stage8_out");
            Files.createDirectories(outStage8);
            Stage8Rebuilder.Result r8 = Stage8Rebuilder.run(outStage7);
            System.out.println("[Stage 8] " + r8);

            // ===== Stage 9 =====
            System.out.println("=== Stage 9 ===");
            Path stage7Out = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage7_out")
                    : cmpPath.getParent().resolve("cmp_stage7_out");
            Path stage9Out = cmpPath.getParent() == null
                    ? Paths.get("cmp_stage9_out")
                    : cmpPath.getParent().resolve("cmp_stage9_out");

            Stage9Stitcher.Result s9 = Stage9Stitcher.run(stage7Out, stage9Out);

            // ===== Stage 10 =====
            System.out.println("=== Stage 10 ===");
            // Mapa default 1..5 -> identidade (ajuste quando tiver a tabela real)
            Map<Integer,Integer> defaultMap = new LinkedHashMap<>();
            for (int k = 1; k <= 5; k++) defaultMap.put(k, k);

            Stage10Mapper.Result s10 = Stage10Mapper.run(stage9Out, defaultMap);

            System.out.println("[AnalyzeSegment] Concluído.");
        }
    }

    /** Lista todos os arquivos cycle_XX_tokens.csv no diretório informado. */
    private static List<Path> listCycleTokenFiles(Path outStage5) throws Exception {
        List<Path> list = new ArrayList<>();
        if (outStage5 == null || !Files.isDirectory(outStage5)) return list;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(outStage5, "cycle_*_tokens.csv")) {
            for (Path p : ds) {
                if (p.getFileName() == null) continue;
                String name = p.getFileName().toString();
                if (TOKENS_CYCLE_RE.matcher(name).matches()) list.add(p);
            }
        }
        return list;
    }

    /** Extrai o índice do ciclo a partir de "cycle_XX_tokens.csv". */
    private static int extractCycleIndex(String filename) {
        if (filename == null) return -1;
        Matcher m = TOKENS_CYCLE_RE.matcher(filename);
        if (m.matches()) {
            try { return Integer.parseInt(m.group(1)); } catch (NumberFormatException ignore) {}
        }
        return -1;
    }
}
