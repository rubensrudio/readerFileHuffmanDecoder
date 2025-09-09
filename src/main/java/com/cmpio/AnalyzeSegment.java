package com.cmpio;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public final class AnalyzeSegment {

    private AnalyzeSegment() {}

    public static void main(String[] args) throws Exception {
        Path cmpPath = (args != null && args.length > 0)
                ? Paths.get(args[0])
                : Paths.get("D:\\tmp\\cmp_dir\\S_R5000_290125_159.cmp");

        try (CmpReader r = new CmpReader(cmpPath)) {
            r.open();

            System.out.println("=== CMP Header Summary ===");
            System.out.println("Arquivo: " + r.getBasePath());
            System.out.println("Byte order: " + r.getByteOrder());
            System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                    r.getOtPos(), r.getHdrPos(), r.getRecPos0(), r.getRecPos1());
            System.out.println("==============================================");

            // 1) Enumerar todos os segmentos não-vazios
            List<int[]> segments = enumerateSegments(r);
            if (segments.isEmpty()) {
                System.out.println("Nenhum segmento não-vazio encontrado. Encerrando.");
                return;
            }
            System.out.println("Segmentos a processar: " + prettyList(segments));

            // 2) Para cada segmento, roda a pipeline completa em uma subpasta dedicada
            Path rootBase = (cmpPath.getParent() != null) ? cmpPath.getParent() : Paths.get(".");
            List<Path> stage16PerSeg = new ArrayList<>(); // coleta finais por segmento p/ consolidado

            for (int[] seg : segments) {
                int s1 = seg[0], s2 = seg[1], s3 = seg[2];
                String segTag = String.format("seg_%02d_%02d_%02d", s1, s2, s3);
                Path segBaseDir = rootBase.resolve(segTag);
                Files.createDirectories(segBaseDir);

                System.out.println("\n==============================");
                System.out.println("Processando segmento " + segTag);
                System.out.println("==============================");

                // Carregar metadados do segmento
                SegmentRecord rec = r.readSegmentRecord(s1, s2, s3);
                ByteOrder order = r.getByteOrder();
                ByteBuffer file = r.getFileBuffer();

                // --- resumo Huffman (curto) ---
                summarizeHuffman(rec);

                // ===== Stage 4 =====
                System.out.println("\n=== Stage 4 ===");
                boolean splitOnly251 = true;
                double padMergeThreshold = 0.70;
                Stage4TokenGrouper.run(file, rec.recStart, order, rec, splitOnly251, padMergeThreshold);

                // ===== Stage 4.3 =====
                System.out.println("\n=== Stage 4.3 ===");
                Path outSchemaDir = segBaseDir.resolve("cmp_stage4_schema");
                Stage4SchemaExtractor.run(file, rec.recStart, order, rec, outSchemaDir, padMergeThreshold);

                // ===== Stage 5 =====
                System.out.println("\n=== Stage 5 ===");
                Path stage5Out = segBaseDir.resolve("cmp_stage5_out");
                Files.createDirectories(stage5Out);
                Stage5Reconstructor.run(file, rec.recStart, order, rec, stage5Out, 0.85);

                // ===== Stage 7 =====
                System.out.println("\n=== Stage 7 ===");
                Path stage7Out = segBaseDir.resolve("cmp_stage7_out");
                Files.createDirectories(stage7Out);

                List<Path> tokenFiles = new ArrayList<>();
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(stage5Out, "cycle_*_tokens.csv")) {
                    for (Path p : ds) tokenFiles.add(p);
                }
                tokenFiles.sort(Comparator.naturalOrder());

                Map<String, Integer> stage7Counts = new LinkedHashMap<>();
                for (Path tok : tokenFiles) {
                    String fname = tok.getFileName().toString(); // cycle_XX_tokens.csv
                    int idx = extractCycleIndex(fname);
                    Path outCycle = stage7Out.resolve(String.format(Locale.ROOT, "cycle_%02d", idx));
                    Files.createDirectories(outCycle);

                    Stage7Decoder.RunMode mode = Stage7Decoder.RunMode.REPEAT_LAST;
                    Stage7Decoder.Result r1 = Stage7Decoder.quickRunCsv(tok, outCycle, mode);
                    System.out.println(r1.toString());
                    stage7Counts.put(String.format(Locale.ROOT, "cycle_%02d", idx), r1.samples);
                }

                // ===== Stage 8 =====
                System.out.println("\n=== Stage 8 ===");
                Stage8Rebuilder.Result s8 = Stage8Rebuilder.run(stage7Out);
                System.out.println(s8);
                // opcional: comparar Stage7 vs Stage8
                if (!stage7Counts.isEmpty() && !s8.samplesPerCycle.isEmpty()) {
                    System.out.println("[Stage 8] Comparativo Stage7→Stage8 (samples por ciclo):");
                    for (Map.Entry<String,Integer> e : s8.samplesPerCycle.entrySet()) {
                        String cyc = e.getKey();
                        Integer v8 = e.getValue();
                        Integer v7 = stage7Counts.get(cyc);
                        String mark = (v7 != null && Objects.equals(v7, v8)) ? "OK" : "Δ";
                        System.out.printf(Locale.ROOT, "  %s: Stage7=%s, Stage8=%d  [%s]%n",
                                cyc, (v7==null? "<n/d>" : v7.toString()), v8, mark);
                    }
                }

                // ===== Stage 9 =====
                System.out.println("\n=== Stage 9 ===");
                Path stage9Out = segBaseDir.resolve("cmp_stage9_out");
                Stage9Stitcher.Result s9 = Stage9Stitcher.run(stage7Out, stage9Out);
                System.out.println("[Stage 9] " + s9);

                // ===== Stage 10 =====
                System.out.println("\n=== Stage 10 ===");
                Path stage9Series = stage9Out.resolve("stage9_series.csv");
                Set<Integer> levels = readLevelsFromSeries(stage9Series);
                Map<Integer, Integer> mapping = Stage10Mapper.defaultMapIdentity(levels);
                Stage10Mapper.Result s10 = Stage10Mapper.run(stage9Out, mapping);
                System.out.println("[Stage 10] " + s10);

                // ===== Stage 11 =====
                System.out.println("\n=== Stage 11 ===");
                Path stage11Out = segBaseDir.resolve("cmp_stage11_out");
                Stage11SignalInspector.Result s11 = Stage11SignalInspector.run(stage9Out, stage11Out);
                System.out.println("[Stage 11] " + s11);

                // ===== Stage 12 =====
                System.out.println("\n=== Stage 12 ===");
                Stage12PeriodGrid.Result s12 = Stage12PeriodGrid.run(segBaseDir);
                System.out.println("[Stage 12] " + (s12.best != null ? s12.best : "<none>"));

                // ===== Stage 13 =====
                System.out.println("\n=== Stage 13 ===");
                Stage13FieldInfer.Result s13 = Stage13FieldInfer.run(segBaseDir);
                System.out.println("[Stage 13] " + s13);

                // ===== Stage 14 =====
                System.out.println("\n=== Stage 14 ===");
                Stage14RecordLayoutValidator.Result s14 = Stage14RecordLayoutValidator.run(segBaseDir);
                System.out.println("[Stage 14] " + s14);

                // ===== Stage 15 =====
                System.out.println("\n=== Stage 15 ===");
                Stage15RecordDeserializer.Result s15 = Stage15RecordDeserializer.run(segBaseDir);
                System.out.println(s15);
                System.out.println("Stage 15 outputs:");
                for (Map.Entry<String, Path> e : s15.perCycleCsv.entrySet()) {
                    System.out.println("  cycle " + e.getKey() + " -> " + e.getValue());
                }
                System.out.println("  all -> " + s15.allCsv);

                // ===== Stage 16 =====
                System.out.println("\n=== Stage 16 ===");
                Stage16Exporter.Result s16 = Stage16Exporter.run(segBaseDir);
                System.out.println("Stage 16 final:");
                System.out.println("  outDir   : " + s16.outDir.toAbsolutePath());
                System.out.println("  finalCsv : " + s16.finalCsv.toAbsolutePath());
                System.out.println("  rows,cols: " + s16.rows + "," + s16.cols);
                System.out.println("  headers  : " + s16.headers);

                stage16PerSeg.add(s16.finalCsv);

                // ===== Stage 17 =====
                System.out.println("\n=== Stage 17 ===");
                Stage17Manifest.Result s17 = Stage17Manifest.run(segBaseDir);
                System.out.println("Stage 17 manifest:");
                System.out.println("  outDir  : " + s17.outDir.toAbsolutePath());
                System.out.println("  manifest: " + s17.manifestJson.toAbsolutePath());
            }

            // 3) Consolidado global (merge dos Stage 16 por segmento)
            System.out.println("\n=== Consolidado Global (todos os segmentos) ===");
            Path globalOut = rootBase.resolve("cmp_export_all");
            Files.createDirectories(globalOut);
            Path mergedCsv = globalOut.resolve("stage16_final_all_segments.csv");
            mergeStage16AcrossSegments(stage16PerSeg, mergedCsv);
            System.out.println("CSV final consolidado: " + mergedCsv.toAbsolutePath());

            System.out.println("\n[AnalyzeSegment] Concluído.");
        }
    }

    // --------- Helpers de enumeração e resumo ---------

    /** Tenta listar segmentos via CmpReader; se não existir, cai no primeiro não-vazio. */
    private static List<int[]> enumerateSegments(CmpReader r) throws IOException {
        // Caminho preferido: CmpReader expõe a lista completa
        try {
            List<int[]> lst = r.listNonEmptySegments();
            if (lst != null && !lst.isEmpty()) return lst;
        } catch (NoSuchMethodError | RuntimeException ignore) {}

        // Fallback: usa somente o primeiro não-vazio (compat)
        int[] first = r.findFirstNonEmpty();
        if (first != null) return Collections.singletonList(first);
        return Collections.emptyList();
    }

    private static void summarizeHuffman(SegmentRecord rec) {
        int nonZero = 0, maxLen = 0;
        for (int L : rec.huffman.lens) {
            if (L > 0) { nonZero++; if (L > maxLen) maxLen = L; }
        }
        System.out.printf(
                "Segment parsed: minDelta=%.6f maxDelta=%.6f, N=%d, base=%d, layout=%s, lensEnc=%s, payloadStart=%d%n",
                rec.md.minDelta, rec.md.maxDelta, rec.huffman.N, rec.huffman.base,
                rec.huffman.layout, rec.huffman.lensEncoding, rec.md.payloadStartByte);
        System.out.printf("Huffman Huffman{N=%d, maxLen=%d, nonZeroLens=%d, kraftOk=%s}%n",
                rec.huffman.N, maxLen, nonZero, rec.huffman.kraftOk);
        int[] hist = new int[Math.max(1, maxLen + 1)];
        for (int L : rec.huffman.lens) if (L >= 0 && L < hist.length) hist[L]++;
        System.out.print("Lengths histogram:");
        for (int L = 1; L < hist.length; L++) if (hist[L] > 0) System.out.print(" " + L + ":" + hist[L]);
        System.out.println();
        long requiredBits  = rec.md.totalBits;
        long availableBits = (long) rec.md.payloadBytes * 8L;
        System.out.printf("requiredBits=%d, availableBits=%d, payloadStartByte=%d%n",
                requiredBits, availableBits, rec.md.payloadStartByte);
    }

    private static String prettyList(List<int[]> segs) {
        List<String> s = new ArrayList<>();
        for (int[] t : segs) s.add("(" + t[0] + "," + t[1] + "," + t[2] + ")");
        return s.toString();
    }

    private static int extractCycleIndex(String filename) {
        try {
            int us = filename.indexOf('_');
            int us2 = filename.indexOf('_', us + 1);
            if (us < 0 || us2 < 0) return 0;
            String mid = filename.substring(us + 1, us2);
            return Integer.parseInt(mid);
        } catch (Exception e) {
            return 0;
        }
    }

    /** Varre stage9_series.csv (idx,value) e retorna os níveis distintos. */
    private static Set<Integer> readLevelsFromSeries(Path seriesCsv) throws IOException {
        Set<Integer> levels = new TreeSet<>();
        if (!Files.exists(seriesCsv)) return levels;
        try (BufferedReader br = Files.newBufferedReader(seriesCsv, StandardCharsets.UTF_8)) {
            String header = br.readLine(); // "idx,value"
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                if (parts.length < 2) continue;
                try { levels.add(Integer.parseInt(parts[1].trim())); } catch (NumberFormatException ignore) {}
            }
        }
        return levels;
    }

    /** Concatena os Stage16 finais de cada segmento em um único CSV com coluna 'segment'. */
    private static void mergeStage16AcrossSegments(List<Path> finals, Path outCsv) throws IOException {
        if (finals == null || finals.isEmpty()) {
            try (BufferedWriter w = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8)) {
                w.write("segment\n");
            }
            return;
        }
        try (BufferedWriter w = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8)) {
            String globalHeader = null;
            for (Path p : finals) {
                if (!Files.exists(p)) continue;
                String segTag = p.getParent().getParent().getFileName().toString(); // .../seg_xx_xx_xx/cmp_stage16_out/stage16_final.csv
                List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
                if (lines.isEmpty()) continue;

                String header = lines.get(0).trim();
                if (globalHeader == null) {
                    // injeta 'segment' como 1ª coluna
                    globalHeader = "segment," + header;
                    w.write(globalHeader); w.write('\n');
                } else if (!("segment," + header).equals(globalHeader)) {
                    // headers diferentes — aqui mantemos simples: ainda concatenamos
                    // (se quiser, implemente normalização/union)
                }
                for (int i = 1; i < lines.size(); i++) {
                    w.write(segTag); w.write(',');
                    w.write(lines.get(i));
                    w.write('\n');
                }
            }
        }
    }
}
