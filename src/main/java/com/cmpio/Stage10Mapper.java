package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Converte os níveis discretos (ex.: 1..5) do stage9_series.csv para valores
 * de domínio real via um mapa (level -> value). Por padrão, usa identidade.
 *
 * Saída: stage10_mapped.csv (colunas: idx, level, value_mapped)
 */
public final class Stage10Mapper {

    private Stage10Mapper() {}

    public static final class Result {
        public final Path outDir;
        public final Path mappedCsv;
        public final Map<Integer, Integer> mapUsed;
        public final int rows;

        public Result(Path outDir, Path mappedCsv, Map<Integer, Integer> mapUsed, int rows) {
            this.outDir = outDir;
            this.mappedCsv = mappedCsv;
            this.mapUsed = mapUsed;
            this.rows = rows;
        }

        @Override
        public String toString() {
            return "Stage10.Result{rows=" + rows + ", outDir=" + outDir +
                    ", mappedCsv=" + mappedCsv + ", mapUsed=" + mapUsed + "}";
        }
    }

    /** Mapa default: identidade (k->k). */
    public static Map<Integer, Integer> defaultMapIdentity(Collection<Integer> domainLevels) {
        Map<Integer, Integer> m = new HashMap<>();
        for (int k : domainLevels) m.put(k, k);
        return m;
    }

    public static Result run(Path stage9Dir, Map<Integer, Integer> mapping) throws IOException {
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        if (!Files.exists(seriesCsv)) {
            throw new FileNotFoundException("stage9_series.csv não encontrado: " + seriesCsv);
        }
        Path outCsv = stage9Dir.resolve("stage10_mapped.csv");

        int rows = 0;
        try (BufferedReader br = Files.newBufferedReader(seriesCsv, StandardCharsets.UTF_8);
             BufferedWriter bw = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8)) {
            String header = br.readLine(); // idx,value
            bw.write("idx,level,value_mapped\n");
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                if (parts.length < 2) continue;
                String sIdx = parts[0].trim();
                String sLvl = parts[1].trim();
                try {
                    int idx = Integer.parseInt(sIdx);
                    int level = Integer.parseInt(sLvl);
                    int mapped = mapping.getOrDefault(level, level);
                    bw.write(Integer.toString(idx));
                    bw.write(',');
                    bw.write(Integer.toString(level));
                    bw.write(',');
                    bw.write(Integer.toString(mapped));
                    bw.write('\n');
                    rows++;
                } catch (NumberFormatException ignore) {
                    // pula linhas ruins
                }
            }
        }

        Result r = new Result(stage9Dir, outCsv, new LinkedHashMap<>(mapping), rows);
        System.out.println("[Stage 10] " + r);
        return r;
    }
}
