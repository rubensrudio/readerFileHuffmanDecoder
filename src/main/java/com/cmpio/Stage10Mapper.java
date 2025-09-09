package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Stage 10 – Mapper (v1.2)
 * - Lê stage9_series.csv (colunas comuns: idx,value | idx,level | value)
 * - Aplica mapa level->valor (identidade por padrão)
 * - Salva stage10_mapped.csv e artefatos de auditoria
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
        Map<Integer, Integer> m = new LinkedHashMap<>();
        for (int k : new TreeSet<>(domainLevels)) m.put(k, k);
        return m;
    }

    /** Overload conveniente: infere o domínio da série e usa mapa identidade. */
    public static Result run(Path stage9Dir) throws IOException {
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        Set<Integer> domain = readDomain(seriesCsv);
        Map<Integer,Integer> map = defaultMapIdentity(domain);
        return run(stage9Dir, map);
    }

    public static Result run(Path stage9Dir, Map<Integer, Integer> mapping) throws IOException {
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        if (!Files.exists(seriesCsv)) {
            throw new FileNotFoundException("stage9_series.csv não encontrado: " + seriesCsv);
        }
        Files.createDirectories(stage9Dir);

        Path outCsv        = stage9Dir.resolve("stage10_mapped.csv");
        Path histCsv       = stage9Dir.resolve("stage10_level_hist.csv");
        Path mapUsedCsv    = stage9Dir.resolve("stage10_map_used.csv");

        Map<Integer,Integer> mapUsed = new LinkedHashMap<>();
        Map<Integer,Integer> hist    = new LinkedHashMap<>();
        Set<Integer> missingSeen     = new LinkedHashSet<>();

        int rows = 0;
        try (BufferedReader br = Files.newBufferedReader(seriesCsv, StandardCharsets.UTF_8);
             BufferedWriter bw = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            String header = br.readLine(); // pode ser null
            int idxCol = 0, valCol = 1;
            if (header != null) {
                String[] h = header.split(",", -1);
                // tenta achar "idx"
                for (int i = 0; i < h.length; i++) {
                    if (h[i].trim().equalsIgnoreCase("idx")) { idxCol = i; break; }
                }
                // tenta achar coluna de valor
                int candidate = -1;
                for (int i = 0; i < h.length; i++) {
                    String t = h[i].trim().toLowerCase(Locale.ROOT);
                    if (t.equals("value") || t.equals("level") || t.equals("val") || t.equals("sample")) {
                        candidate = i; break;
                    }
                }
                if (candidate >= 0) valCol = candidate;
                else if (h.length == 1) { idxCol = -1; valCol = 0; } // CSV com 1 coluna (apenas value)
            }

            bw.write("idx,level,value_mapped\n");

            String line;
            int autoIdx = 0;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] parts = line.split(",", -1);
                String sIdx = (idxCol >= 0 && idxCol < parts.length) ? parts[idxCol].trim() : null;
                String sLvl = (valCol >= 0 && valCol < parts.length) ? parts[valCol].trim() : null;
                if (sLvl == null || sLvl.isEmpty()) continue;

                try {
                    int idx = (sIdx == null || sIdx.isEmpty()) ? autoIdx : parseIntSafe(sIdx);
                    int level = parseIntSafe(sLvl);
                    int mapped = mapping.getOrDefault(level, level);
                    if (!mapping.containsKey(level)) missingSeen.add(level);

                    // contagens de auditoria
                    hist.put(level, hist.getOrDefault(level, 0) + 1);
                    if (!mapUsed.containsKey(level)) mapUsed.put(level, mapped);

                    bw.write(Integer.toString(idx));
                    bw.write(',');
                    bw.write(Integer.toString(level));
                    bw.write(',');
                    bw.write(Integer.toString(mapped));
                    bw.write('\n');

                    rows++;
                    autoIdx++;
                } catch (NumberFormatException ignore) {
                    // pula linhas ruins
                }
            }
        }

        // artefatos de auditoria
        try (BufferedWriter w = Files.newBufferedWriter(histCsv, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            w.write("level,count\n");
            for (Map.Entry<Integer,Integer> e : new TreeMap<>(hist).entrySet()) {
                w.write(e.getKey() + "," + e.getValue() + "\n");
            }
        }
        try (BufferedWriter w = Files.newBufferedWriter(mapUsedCsv, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            w.write("level,value_mapped\n");
            for (Map.Entry<Integer,Integer> e : mapUsed.entrySet()) {
                w.write(e.getKey() + "," + e.getValue() + "\n");
            }
        }

        if (!missingSeen.isEmpty()) {
            System.out.println("[Stage 10] Aviso: níveis sem entrada explícita no mapping, mapeados por identidade: " + missingSeen);
        }

        Result r = new Result(stage9Dir, outCsv, mapUsed, rows);
        System.out.println("[Stage 10] " + r);
        return r;
    }

    // -------- helpers --------

    private static int parseIntSafe(String s) {
        // remove BOM se aparecer colado no primeiro campo
        if (!s.isEmpty() && s.charAt(0) == '\uFEFF') s = s.substring(1);
        return Integer.parseInt(s.trim());
    }

    /** Lê todos os levels existentes em stage9_series.csv para inferir o domínio. */
    private static Set<Integer> readDomain(Path seriesCsv) throws IOException {
        if (!Files.exists(seriesCsv)) throw new FileNotFoundException("stage9_series.csv não encontrado: " + seriesCsv);
        Set<Integer> domain = new LinkedHashSet<>();
        try (BufferedReader br = Files.newBufferedReader(seriesCsv, StandardCharsets.UTF_8)) {
            String header = br.readLine();
            int valCol = 1;
            if (header != null) {
                String[] h = header.split(",", -1);
                int candidate = -1;
                for (int i = 0; i < h.length; i++) {
                    String t = h[i].trim().toLowerCase(Locale.ROOT);
                    if (t.equals("value") || t.equals("level") || t.equals("val") || t.equals("sample")) {
                        candidate = i; break;
                    }
                }
                if (candidate >= 0) valCol = candidate;
                else if (h.length == 1) valCol = 0;
            }
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] p = line.split(",", -1);
                if (valCol < p.length) {
                    String s = p[valCol].trim();
                    if (!s.isEmpty()) {
                        try { domain.add(parseIntSafe(s)); } catch (NumberFormatException ignore) {}
                    }
                }
            }
        }
        return domain;
    }
}
