package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Lê os CSVs do Stage 7 (samples.csv em cada cycle_XX) e produz:
 *  - stage9_series.csv    : série consolidada por índice (modo/mediana dos ciclos)
 *  - stage9_concat.csv    : concatenação (para debugging/visualização)
 *
 * Regras:
 *  - Se todos os ciclos têm o mesmo comprimento L, para cada idx em [0..L-1] escolhe o valor por "modo" (mais frequente).
 *    Em empate, prioriza o valor do último ciclo.
 *  - Se os comprimentos divergem, escolhe o ciclo mais longo como "golden" para stage9_series.csv.
 */
public final class Stage9Stitcher {

    private Stage9Stitcher() {}

    public static final class Result {
        public final int cycles;
        public final int seriesLen;
        public final Path outDir;
        public final Path consolidatedCsv; // stage9_series.csv
        public final Path concatenatedCsv; // stage9_concat.csv
        public final Map<String, Integer> cycleLengths;

        public Result(int cycles, int seriesLen, Path outDir, Path consolidatedCsv, Path concatenatedCsv,
                      Map<String, Integer> cycleLengths) {
            this.cycles = cycles;
            this.seriesLen = seriesLen;
            this.outDir = outDir;
            this.consolidatedCsv = consolidatedCsv;
            this.concatenatedCsv = concatenatedCsv;
            this.cycleLengths = cycleLengths;
        }

        @Override
        public String toString() {
            return "Stage9.Result{cycles=" + cycles +
                    ", seriesLen=" + seriesLen +
                    ", outDir=" + outDir +
                    ", consolidated=" + consolidatedCsv +
                    ", concatenated=" + concatenatedCsv +
                    ", cycleLengths=" + cycleLengths + "}";
        }
    }

    public static Result run(Path stage7OutDir, Path outDir) throws IOException {
        if (!Files.exists(stage7OutDir)) {
            throw new FileNotFoundException("Stage 7 output dir não existe: " + stage7OutDir);
        }
        if (!Files.exists(outDir)) Files.createDirectories(outDir);

        // Descobrir os subdirs cycle_XX
        List<Path> cycleDirs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(stage7OutDir)) {
            for (Path p : ds) {
                String name = p.getFileName().toString();
                if (Files.isDirectory(p) && name.startsWith("cycle_")) {
                    cycleDirs.add(p);
                }
            }
        }
        cycleDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

        Map<String, int[]> samplesByCycle = new LinkedHashMap<>();
        Map<String, Integer> lensByCycle  = new LinkedHashMap<>();

        for (Path cdir : cycleDirs) {
            Path samplesCsv = cdir.resolve("samples.csv");
            int[] vals = readSamplesCsv(samplesCsv);
            samplesByCycle.put(cdir.getFileName().toString(), vals);
            lensByCycle.put(cdir.getFileName().toString(), vals.length);
        }

        // Concat para debug
        Path concatCsv = outDir.resolve("stage9_concat.csv");
        try (BufferedWriter bw = Files.newBufferedWriter(concatCsv, StandardCharsets.UTF_8)) {
            bw.write("cycle,idx,value\n");
            for (Map.Entry<String, int[]> e : samplesByCycle.entrySet()) {
                String cycle = e.getKey();
                int[] v = e.getValue();
                for (int i = 0; i < v.length; i++) {
                    bw.write(cycle);
                    bw.write(',');
                    bw.write(Integer.toString(i));
                    bw.write(',');
                    bw.write(Integer.toString(v[i]));
                    bw.write('\n');
                }
            }
        }

        // Consolidar
        int seriesLen;
        Path consolidatedCsv = outDir.resolve("stage9_series.csv");

        if (samplesByCycle.isEmpty()) {
            // nada para consolidar
            try (BufferedWriter bw = Files.newBufferedWriter(consolidatedCsv, StandardCharsets.UTF_8)) {
                bw.write("idx,value\n");
            }
            return new Result(0, 0, outDir, consolidatedCsv, concatCsv, lensByCycle);
        }

        // Verificar se todos têm o mesmo tamanho
        Set<Integer> sizes = new HashSet<>(lensByCycle.values());
        if (sizes.size() == 1) {
            // modo/mediana por índice
            seriesLen = sizes.iterator().next();
            int[] consolidated = new int[seriesLen];
            List<int[]> series = new ArrayList<>(samplesByCycle.values());

            for (int i = 0; i < seriesLen; i++) {
                // coletar valores desse índice em todos os ciclos
                Map<Integer, Integer> freq = new HashMap<>();
                for (int[] s : series) {
                    int val = s[i];
                    freq.put(val, freq.getOrDefault(val, 0) + 1);
                }
                // pegar o "preferido": maior frequência, desempate pelo último ciclo
                int chosen = series.get(series.size() - 1)[i];
                int bestCount = -1;
                for (Map.Entry<Integer, Integer> f : freq.entrySet()) {
                    int v = f.getKey();
                    int count = f.getValue();
                    if (count > bestCount || (count == bestCount && v == chosen)) {
                        bestCount = count;
                        chosen = v;
                    }
                }
                consolidated[i] = chosen;
            }

            try (BufferedWriter bw = Files.newBufferedWriter(consolidatedCsv, StandardCharsets.UTF_8)) {
                bw.write("idx,value\n");
                for (int i = 0; i < consolidated.length; i++) {
                    bw.write(Integer.toString(i));
                    bw.write(',');
                    bw.write(Integer.toString(consolidated[i]));
                    bw.write('\n');
                }
            }
        } else {
            // tamanhos diferentes: pega o ciclo mais longo
            String longest = null;
            int best = -1;
            for (Map.Entry<String, Integer> e : lensByCycle.entrySet()) {
                if (e.getValue() > best) {
                    best = e.getValue();
                    longest = e.getKey();
                }
            }
            seriesLen = best;
            int[] chosen = samplesByCycle.get(longest);

            try (BufferedWriter bw = Files.newBufferedWriter(consolidatedCsv, StandardCharsets.UTF_8)) {
                bw.write("idx,value\n");
                for (int i = 0; i < chosen.length; i++) {
                    bw.write(Integer.toString(i));
                    bw.write(',');
                    bw.write(Integer.toString(chosen[i]));
                    bw.write('\n');
                }
            }
        }

        Result res = new Result(cycleDirs.size(), seriesLen, outDir, consolidatedCsv, concatCsv, lensByCycle);
        System.out.println("[Stage 9] " + res);
        return res;
    }

    /** Lê um samples.csv robustamente. Aceita cabeçalho e tenta achar a coluna "value"; se não achar, usa a 2ª coluna. */
    static int[] readSamplesCsv(Path csv) throws IOException {
        if (!Files.exists(csv)) {
            throw new FileNotFoundException("samples.csv não encontrado: " + csv);
        }
        List<Integer> vals = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            String header = br.readLine();
            if (header == null) return new int[0];

            String[] h = header.split(",");
            int valueIdx = -1;
            for (int i = 0; i < h.length; i++) {
                String col = h[i].trim().toLowerCase(Locale.ROOT);
                if (col.equals("value") || col.equals("val") || col.equals("sample") || col.equals("level")) {
                    valueIdx = i;
                    break;
                }
            }
            if (valueIdx < 0) {
                // se não achou, tenta 2ª coluna
                valueIdx = (h.length >= 2) ? 1 : 0;
            }

            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                if (valueIdx >= parts.length) continue;
                String s = parts[valueIdx].trim();
                if (s.isEmpty()) continue;
                try {
                    vals.add(Integer.parseInt(s));
                } catch (NumberFormatException nfe) {
                    // ignora linhas inválidas
                }
            }
        }
        int[] out = new int[vals.size()];
        for (int i = 0; i < out.length; i++) out[i] = vals.get(i);
        return out;
    }
}
