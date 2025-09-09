package com.cmpio;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lê as saídas do Stage 7 (ops.csv) e reconstroi as amostras,
 * salvando como samples.csv (se precisar reprocessar) ou
 * apenas validando consistência.
 *
 * Obs: o Stage 7 atual já escreve samples.csv. Esta classe
 *       serve para (a) revalidar, (b) reconstruir se só houver ops.csv,
 *       e (c) consolidar ciclos.
 */
public final class Stage8Rebuilder {

    private Stage8Rebuilder() {}

    private static final Pattern INT = Pattern.compile("[-+]?\\d+");

    public static final class Result {
        public int cycles;
        public Map<String,Integer> samplesPerCycle = new LinkedHashMap<>();
        @Override public String toString() {
            return "Stage8.Result{cycles=" + cycles + ", samplesPerCycle=" + samplesPerCycle + "}";
        }
    }

    /** Lê ops.csv (Stage 7) e reconstrói as amostras. */
    public static List<Integer> rebuildFromOps(Path opsCsv) throws IOException {
        List<Integer> samples = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(opsCsv, StandardCharsets.UTF_8)) {
            String line = br.readLine(); // header (pode ser null)
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",", -1); // preserva campos vazios
                if (parts.length < 2) continue;      // precisa pelo menos do 'kind'

                // idx,kind,value,runLen,outSampleIndex
                String kind = parts[1].trim();
                if ("LITERAL".equalsIgnoreCase(kind)) {
                    // value é a 3ª coluna; se não existir, tenta achar 1º int na linha
                    Integer val = (parts.length > 2) ? safeParse(parts[2]) : safeParse(line);
                    if (val != null) samples.add(val);
                } else if ("RUN91".equalsIgnoreCase(kind)) {
                    Integer len = (parts.length > 3) ? safeParse(parts[3]) : safeParse(line);
                    int last = samples.isEmpty() ? 0 : samples.get(samples.size() - 1);
                    int n = (len != null) ? Math.max(0, len) : 0;
                    for (int i = 0; i < n; i++) samples.add(last);
                }
            }
        }
        return samples;
    }

    private static Integer safeParse(String s) {
        if (s == null) return null;
        Matcher m = INT.matcher(s);
        return m.find() ? Integer.parseInt(m.group()) : null;
    }

    /** Processa todos os subdirs cycle_xx dentro de stage7Dir; reconstroi/valida samples.csv. */
    public static Result run(Path stage7Dir) throws IOException {
        Result res = new Result();
        if (!Files.isDirectory(stage7Dir)) return res;

        // Ordena por nome (cycle_00, cycle_01, ...)
        List<Path> cycleDirs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(stage7Dir, "cycle_*")) {
            for (Path p : ds) if (Files.isDirectory(p)) cycleDirs.add(p);
        }
        cycleDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (Path cycleDir : cycleDirs) {
            String name = cycleDir.getFileName().toString();
            Path samplesCsv = cycleDir.resolve("samples.csv");

            if (Files.notExists(samplesCsv) || Files.size(samplesCsv) == 0L) {
                // reconstruir a partir de ops.csv
                Path opsCsv = cycleDir.resolve("ops.csv");
                if (Files.exists(opsCsv)) {
                    List<Integer> samples = rebuildFromOps(opsCsv);
                    Stage7Decoder.writeSamplesCsv(samplesCsv, samples);
                }
            }

            int count = 0;
            if (Files.exists(samplesCsv)) {
                try (BufferedReader br = Files.newBufferedReader(samplesCsv, StandardCharsets.UTF_8)) {
                    String hdr = br.readLine(); // pode ser header ou 1ª linha
                    String line;
                    // Se o arquivo não tem header e a 1ª linha já é um número, conta também
                    if (hdr != null && Stage7Decoder.findFirstInt(hdr) != null && !hdr.toLowerCase(Locale.ROOT).contains("sample")) {
                        count++; // conta a primeira linha como dado
                    }
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty()) continue;
                        count++;
                    }
                }
            }
            res.samplesPerCycle.put(name, count);
            res.cycles++;
        }
        return res;
    }
}
