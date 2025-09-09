package com.cmpio;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Stage 12 - PeriodGrid
 * - Lê stage9_series.csv (pasta cmp_stage9_out)
 * - Testa períodos candidatos (39, 78, 117 e divisores de N>1)
 * - Para cada período e para cada ciclo, faz reshape em grade (rows = N/period, cols = period)
 * - Calcula métricas por coluna: distintos, entropia aprox., variância, % binário (0/1, 0/255)
 * - Gera CSVs por período/ciclo + um summary com o melhor período global
 *
 * Agora aceita automaticamente:
 *   (A) "idx,value" (série consolidada única) -> vira um único ciclo: "series"
 *   (B) "idx,cycle_00,cycle_01,..." (colunas por ciclo)
 *   (C) "cycle,idx,value" (arquivo concatenado por linhas - stage9_concat.csv)
 */
public final class Stage12PeriodGrid {

    private Stage12PeriodGrid() {}

    public static final class ColStats {
        public int col;
        public int distinct;
        public double entropyBits;
        public double variance;
        public double pctBinary01;
        public double pctBinary0255;
        public boolean monotonicNonDecreasing;
        public boolean constant;
    }

    public static final class PeriodScore {
        public int period;
        public Map<String, Double> perCycleScore = new LinkedHashMap<>();
        public double totalScore;

        @Override public String toString() {
            return "PeriodScore{period=" + period + ", totalScore=" + totalScore + ", perCycle=" + perCycleScore + "}";
        }
    }

    public static final class Result {
        public int seriesLen;
        public List<String> cycles = new ArrayList<>();
        public List<Integer> candidates = new ArrayList<>();
        public PeriodScore best;
        public Path outDir;
        public Path summary;
        public Map<Integer, Path> perPeriodSummaryCsv = new LinkedHashMap<>();
    }

    /** Ponto de entrada simples: baseDir = pasta do arquivo CMP (onde já existem cmp_stage9_out etc.) */
    public static Result run(Path baseDir) throws IOException {
        Path stage9Dir = baseDir.resolve("cmp_stage9_out");
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        Path concatCsv = stage9Dir.resolve("stage9_concat.csv");

        if (!Files.exists(seriesCsv) && !Files.exists(concatCsv)) {
            throw new FileNotFoundException("Stage 12: não encontrei stage9_series.csv nem stage9_concat.csv em " + stage9Dir);
        }
        Path outDir = baseDir.resolve("cmp_stage12_out");
        Files.createDirectories(outDir);

        // Carrega séries por ciclo, aceitando vários formatos.
        SeriesData sd;
        if (Files.exists(seriesCsv)) {
            sd = readSeriesFlexible(seriesCsv, concatCsv);
        } else {
            // Só existe concat -> lê do concat
            sd = readConcat(concatCsv);
        }

        // Candidatos: 39, 78, 117, + divisores (>1) do comprimento
        Set<Integer> cand = new LinkedHashSet<>(Arrays.asList(39, 78, 117));
        cand.addAll(divisorsGreaterThan1(sd.length));

        Result res = new Result();
        res.seriesLen = sd.length;
        res.cycles.addAll(sd.cycles);
        res.candidates.addAll(cand);
        res.outDir = outDir;

        List<PeriodScore> scores = new ArrayList<>();
        for (int p : cand) {
            if (p <= 1 || (sd.length % p) != 0) continue;
            int rowsCheck = sd.length / p;
            if (rowsCheck <= 1) {
                // Evita grades de 1 linha (p. ex., period == N)
                continue;
            }
            Path periodSummaryCsv = outDir.resolve(String.format("stage12_colstats_period_%03d.csv", p));
            try (BufferedWriter w = Files.newBufferedWriter(periodSummaryCsv)) {
                w.write("cycle,col,distinct,entropyBits,variance,pctBinary01,pctBinary0255,monotonic,constant\n");
                PeriodScore ps = new PeriodScore();
                ps.period = p;

                for (String cycle : sd.cycles) {
                    int[] data = sd.series.get(cycle);
                    if (data.length % p != 0) continue; // segurança
                    ColStats[] stats = computeColStats(data, p);
                    // Export por coluna (por ciclo)
                    for (ColStats c : stats) {
                        w.write(String.format(Locale.ROOT,
                                "%s,%d,%d,%.6f,%.6f,%.4f,%.4f,%s,%s\n",
                                cycle, c.col, c.distinct, c.entropyBits, c.variance,
                                c.pctBinary01, c.pctBinary0255,
                                c.monotonicNonDecreasing, c.constant));
                    }
                    // Score por ciclo: colunas “boas” = baixa entropia ou binárias + alguma monotonia
                    double score = scoreColumns(stats);
                    ps.perCycleScore.put(cycle, score);
                }
                // Score total = soma dos ciclos
                ps.totalScore = ps.perCycleScore.values().stream().mapToDouble(d -> d).sum();
                scores.add(ps);
            }
            res.perPeriodSummaryCsv.put(p, periodSummaryCsv);
        }

        // Escolhe melhor período
        scores.sort((a, b) -> Double.compare(b.totalScore, a.totalScore));
        res.best = scores.isEmpty() ? null : scores.get(0);

        // Summary
        Path summary = outDir.resolve("stage12_best_period.txt");
        try (BufferedWriter w = Files.newBufferedWriter(summary)) {
            w.write("Stage 12 - PeriodGrid\n");
            w.write("seriesLen=" + sd.length + "\n");
            w.write("cycles=" + sd.cycles + "\n");
            w.write("candidates=" + res.candidates + "\n");
            if (res.best != null) {
                w.write("BEST=" + res.best + "\n");
            } else {
                w.write("BEST=<none>\n");
            }
        }
        res.summary = summary;

        // Exporta as grades do melhor período (uma por ciclo)
        if (res.best != null) {
            int p = res.best.period;
            for (String cycle : sd.cycles) {
                int[] data = sd.series.get(cycle);
                if (data.length % p != 0) continue;
                int rows = data.length / p;
                Path gridCsv = outDir.resolve(String.format("stage12_grid_%s_period_%03d.csv", cycle, p));
                try (BufferedWriter w = Files.newBufferedWriter(gridCsv)) {
                    // header
                    String header = join(range(0, p).stream().map(i -> "c" + i).collect(Collectors.toList()), ",");
                    w.write(header);
                    w.write("\n");
                    for (int r = 0; r < rows; r++) {
                        StringBuilder sb = new StringBuilder();
                        for (int c = 0; c < p; c++) {
                            if (c > 0) sb.append(',');
                            sb.append(data[r * p + c]);
                        }
                        w.write(sb.toString());
                        w.write("\n");
                    }
                }
            }
        }

        // console
        System.out.println("[Stage 12] " + (res.best != null ? res.best.toString() : "nenhum período escolhido"));
        return res;
    }

    // Overload: permite injetar candidatos extras e aceitar "remainder"
    public static Result run(Path baseDir,
                             Collection<Integer> extraCandidates,
                             boolean allowRemainder) throws IOException {
        Path stage9Dir = baseDir.resolve("cmp_stage9_out");
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        Path concatCsv = stage9Dir.resolve("stage9_concat.csv");

        if (!Files.exists(seriesCsv) && !Files.exists(concatCsv)) {
            throw new FileNotFoundException("Stage 12: não encontrei stage9_series.csv nem stage9_concat.csv em " + stage9Dir);
        }
        Path outDir = baseDir.resolve("cmp_stage12_out");
        Files.createDirectories(outDir);

        // Carrega séries (mesmo método que você já tem)
        SeriesData sd = Files.exists(seriesCsv) ? readSeriesFlexible(seriesCsv, concatCsv) : readConcat(concatCsv);

        // Candidatos padrão
        Set<Integer> cand = new LinkedHashSet<>(Arrays.asList(39, 78, 117));
        cand.addAll(divisorsGreaterThan1(sd.length));
        if (extraCandidates != null) cand.addAll(extraCandidates);

        Result res = new Result();
        res.seriesLen = sd.length;
        res.cycles.addAll(sd.cycles);
        res.candidates.addAll(cand);
        res.outDir = outDir;

        List<PeriodScore> scores = new ArrayList<>();
        for (int p : cand) {
            boolean divisible = (p > 1) && (sd.length % p == 0);
            if (!divisible && !allowRemainder) continue;

            // quando não divide e allowRemainder=true, aplicamos "trim" simples:
            int usableLen = divisible ? sd.length : (sd.length / p) * p;
            if (usableLen < p) continue; // segurança

            Path periodSummaryCsv = outDir.resolve(String.format(Locale.ROOT,
                    "stage12_colstats_period_%03d%s.csv", p, divisible ? "" : "_trim"));
            try (BufferedWriter w = Files.newBufferedWriter(periodSummaryCsv)) {
                w.write("cycle,col,distinct,entropyBits,variance,pctBinary01,pctBinary0255,monotonic,constant\n");
                PeriodScore ps = new PeriodScore();
                ps.period = p;

                for (String cycle : sd.cycles) {
                    int[] data = sd.series.get(cycle);
                    if (data.length < usableLen) continue;
                    ColStats[] stats = computeColStatsTrimmed(data, p, usableLen);
                    for (ColStats c : stats) {
                        w.write(String.format(Locale.ROOT,
                                "%s,%d,%d,%.6f,%.6f,%.4f,%.4f,%s,%s\n",
                                cycle, c.col, c.distinct, c.entropyBits, c.variance,
                                c.pctBinary01, c.pctBinary0255,
                                c.monotonicNonDecreasing, c.constant));
                    }
                    double score = scoreColumns(stats);
                    ps.perCycleScore.put(cycle, score);
                }
                ps.totalScore = ps.perCycleScore.values().stream().mapToDouble(d -> d).sum();
                scores.add(ps);
            }
            res.perPeriodSummaryCsv.put(p, periodSummaryCsv);
        }

        scores.sort((a, b) -> Double.compare(b.totalScore, a.totalScore));
        res.best = scores.isEmpty() ? null : scores.get(0);

        Path summary = outDir.resolve("stage12_best_period.txt");
        try (BufferedWriter w = Files.newBufferedWriter(summary)) {
            w.write("Stage 12 - PeriodGrid\n");
            w.write("seriesLen=" + sd.length + "\n");
            w.write("cycles=" + sd.cycles + "\n");
            w.write("candidates=" + res.candidates + "\n");
            if (res.best != null) {
                w.write("BEST=" + res.best + "\n");
                if (sd.length % res.best.period != 0) {
                    int rem = sd.length % res.best.period;
                    w.write("NOTE: best period does not divide N; trimmed tail=" + rem + " samples.\n");
                }
            } else {
                w.write("BEST=<none>\n");
            }
        }
        res.summary = summary;

        // Exporta grade só se couber pelo menos 1 linha
        if (res.best != null) {
            int p = res.best.period;
            boolean divisible = (sd.length % p == 0);
            int usableLen = divisible ? sd.length : (sd.length / p) * p;
            int rows = usableLen / p;

            for (String cycle : sd.cycles) {
                int[] data = sd.series.get(cycle);
                if (data.length < usableLen) continue;
                Path gridCsv = outDir.resolve(String.format(Locale.ROOT,
                        "stage12_grid_%s_period_%03d%s.csv", cycle, p, divisible ? "" : "_trim"));
                try (BufferedWriter w = Files.newBufferedWriter(gridCsv)) {
                    // header
                    StringBuilder h = new StringBuilder();
                    for (int c = 0; c < p; c++) { if (c>0) h.append(','); h.append("c").append(c); }
                    w.write(h.toString()); w.write("\n");

                    for (int r = 0; r < rows; r++) {
                        StringBuilder sb = new StringBuilder();
                        for (int c = 0; c < p; c++) {
                            if (c > 0) sb.append(',');
                            sb.append(data[r * p + c]);
                        }
                        w.write(sb.toString()); w.write("\n");
                    }
                }
            }
        }

        System.out.println("[Stage 12] " + (res.best != null ? res.best.toString() : "nenhum período escolhido"));
        return res;
    }

    private static ColStats[] computeColStatsTrimmed(int[] data, int period, int usableLen) {
        int rows = usableLen / period;
        ColStats[] out = new ColStats[period];
        for (int c = 0; c < period; c++) {
            List<Integer> colVals = new ArrayList<>(rows);
            for (int r = 0; r < rows; r++) {
                colVals.add(data[r * period + c]);
            }
            out[c] = columnStats(c, colVals);
        }
        return out;
    }

    // ========= util/heurísticas =========

    private static double scoreColumns(ColStats[] stats) {
        double s = 0.0;
        for (ColStats c : stats) {
            // Heurística: recompensa colunas com baixa entropia, com %binário alto, e monotonia/constância
            double binBoost = Math.max(c.pctBinary01, c.pctBinary0255);
            double entropyTerm = Math.max(0, 8.0 - c.entropyBits); // quanto mais baixa, melhor
            double mono = c.monotonicNonDecreasing ? 1.0 : 0.0;
            double constant = c.constant ? 1.0 : 0.0;
            s += entropyTerm + 4.0 * binBoost + 2.0 * mono + 2.0 * constant;
        }
        return s;
    }

    private static ColStats[] computeColStats(int[] data, int period) {
        int rows = data.length / period;
        ColStats[] out = new ColStats[period];
        for (int c = 0; c < period; c++) {
            List<Integer> colVals = new ArrayList<>(rows);
            for (int r = 0; r < rows; r++) {
                colVals.add(data[r * period + c]);
            }
            out[c] = columnStats(c, colVals);
        }
        return out;
    }

    private static ColStats columnStats(int col, List<Integer> vals) {
        ColStats cs = new ColStats();
        cs.col = col;
        // distintos
        Set<Integer> set = new HashSet<>(vals);
        cs.distinct = set.size();
        cs.constant = (cs.distinct == 1);

        // variância
        double mean = vals.stream().mapToDouble(i -> i).average().orElse(0.0);
        double var = 0.0;
        for (int v : vals) {
            double d = v - mean;
            var += d * d;
        }
        cs.variance = (vals.isEmpty() ? 0.0 : var / vals.size());

        // entropia (aprox.) — frequências em inteiros
        Map<Integer, Integer> freq = new HashMap<>();
        for (int v : vals) freq.merge(v, 1, Integer::sum);
        double H = 0.0;
        int n = vals.size();
        for (int f : freq.values()) {
            double p = f / (double) n;
            if (p > 0) H += -p * (log2(p));
        }
        cs.entropyBits = H;

        // binários
        int bin01 = 0, bin0255 = 0;
        for (int v : vals) {
            if (v == 0 || v == 1) bin01++;
            if (v == 0 || v == 255) bin0255++;
        }
        cs.pctBinary01 = n == 0 ? 0.0 : (bin01 / (double) n);
        cs.pctBinary0255 = n == 0 ? 0.0 : (bin0255 / (double) n);

        // monotonia não-decrescente
        boolean mono = true;
        for (int i = 1; i < vals.size(); i++) {
            if (vals.get(i) < vals.get(i - 1)) { mono = false; break; }
        }
        cs.monotonicNonDecreasing = mono;

        return cs;
    }

    private static double log2(double x) {
        return Math.log(x) / Math.log(2.0);
    }

    private static Set<Integer> divisorsGreaterThan1(int n) {
        Set<Integer> ds = new TreeSet<>();
        for (int i = 2; i <= n; i++) {
            if (n % i == 0) ds.add(i);
        }
        return ds;
    }

    private static String join(List<String> xs, String sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) sb.append(sep);
            sb.append(xs.get(i));
        }
        return sb.toString();
    }

    private static List<Integer> range(int a, int b) {
        List<Integer> r = new ArrayList<>(Math.max(0, b - a));
        for (int i = a; i < b; i++) r.add(i);
        return r;
    }

    // ======== leitura flexível Stage 9 ========

    private static final class SeriesData {
        int length;
        List<String> cycles = new ArrayList<>();
        Map<String, int[]> series = new LinkedHashMap<>();
    }

    /**
     * Modo flexível: tenta ler stage9_series.csv em múltiplos formatos:
     *   (B) idx,cycle_00,cycle_01,... -> colunas por ciclo
     *   (A) idx,value -> série única (ciclo "series")
     * Se não houver (A) nem (B), tenta ler stage9_concat.csv como (C) cycle,idx,value.
     */
    private static SeriesData readSeriesFlexible(Path seriesCsv, Path concatCsv) throws IOException {
        List<String> lines = Files.readAllLines(seriesCsv);
        if (lines.isEmpty()) throw new IOException("CSV vazio: " + seriesCsv);

        String[] header = splitCsv(lines.get(0));
        // Verifica se há colunas cycle_*
        List<Integer> cycleCols = new ArrayList<>();
        List<String> cycleNames = new ArrayList<>();
        int valueCol = -1;
        int idxCol = -1;

        for (int i = 0; i < header.length; i++) {
            String h = header[i].trim();
            String hl = h.toLowerCase(Locale.ROOT);
            if (h.startsWith("cycle_")) {
                cycleCols.add(i);
                cycleNames.add(h);
            } else if (hl.equals("value")) {
                valueCol = i;
            } else if (hl.equals("idx")) {
                idxCol = i;
            }
        }

        if (!cycleCols.isEmpty()) {
            // (B) colunas por ciclo
            return readSeriesMultiColumns(lines, cycleCols, cycleNames);
        } else if (valueCol >= 0) {
            // (A) idx,value — uma única série
            return readSeriesSingleColumn(lines, valueCol);
        } else {
            // Tenta fallback no concat
            if (concatCsv != null && Files.exists(concatCsv)) {
                return readConcat(concatCsv);
            }
            throw new IOException("Stage 12: formato não reconhecido em " + seriesCsv + " (sem 'cycle_*' nem coluna 'value').");
        }
    }

    /** (B) Lê formato com colunas por ciclo: idx,cycle_00,cycle_01,... */
    private static SeriesData readSeriesMultiColumns(List<String> lines, List<Integer> cycleCols, List<String> cycleNames) {
        List<int[]> cols = new ArrayList<>();
        for (int k = 0; k < cycleCols.size(); k++) cols.add(new int[lines.size() - 1]);

        int row = 0;
        for (int i = 1; i < lines.size(); i++) {
            String[] parts = splitCsv(lines.get(i));
            for (int k = 0; k < cycleCols.size(); k++) {
                int col = cycleCols.get(k);
                int v = 0;
                if (col < parts.length) {
                    String s = parts[col].trim();
                    if (!s.isEmpty()) v = parseIntSafe(s);
                }
                cols.get(k)[row] = v;
            }
            row++;
        }
        SeriesData sd = new SeriesData();
        sd.length = row;
        sd.cycles.addAll(cycleNames);
        for (int k = 0; k < cycleNames.size(); k++) {
            sd.series.put(cycleNames.get(k), cols.get(k));
        }
        return sd;
    }

    /** (A) Lê formato consolidado único: idx,value -> ciclo "series" */
    private static SeriesData readSeriesSingleColumn(List<String> lines, int valueCol) {
        List<Integer> vals = new ArrayList<>(lines.size() - 1);
        for (int i = 1; i < lines.size(); i++) {
            String[] parts = splitCsv(lines.get(i));
            if (valueCol >= parts.length) continue;
            String s = parts[valueCol].trim();
            if (s.isEmpty()) continue;
            vals.add(parseIntSafe(s));
        }
        int[] arr = new int[vals.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = vals.get(i);

        SeriesData sd = new SeriesData();
        sd.length = arr.length;
        sd.cycles.add("series");
        sd.series.put("series", arr);
        return sd;
    }

    /** (C) Lê o concat: cycle,idx,value -> várias séries por nome de ciclo */
    private static SeriesData readConcat(Path concatCsv) throws IOException {
        List<String> lines = Files.readAllLines(concatCsv);
        if (lines.isEmpty()) throw new IOException("CSV vazio: " + concatCsv);

        String[] header = splitCsv(lines.get(0));
        int cycleCol = -1, idxCol = -1, valueCol = -1;
        for (int i = 0; i < header.length; i++) {
            String h = header[i].trim().toLowerCase(Locale.ROOT);
            if (h.equals("cycle")) cycleCol = i;
            else if (h.equals("idx")) idxCol = i;
            else if (h.equals("value")) valueCol = i;
        }
        if (cycleCol < 0 || valueCol < 0) {
            throw new IOException("Stage 12: stage9_concat.csv sem colunas esperadas (cycle, value): " + concatCsv);
        }

        // Agrupa por ciclo, preservando ordem por idx (se houver)
        Map<String, List<int[]>> buckets = new LinkedHashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] parts = splitCsv(lines.get(i));
            if (cycleCol >= parts.length || valueCol >= parts.length) continue;
            String cycle = parts[cycleCol].trim();
            int idx = (idxCol >= 0 && idxCol < parts.length) ? parseIntSafe(parts[idxCol].trim()) : i - 1;
            int val = parseIntSafe(parts[valueCol].trim());
            buckets.computeIfAbsent(cycle, k -> new ArrayList<>()).add(new int[]{idx, val});
        }

        // Ordena por idx e materializa arrays
        SeriesData sd = new SeriesData();
        int maxLen = 0;
        for (Map.Entry<String, List<int[]>> e : buckets.entrySet()) {
            String cycle = e.getKey();
            List<int[]> rows = e.getValue();
            rows.sort(Comparator.comparingInt(a -> a[0]));
            int[] arr = new int[rows.size()];
            for (int i = 0; i < arr.length; i++) arr[i] = rows.get(i)[1];
            sd.cycles.add(cycle);
            sd.series.put(cycle, arr);
            maxLen = Math.max(maxLen, arr.length);
        }
        sd.length = maxLen; // atenção: ciclos podem ter tamanhos diferentes
        return sd;
    }

    private static String[] splitCsv(String line) {
        // simples: CSV sem aspas
        return line.split(",", -1);
    }

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s); } catch (Exception ignore) {}
        // aceita hex tipo "0x1A" ou "1A"
        String t = s.toLowerCase(Locale.ROOT);
        if (t.startsWith("0x")) {
            try { return Integer.parseInt(t.substring(2), 16); } catch (Exception ignore2) {}
        } else {
            // tenta hex puro
            try { return Integer.parseInt(t, 16); } catch (Exception ignore3) {}
        }
        return 0;
    }
}
