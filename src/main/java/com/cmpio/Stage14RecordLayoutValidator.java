package com.cmpio;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Stage14RecordLayoutValidator {

    private Stage14RecordLayoutValidator() {}

    public static final class Field {
        public String cycle;
        public int startCol;
        public int widthCols;
        public String kind;   // CONST | FLAG | COUNTER | U16_LE | U16_BE | UNKNOWN
        public String notes;
    }

    public static final class FieldValidation {
        public Field f;
        public boolean valid;
        public String reason;
    }

    public static final class Result {
        public int bestPeriod;
        public List<String> cycles = new ArrayList<>();
        public Path outDir;
        public Path finalCsv;
        public Path finalTxt;
        public Map<String, List<FieldValidation>> perCycle = new LinkedHashMap<>();

        @Override public String toString() {
            return "Stage14.Result{period=" + bestPeriod + ", cycles=" + cycles + ", outDir=" + outDir + "}";
        }
    }

    public static Result run(Path baseDir) throws IOException {
        Path s12 = baseDir.resolve("cmp_stage12_out");
        Path s13 = baseDir.resolve("cmp_stage13_out");
        Path s9  = baseDir.resolve("cmp_stage9_out");
        Path bestTxt = s12.resolve("stage12_best_period.txt");
        Path layoutCsv = s13.resolve("stage13_layout.csv");

        if (!Files.exists(layoutCsv)) throw new FileNotFoundException("stage13_layout.csv não encontrado: " + layoutCsv);

        // 1) período robusto (regex + fallbacks)
        int period = -1;
        if (Files.exists(bestTxt)) {
            period = readBestPeriodFlexible(bestTxt);
        }
        if (period <= 1) {
            Integer p = detectPeriodFromGrids(s12);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            Integer p = detectPeriodFromColstats(s12);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            Integer p = detectPeriodFromStage9(s9);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            throw new IOException("Não consegui obter um período válido (Stage 12 BEST ausente e sem grids/colstats úteis).");
        }

        Path outDir = baseDir.resolve("cmp_stage14_out");
        Files.createDirectories(outDir);

        // 2) Carrega layout inferido no Stage 13
        Map<String, List<Field>> perCycleFields = readLayout(layoutCsv);
        List<String> cycles = new ArrayList<>(perCycleFields.keySet());
        Collections.sort(cycles);

        Result res = new Result();
        res.bestPeriod = period;
        res.cycles.addAll(cycles);
        res.outDir = outDir;

        // 3) Validar por ciclo usando grid do Stage12; se não houver, reconstruir do Stage9
        SeriesData sd = readSeriesFlexible(s9.resolve("stage9_series.csv"), s9.resolve("stage9_concat.csv"));

        for (String cycle : cycles) {
            int[][] M = null;

            // tenta grid “cheio” e “trim”
            Path grid = s12.resolve(String.format("stage12_grid_%s_period_%03d.csv", cycle, period));
            if (Files.exists(grid)) {
                M = readGrid(grid);
            } else {
                Path gridTrim = s12.resolve(String.format("stage12_grid_%s_period_%03d_trim.csv", cycle, period));
                if (Files.exists(gridTrim)) {
                    M = readGrid(gridTrim);
                }
            }

            // fallback: reconstruir do Stage 9
            if (M == null) {
                int[] series = sd.series.get(cycle);
                if (series == null) {
                    throw new FileNotFoundException(
                            "Grade do ciclo não encontrada: " + grid +
                                    " (nem _trim). E ciclo '" + cycle + "' não existe no Stage 9 para reconstrução."
                    );
                }
                M = reshapeSeriesToGrid(series, period);
            }

            List<FieldValidation> vlist = new ArrayList<>();
            for (Field f : perCycleFields.get(cycle)) {
                vlist.add(validateField(M, f));
            }
            res.perCycle.put(cycle, vlist);
        }

        // 4) Export CSV final
        Path finalCsv = outDir.resolve("stage14_layout_final.csv");
        try (BufferedWriter w = Files.newBufferedWriter(finalCsv)) {
            w.write("cycle,startCol,widthCols,kind,valid,reason,notes\n");
            for (String cycle : cycles) {
                for (FieldValidation v : res.perCycle.get(cycle)) {
                    w.write(String.format(Locale.ROOT, "%s,%d,%d,%s,%s,%s,%s\n",
                            cycle, v.f.startCol, v.f.widthCols, v.f.kind,
                            v.valid, sanitize(v.reason), sanitize(v.f.notes)));
                }
            }
        }
        res.finalCsv = finalCsv;

        // 5) Resumo TXT
        Path finalTxt = outDir.resolve("stage14_layout_final.txt");
        try (BufferedWriter w = Files.newBufferedWriter(finalTxt)) {
            w.write("Stage 14 - RecordLayoutValidator\n");
            w.write("bestPeriod=" + period + "\n");
            w.write("cycles=" + cycles + "\n\n");
            for (String cycle : cycles) {
                w.write("[" + cycle + "]\n");
                for (FieldValidation v : res.perCycle.get(cycle)) {
                    w.write(String.format("  - col %d..%d %-9s -> %s (%s) %s\n",
                            v.f.startCol, v.f.startCol + v.f.widthCols - 1,
                            v.f.kind, v.valid ? "OK" : "FAIL",
                            (v.reason == null ? "" : v.reason),
                            (v.f.notes == null ? "" : v.f.notes)));
                }
                w.write("\n");
            }
        }
        res.finalTxt = finalTxt;

        System.out.println("[Stage 14] " + res);
        return res;
    }

    // ===== validação =====

    private static FieldValidation validateField(int[][] M, Field f) {
        FieldValidation v = new FieldValidation();
        v.f = f;
        v.valid = true;
        v.reason = "ok";

        int rows = M.length;
        int cols = (rows == 0 ? 0 : M[0].length);
        if (f.startCol < 0 || f.startCol + f.widthCols > cols) {
            v.valid = false; v.reason = "fora dos limites do período"; return v;
        }
        if ((f.kind.equals("U16_LE") || f.kind.equals("U16_BE")) && f.widthCols != 2) {
            v.valid = false; v.reason = "U16 requer widthCols=2"; return v;
        }

        switch (f.kind) {
            case "CONST": {
                boolean ok = isConstantColumn(M, f.startCol);
                v.valid = ok; v.reason = ok ? "constante" : "variação detectada";
                break;
            }
            case "FLAG": {
                boolean ok = isBinaryColumn(M, f.startCol, 0, 1) || isBinaryColumn(M, f.startCol, 0, 255);
                v.valid = ok; v.reason = ok ? "binário" : "valores fora de {0,1}/{0,255}";
                break;
            }
            case "COUNTER": {
                boolean ok = isMonotonicNonDecreasing(M, f.startCol) && deltaVariance(M, f.startCol) < 5.0;
                v.valid = ok; v.reason = ok ? "monótono" : "não monótono/ruidoso";
                break;
            }
            case "U16_LE": {
                boolean ok = pairDeltaVar(M, f.startCol, f.startCol + 1, true) < 50.0;
                v.valid = ok; v.reason = ok ? "U16 LE coeso" : "U16 LE inconsistente";
                break;
            }
            case "U16_BE": {
                boolean ok = pairDeltaVar(M, f.startCol, f.startCol + 1, false) < 50.0;
                v.valid = ok; v.reason = ok ? "U16 BE coeso" : "U16 BE inconsistente";
                break;
            }
            default: {
                v.valid = true; v.reason = "não testado";
            }
        }
        return v;
    }

    // ===== util métricas/IO =====

    private static boolean isConstantColumn(int[][] M, int c) {
        if (M.length == 0) return true;
        int v = M[0][c];
        for (int r = 1; r < M.length; r++) if (M[r][c] != v) return false;
        return true;
    }

    private static boolean isBinaryColumn(int[][] M, int c, int a, int b) {
        for (int[] row : M) {
            int v = row[c];
            if (v != a && v != b) return false;
        }
        return true;
    }

    private static boolean isMonotonicNonDecreasing(int[][] M, int c) {
        for (int r = 1; r < M.length; r++) if (M[r][c] < M[r - 1][c]) return false;
        return true;
    }

    private static double deltaVariance(int[][] M, int c) {
        if (M.length <= 1) return 0.0;
        List<Integer> deltas = new ArrayList<>(M.length - 1);
        for (int r = 1; r < M.length; r++) deltas.add(M[r][c] - M[r - 1][c]);
        return variance(deltas);
    }

    private static double pairDeltaVar(int[][] M, int c0, int c1, boolean le) {
        if (M.length <= 1) return 0.0;
        List<Integer> vals = new ArrayList<>(M.length);
        for (int[] row : M) {
            int lo = row[c0] & 0xFF, hi = row[c1] & 0xFF;
            int v = le ? (lo | (hi << 8)) : (hi | (lo << 8));
            vals.add(v);
        }
        List<Integer> deltas = new ArrayList<>(vals.size() - 1);
        for (int i = 1; i < vals.size(); i++) deltas.add(vals.get(i) - vals.get(i - 1));
        return variance(deltas);
    }

    private static double variance(List<Integer> xs) {
        if (xs.isEmpty()) return 0.0;
        double mean = xs.stream().mapToDouble(i -> i).average().orElse(0.0);
        double var = 0.0;
        for (int v : xs) { double d = v - mean; var += d * d; }
        return var / xs.size();
    }

    private static String sanitize(String s) {
        if (s == null) return "";
        return s.replace('\n', ' ').replace('\r', ' ').replace(',', ';');
    }

    // ========= período robusto =========

    private static int readBestPeriodFlexible(Path bestTxt) throws IOException {
        Pattern P = Pattern.compile("\\bperiod\\s*=\\s*(\\d+)");
        for (String ln : Files.readAllLines(bestTxt)) {
            Matcher m = P.matcher(ln);
            if (m.find()) {
                try { return Integer.parseInt(m.group(1)); } catch (Exception ignore) {}
            }
        }
        return -1;
    }

    private static Integer detectPeriodFromGrids(Path stage12Dir) throws IOException {
        Map<Integer,Integer> freq = new HashMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(stage12Dir, "stage12_grid_*_period_*.csv")) {
            Pattern P = Pattern.compile("_period_(\\d{3})(?:_trim)?\\.csv$");
            for (Path p : ds) {
                String fn = p.getFileName().toString();
                Matcher m = P.matcher(fn);
                if (m.find()) {
                    int per = Integer.parseInt(m.group(1));
                    freq.merge(per, 1, Integer::sum);
                }
            }
        }
        return pickMostFrequent(freq);
    }

    private static Integer detectPeriodFromColstats(Path stage12Dir) throws IOException {
        Map<Integer,Integer> freq = new HashMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(stage12Dir, "stage12_colstats_period_*.csv")) {
            Pattern P = Pattern.compile("_period_(\\d{3})\\.csv$");
            for (Path p : ds) {
                String fn = p.getFileName().toString();
                Matcher m = P.matcher(fn);
                if (m.find()) {
                    int per = Integer.parseInt(m.group(1));
                    freq.merge(per, 1, Integer::sum);
                }
            }
        }
        return pickMostFrequent(freq);
    }

    private static Integer detectPeriodFromStage9(Path stage9Dir) throws IOException {
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        if (!Files.exists(seriesCsv)) return null;
        List<String> lines = Files.readAllLines(seriesCsv);
        int N = Math.max(0, lines.size() - 1);
        if (N <= 1) return null;

        List<Integer> divisores = new ArrayList<>();
        for (int d = 2; d <= N; d++) if (N % d == 0) divisores.add(d);

        List<Integer> prefer = Arrays.asList(78, 39, 117, 26, 13);
        for (int p : prefer) if (divisores.contains(p)) return p;

        return divisores.isEmpty() ? null : divisores.get(divisores.size()-1);
    }

    private static Integer pickMostFrequent(Map<Integer,Integer> freq) {
        int bestP = -1, bestC = -1;
        for (Map.Entry<Integer,Integer> e : freq.entrySet()) {
            int p = e.getKey(), c = e.getValue();
            if (c > bestC || (c == bestC && p > bestP)) { bestC = c; bestP = p; }
        }
        return (bestP > 1) ? bestP : null;
    }

    // ========= leitura de layout / grid / séries =========

    private static Map<String, List<Field>> readLayout(Path layoutCsv) throws IOException {
        Map<String, List<Field>> map = new LinkedHashMap<>();
        List<String> lines = Files.readAllLines(layoutCsv);
        for (int i = 1; i < lines.size(); i++) {
            String[] p = lines.get(i).split(",", -1);
            if (p.length < 5) continue;
            Field f = new Field();
            f.cycle = p[0].trim();
            f.startCol = parseIntSafe(p[1]);
            f.widthCols = parseIntSafe(p[2]);
            f.kind = p[3].trim();
            f.notes = (p.length >= 5 ? p[4].trim() : "");
            map.computeIfAbsent(f.cycle, k -> new ArrayList<>()).add(f);
        }
        return map;
    }

    private static int[][] readGrid(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        if (lines.isEmpty()) return new int[0][0];
        List<int[]> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] parts = lines.get(i).split(",", -1);
            int[] r = new int[parts.length];
            for (int j = 0; j < parts.length; j++) r[j] = parseIntSafe(parts[j].trim());
            rows.add(r);
        }
        int[][] M = new int[rows.size()][];
        for (int i = 0; i < rows.size(); i++) M[i] = rows.get(i);
        return M;
    }

    // ===== Stage 9 fallback =====

    private static final class SeriesData {
        int length;
        List<String> cycles = new ArrayList<>();
        Map<String, int[]> series = new LinkedHashMap<>();
    }

    private static SeriesData readSeriesFlexible(Path seriesCsv, Path concatCsv) throws IOException {
        if (Files.exists(seriesCsv)) {
            return readSeriesSingleColumn(seriesCsv);
        }
        if (Files.exists(concatCsv)) {
            return readConcat(concatCsv);
        }
        SeriesData sd = new SeriesData();
        sd.length = 0;
        return sd;
    }

    private static SeriesData readSeriesSingleColumn(Path seriesCsv) throws IOException {
        List<String> lines = Files.readAllLines(seriesCsv);
        SeriesData sd = new SeriesData();
        if (lines.isEmpty()) { sd.length = 0; sd.cycles.add("series"); sd.series.put("series", new int[0]); return sd; }
        List<Integer> vals = new ArrayList<>(lines.size() - 1);
        for (int i = 1; i < lines.size(); i++) {
            String[] p = lines.get(i).split(",", -1);
            if (p.length < 2) continue;
            String s = p[1].trim();
            if (s.isEmpty()) continue;
            try { vals.add(Integer.parseInt(s)); } catch (Exception ignore) {}
        }
        int[] arr = new int[vals.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = vals.get(i);
        sd.length = arr.length;
        sd.cycles.add("series");
        sd.series.put("series", arr);
        return sd;
    }

    private static SeriesData readConcat(Path concatCsv) throws IOException {
        List<String> lines = Files.readAllLines(concatCsv);
        SeriesData sd = new SeriesData();
        if (lines.isEmpty()) { sd.length = 0; return sd; }

        String[] header = lines.get(0).split(",", -1);
        int cycleCol = -1, idxCol = -1, valueCol = -1;
        for (int i = 0; i < header.length; i++) {
            String h = header[i].trim().toLowerCase(Locale.ROOT);
            if (h.equals("cycle")) cycleCol = i;
            else if (h.equals("idx")) idxCol = i;
            else if (h.equals("value")) valueCol = i;
        }
        if (cycleCol < 0 || valueCol < 0) return sd;

        Map<String, List<int[]>> buckets = new LinkedHashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] parts = lines.get(i).split(",", -1);
            if (cycleCol >= parts.length || valueCol >= parts.length) continue;
            String cycle = parts[cycleCol].trim();
            int idx = (idxCol >= 0 && idxCol < parts.length) ? parseIntSafe(parts[idxCol].trim()) : (i - 1);
            int val = parseIntSafe(parts[valueCol].trim());
            buckets.computeIfAbsent(cycle, k -> new ArrayList<>()).add(new int[]{idx, val});
        }

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
        sd.length = maxLen;
        return sd;
    }

    private static int[][] reshapeSeriesToGrid(int[] data, int period) {
        if (data == null || period <= 0) return new int[0][0];
        int rows = data.length / period;
        if (rows == 0) return new int[0][0];
        int[][] M = new int[rows][period];
        for (int r = 0; r < rows; r++) {
            System.arraycopy(data, r * period, M[r], 0, period);
        }
        return M;
    }

    // ===== util parse num =====

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s); } catch (Exception ignore) {}
        String t = s.toLowerCase(Locale.ROOT);
        if (t.startsWith("0x")) { try { return Integer.parseInt(t.substring(2), 16); } catch (Exception ignore2) {} }
        else { try { return Integer.parseInt(t, 16); } catch (Exception ignore3) {} }
        return 0;
    }
}
