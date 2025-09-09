package com.cmpio;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Stage15RecordDeserializer {

    private Stage15RecordDeserializer() {}

    public static final class Field {
        public String cycle;
        public int startCol;
        public int widthCols;
        public String kind;
        public String notes;
    }

    public static final class Result {
        public int bestPeriod;
        public List<String> cycles = new ArrayList<>();
        public Path outDir;
        public Map<String, Path> perCycleCsv = new LinkedHashMap<>();
        public Path allCsv;

        @Override public String toString() {
            return "Stage15.Result{period=" + bestPeriod + ", cycles=" + cycles + ", outDir=" + outDir + "}";
        }
    }

    public static Result run(Path baseDir) throws IOException {
        Path s12 = baseDir.resolve("cmp_stage12_out");
        Path s14 = baseDir.resolve("cmp_stage14_out");
        Path s9  = baseDir.resolve("cmp_stage9_out");
        Path bestTxt = s12.resolve("stage12_best_period.txt");
        Path finalCsv = s14.resolve("stage14_layout_final.csv");

        if (!Files.exists(finalCsv)) {
            throw new FileNotFoundException("stage14_layout_final.csv não encontrado: " + finalCsv);
        }

        // ===== período robusto =====
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

        // Campos válidos do Stage 14
        Map<String, List<Field>> perCycle = readLayout(finalCsv, true);
        List<String> cycles = new ArrayList<>(perCycle.keySet());
        Collections.sort(cycles);

        Path outDir = baseDir.resolve("cmp_stage15_out");
        Files.createDirectories(outDir);

        Result res = new Result();
        res.bestPeriod = period;
        res.cycles.addAll(cycles);
        res.outDir = outDir;

        // Construir headers por ciclo
        Map<String, List<String>> perCycleHeaders = new LinkedHashMap<>();
        for (String cycle : cycles) {
            List<String> headers = new ArrayList<>();
            for (Field f : perCycle.get(cycle)) {
                String base = String.format("%s_%s_c%02d", cycle, f.kind, f.startCol);
                if (f.widthCols == 1) headers.add(base);
                else if (f.widthCols == 2) headers.add(base); // U16_* etc
                else headers.add(base + "_w" + f.widthCols);
            }
            perCycleHeaders.put(cycle, headers);
        }

        // Carregar séries do Stage 9 (para eventual fallback na grade)
        SeriesData sd = readSeriesFlexible(s9.resolve("stage9_series.csv"), s9.resolve("stage9_concat.csv"));

        // Por ciclo: ler grid (ou reconstruir) e exportar registros
        List<String[]> allRows = new ArrayList<>();
        List<String> allHeader = null;

        for (String cycle : cycles) {
            int[][] M = readGridOrRebuild(s12, sd, cycle, period);
            if (M.length == 0) {
                System.out.println("[Stage 15] Aviso: ciclo " + cycle + " possui 0 linhas — pulando.");
                continue;
            }

            List<Field> fields = perCycle.get(cycle);
            if (fields == null || fields.isEmpty()) {
                System.out.println("[Stage 15] Nenhum campo válido para ciclo " + cycle + " — pulando.");
                continue;
            }
            List<String> headers = perCycleHeaders.get(cycle);
            if (headers == null || headers.isEmpty()) {
                System.out.println("[Stage 15] Ciclo " + cycle + " sem headers — pulando.");
                continue;
            }

            // CSV por ciclo
            Path cycleCsv = outDir.resolve(String.format("stage15_records_%s.csv", cycle));
            try (BufferedWriter w = Files.newBufferedWriter(cycleCsv)) {
                w.write(String.join(",", headers)); w.write("\n");
                for (int r = 0; r < M.length; r++) {
                    List<String> row = new ArrayList<>(headers.size());
                    for (Field f : fields) {
                        row.add(String.valueOf(extractFieldValue(M, r, f)));
                    }
                    w.write(String.join(",", row)); w.write("\n");
                }
            }
            res.perCycleCsv.put(cycle, cycleCsv);

            // Monta header do ALL (primeira vez)
            List<String> allHdr = new ArrayList<>();
            allHdr.add("cycle");
            allHdr.addAll(headers);
            if (allHeader == null) allHeader = allHdr;

            // Adiciona linhas do ALL
            for (int r = 0; r < M.length; r++) {
                List<String> row = new ArrayList<>();
                row.add(cycle);
                for (Field f : fields) row.add(String.valueOf(extractFieldValue(M, r, f)));
                allRows.add(row.toArray(new String[0]));
            }
        }

        // Export “ALL”
        Path allCsv = outDir.resolve("stage15_records_all.csv");
        try (BufferedWriter w = Files.newBufferedWriter(allCsv)) {
            if (allHeader == null) {
                // nenhum ciclo gerou linhas — emite CSV válido mínimo
                allHeader = Collections.singletonList("cycle");
            }
            w.write(String.join(",", allHeader));
            w.write("\n");
            for (String[] a : allRows) {
                w.write(String.join(",", a));
                w.write("\n");
            }
        }
        res.allCsv = allCsv;

        System.out.println("[Stage 15] " + res);
        return res;
    }

    // ===== helpers principais =====

    private static long extractFieldValue(int[][] M, int row, Field f) {
        int c = f.startCol;
        switch (f.kind) {
            case "CONST":
            case "FLAG":
            case "COUNTER":
            case "UNKNOWN":
                return asUnsigned(M[row][c]);
            case "U16_LE":
                return (asUnsigned(M[row][c]) | (asUnsigned(M[row][c + 1]) << 8));
            case "U16_BE":
                return (asUnsigned(M[row][c + 1]) | (asUnsigned(M[row][c]) << 8));
            default:
                return asUnsigned(M[row][c]);
        }
    }

    private static int asUnsigned(int v) {
        return v & 0xFF;
    }

    // ===== período robusto (igual filosofia do Stage 14) =====

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

    // ===== leitura de layout =====

    private static Map<String, List<Field>> readLayout(Path finalCsv, boolean onlyValid) throws IOException {
        Map<String, List<Field>> map = new LinkedHashMap<>();
        List<String> lines = Files.readAllLines(finalCsv);
        for (int i = 1; i < lines.size(); i++) {
            String[] p = lines.get(i).split(",", -1);
            if (p.length < 6) continue;
            boolean valid = Boolean.parseBoolean(p[4].trim());
            if (onlyValid && !valid) continue;

            Field f = new Field();
            f.cycle = p[0].trim();
            f.startCol = parseIntSafe(p[1]);
            f.widthCols = parseIntSafe(p[2]);
            f.kind = p[3].trim();
            f.notes = (p.length >= 7 ? p[6].trim() : "");
            map.computeIfAbsent(f.cycle, k -> new ArrayList<>()).add(f);
        }
        return map;
    }

    // ===== carregar grid (com fallback) =====

    private static int[][] readGridOrRebuild(Path stage12Dir, SeriesData sd, String cycle, int period) throws IOException {
        Path grid = stage12Dir.resolve(String.format("stage12_grid_%s_period_%03d.csv", cycle, period));
        if (Files.exists(grid)) return readGrid(grid);

        Path gridTrim = stage12Dir.resolve(String.format("stage12_grid_%s_period_%03d_trim.csv", cycle, period));
        if (Files.exists(gridTrim)) return readGrid(gridTrim);

        // Reconstruir do Stage 9
        int[] series = sd.series.get(cycle);
        if (series == null) {
            throw new FileNotFoundException("Grade não encontrada para ciclo " + cycle + " (nem _trim), e não existe série no Stage 9 para reconstrução.");
        }
        return reshapeSeriesToGrid(series, period);
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

    // ===== Stage 9 séries (para fallback) =====

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

    // ===== util numérico =====

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s); } catch (Exception ignore) {}
        String t = s.toLowerCase(Locale.ROOT);
        if (t.startsWith("0x")) { try { return Integer.parseInt(t.substring(2), 16); } catch (Exception ignore2) {} }
        else { try { return Integer.parseInt(t, 16); } catch (Exception ignore3) {} }
        return 0;
    }
}
