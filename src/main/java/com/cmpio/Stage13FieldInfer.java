package com.cmpio;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stage 13 - FieldInfer (robusto a BEST ausente e ausência de grids no Stage 12)
 */
public final class Stage13FieldInfer {

    private Stage13FieldInfer() {}

    public static final class Field {
        public int startCol;     // coluna inicial (no período)
        public int widthCols;    // quantas colunas ocupa
        public String kind;      // CONST | FLAG | COUNTER | U16_LE | U16_BE | UNKNOWN
        public String notes;

        @Override public String toString() {
            return String.format(Locale.ROOT, "Field{col=%d..%d, width=%d, kind=%s, notes=%s}",
                    startCol, startCol + widthCols - 1, widthCols, kind, notes);
        }
    }

    public static final class Result {
        public int bestPeriod;
        public List<String> cycles = new ArrayList<>();
        public Map<String, List<Field>> perCycleFields = new LinkedHashMap<>();
        public Path outDir;
        public Path layoutCsv;
        public Path layoutTxt;
    }

    // ---------- Execução principal ----------

    public static Result run(Path baseDir) throws IOException {
        Path stage12Dir = baseDir.resolve("cmp_stage12_out");
        Path stage9Dir  = baseDir.resolve("cmp_stage9_out");
        Path bestTxt    = stage12Dir.resolve("stage12_best_period.txt");
        if (!Files.exists(bestTxt)) {
            throw new FileNotFoundException("Não encontrei stage12_best_period.txt em " + bestTxt);
        }

        // 1) Descobrir período
        int period = readBestPeriod(bestTxt);
        if (period <= 1) {
            Integer p = detectPeriodFromGrids(stage12Dir);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            Integer p = detectPeriodFromColstats(stage12Dir);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            Integer p = detectPeriodFromStage9(stage9Dir);
            if (p != null && p > 1) period = p;
        }
        if (period <= 1) {
            throw new IOException("Período inválido/indisponível no Stage 12 (não há BEST nem grids/colstats úteis).");
        }

        // 2) Obter ciclos: primeiro pelos grids do Stage 12; se vier vazio, pega do Stage 9
        List<String> cycles = findCycles(stage12Dir, period);
        SeriesData sd = readSeriesFlexible(stage9Dir.resolve("stage9_series.csv"),
                stage9Dir.resolve("stage9_concat.csv"));
        if (cycles.isEmpty()) {
            // Sem grids para este período? usa os ciclos do Stage 9
            cycles = new ArrayList<>(sd.cycles);
        }

        if (cycles.isEmpty()) {
            throw new IOException("Nenhum ciclo disponível (nem grids no Stage12, nem séries no Stage9).");
        }

        Result res = new Result();
        res.bestPeriod = period;
        res.cycles.addAll(cycles);

        Path outDir = baseDir.resolve("cmp_stage13_out");
        Files.createDirectories(outDir);
        res.outDir = outDir;

        // 3) Para cada ciclo: usar grid do Stage 12; se ausente, reconstruir da série do Stage 9
        List<String> allCycles = new ArrayList<>(cycles);
        Collections.sort(allCycles);

        for (String cycle : allCycles) {
            int[][] matrix = null;

            // tenta grid “cheio”
            Path grid = stage12Dir.resolve(String.format(Locale.ROOT,
                    "stage12_grid_%s_period_%03d.csv", cycle, period));
            if (Files.exists(grid)) {
                matrix = readGrid(grid);
            } else {
                // tenta grid “trim”
                Path gridTrim = stage12Dir.resolve(String.format(Locale.ROOT,
                        "stage12_grid_%s_period_%03d_trim.csv", cycle, period));
                if (Files.exists(gridTrim)) {
                    matrix = readGrid(gridTrim);
                }
            }

            // fallback: reconstruir do Stage 9
            if (matrix == null) {
                int[] series = sd.series.get(cycle);
                if (series == null) {
                    // Se o Stage 12 deu um ciclo que não existe no Stage 9, pula (ou lança?)
                    // Vamos lançar para sinalizar problema real de pipeline:
                    throw new IOException("Ciclo '" + cycle + "' não encontrado no Stage 9 para reconstruir grade.");
                }
                matrix = reshapeSeriesToGrid(series, period, true); // trim remainder se necessário
            }

            // inferir campos
            List<Field> fields = inferFields(matrix);
            res.perCycleFields.put(cycle, fields);
        }

        // 4) Export CSV/TXT
        Path layoutCsv = outDir.resolve("stage13_layout.csv");
        try (BufferedWriter w = Files.newBufferedWriter(layoutCsv)) {
            w.write("cycle,startCol,widthCols,kind,notes\n");
            for (String cycle : allCycles) {
                for (Field f : res.perCycleFields.get(cycle)) {
                    w.write(String.format(Locale.ROOT, "%s,%d,%d,%s,%s%n",
                            cycle, f.startCol, f.widthCols, f.kind, sanitize(f.notes)));
                }
            }
        }
        res.layoutCsv = layoutCsv;

        Path layoutTxt = outDir.resolve("stage13_layout.txt");
        try (BufferedWriter w = Files.newBufferedWriter(layoutTxt)) {
            w.write("Stage 13 - FieldInfer\n");
            w.write("bestPeriod=" + period + "\n");
            w.write("cycles=" + allCycles + "\n\n");
            for (String cycle : allCycles) {
                w.write("[" + cycle + "]\n");
                for (Field f : res.perCycleFields.get(cycle)) {
                    w.write("  - " + f + "\n");
                }
                w.write("\n");
            }
        }
        res.layoutTxt = layoutTxt;

        System.out.println("[Stage 13] " + res.layoutTxt);
        return res;
    }

    // ===================== Fallbacks de período =====================

    private static int readBestPeriod(Path bestTxt) throws IOException {
        List<String> lines = Files.readAllLines(bestTxt);
        Pattern P = Pattern.compile("\\bperiod\\s*=\\s*(\\d+)");
        for (String ln : lines) {
            Matcher m = P.matcher(ln);
            if (m.find()) {
                try { return Integer.parseInt(m.group(1)); } catch (Exception ignore) {}
            }
        }
        return -1;
    }

    /** Procura arquivos stage12_grid_*_period_XXX(.csv|_trim.csv) e retorna o XXX mais frequente. */
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

    /** Procura stage12_colstats_period_XXX.csv e retorna o XXX mais frequente. */
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

    /** Último recurso: usa o tamanho da série do Stage 9 para sugerir divisores >1 (preferindo 78, 39, 117, 26, 13). */
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
            if (c > bestC || (c == bestC && p > bestP)) {
                bestC = c; bestP = p;
            }
        }
        return (bestP > 1) ? bestP : null;
    }

    // ===================== Carregar série do Stage 9 =====================

    private static final class SeriesData {
        int length;
        List<String> cycles = new ArrayList<>();
        Map<String, int[]> series = new LinkedHashMap<>();
    }

    /** Lê série(s) do Stage 9: suporta (A) idx,value → "series" e (C) cycle,idx,value → várias séries. */
    private static SeriesData readSeriesFlexible(Path seriesCsv, Path concatCsv) throws IOException {
        if (Files.exists(seriesCsv)) {
            return readSeriesSingleColumn(seriesCsv);
        }
        if (Files.exists(concatCsv)) {
            return readConcat(concatCsv);
        }
        // fallback vazio
        SeriesData sd = new SeriesData();
        sd.length = 0;
        return sd;
    }

    /** (A) idx,value -> ciclo único "series" */
    private static SeriesData readSeriesSingleColumn(Path seriesCsv) throws IOException {
        List<String> lines = Files.readAllLines(seriesCsv);
        SeriesData sd = new SeriesData();
        if (lines.isEmpty()) { sd.length = 0; return sd; }
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

    /** (C) cycle,idx,value -> várias séries por nome de ciclo */
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

    private static int[][] reshapeSeriesToGrid(int[] data, int period, boolean trimRemainder) {
        if (data == null) return new int[0][0];
        int rows = data.length / period;
        int rem = data.length % period;
        if (rows == 0) return new int[0][0];
        int usable = rows * period;
        int[][] M = new int[rows][period];
        for (int r = 0; r < rows; r++) {
            System.arraycopy(data, r * period, M[r], 0, period);
        }
        // Nota: se rem > 0, simplesmente ignoramos o “tail”
        return M;
    }

    // ===================== Inferência de campos =====================

    private static List<Field> inferFields(int[][] M) {
        int rows = M.length;
        int cols = (rows == 0 ? 0 : M[0].length);

        boolean[] isConst = new boolean[cols];
        boolean[] isBin01 = new boolean[cols];
        boolean[] isBin0255 = new boolean[cols];
        boolean[] isMono = new boolean[cols];
        double[] variance = new double[cols];

        for (int c = 0; c < cols; c++) {
            List<Integer> col = new ArrayList<>(rows);
            for (int r = 0; r < rows; r++) col.add(M[r][c]);
            isConst[c] = isConstant(col);
            isBin01[c] = isBinary(col, 0, 1);
            isBin0255[c] = isBinary(col, 0, 255);
            isMono[c] = isMonotonicNonDecreasing(col);
            variance[c] = variance(col);
        }

        List<Field> fields = new ArrayList<>();
        boolean[] used = new boolean[cols];
        for (int c = 0; c < cols; c++) {
            if (used[c]) continue;

            if (isConst[c]) {
                fields.add(f(c, 1, "CONST", "constant=" + constantValue(M, c)));
                used[c] = true; continue;
            }
            if (isBin01[c] || isBin0255[c]) {
                fields.add(f(c, 1, "FLAG", isBin01[c] ? "binary{0,1}" : "binary{0,255}"));
                used[c] = true; continue;
            }
            if (isMono[c] && counterLike(M, c)) {
                fields.add(f(c, 1, "COUNTER", "monotonic"));
                used[c] = true; continue;
            }

            // tentar U16 (LE/BE) com vizinho
            if (c + 1 < cols && !used[c + 1]) {
                String two = tryPairField(M, c, c + 1);
                if (two != null) {
                    fields.add(f(c, 2, two, "adjacent"));
                    used[c] = used[c + 1] = true;
                    continue;
                }
            }

            fields.add(f(c, 1, "UNKNOWN", "var=" + String.format(Locale.ROOT, "%.3f", variance[c])));
            used[c] = true;
        }

        fields.sort(Comparator.comparingInt(a -> a.startCol));
        return fields;
    }

    private static Field f(int start, int width, String kind, String notes) {
        Field F = new Field();
        F.startCol = start; F.widthCols = width; F.kind = kind; F.notes = notes;
        return F;
    }

    private static boolean isConstant(List<Integer> col) {
        if (col.isEmpty()) return true;
        int v = col.get(0);
        for (int x : col) if (x != v) return false;
        return true;
    }
    private static boolean isBinary(List<Integer> col, int a, int b) {
        for (int x : col) if (x != a && x != b) return false;
        return true;
    }
    private static boolean isMonotonicNonDecreasing(List<Integer> col) {
        for (int i = 1; i < col.size(); i++) if (col.get(i) < col.get(i - 1)) return false;
        return true;
    }
    private static boolean counterLike(int[][] M, int c) {
        List<Integer> col = new ArrayList<>(M.length);
        for (int[] row : M) col.add(row[c]);
        if (col.size() < 3) return false;
        List<Integer> d = new ArrayList<>(col.size() - 1);
        for (int i = 1; i < col.size(); i++) d.add(col.get(i) - col.get(i - 1));
        double v = variance(d);
        return v < 5.0;
    }
    private static double variance(List<Integer> xs) {
        if (xs.isEmpty()) return 0.0;
        double mean = xs.stream().mapToDouble(i -> i).average().orElse(0.0);
        double var = 0.0;
        for (int v : xs) { double d = v - mean; var += d * d; }
        return var / xs.size();
    }
    private static Integer constantValue(int[][] M, int c) {
        return M.length == 0 ? null : M[0][c];
    }

    private static String tryPairField(int[][] M, int c0, int c1) {
        PairScore le = scoreU16(M, c0, c1, true);
        PairScore be = scoreU16(M, c0, c1, false);
        if (le.betterThan(be) && le.good()) return "U16_LE";
        if (be.betterThan(le) && be.good()) return "U16_BE";
        return null;
    }
    private static final class PairScore {
        double deltaVar; int distinct;
        boolean betterThan(PairScore o) {
            int cmp = Double.compare(this.deltaVar, o.deltaVar);
            if (cmp != 0) return cmp < 0;
            return Integer.compare(this.distinct, o.distinct) < 0;
        }
        boolean good() { return deltaVar < 50.0; }
    }
    private static PairScore scoreU16(int[][] M, int loCol, int hiCol, boolean littleEndian) {
        List<Integer> vals = new ArrayList<>(M.length);
        for (int[] row : M) {
            int lo = row[loCol] & 0xFF;
            int hi = row[hiCol] & 0xFF;
            int v = littleEndian ? (lo | (hi << 8)) : (hi | (lo << 8));
            vals.add(v);
        }
        PairScore ps = new PairScore();
        Set<Integer> distinct = new HashSet<>(vals);
        ps.distinct = distinct.size();
        List<Integer> deltas = new ArrayList<>(Math.max(0, vals.size() - 1));
        for (int i = 1; i < vals.size(); i++) deltas.add(vals.get(i) - vals.get(i - 1));
        ps.deltaVar = variance(deltas);
        return ps;
    }

    // ===================== IO e descoberta de ciclos/grids =====================

    private static String sanitize(String s) {
        if (s == null) return "";
        return s.replace('\n', ' ').replace('\r', ' ').replace(',', ';');
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

    private static List<String> findCycles(Path stage12Dir, int period) throws IOException {
        List<String> out = new ArrayList<>();
        String p3 = String.format("%03d", period);
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(
                stage12Dir, "stage12_grid_*_period_" + p3 + "*.csv")) {
            for (Path p : ds) {
                String fn = p.getFileName().toString();
                int a = fn.indexOf("stage12_grid_");
                int b = fn.indexOf("_period_");
                if (a >= 0 && b > a) {
                    String mid = fn.substring(a + "stage12_grid_".length(), b);
                    if (!out.contains(mid)) out.add(mid);
                }
            }
        }
        return out;
    }

    private static int parseIntSafe(String s) {
        try { return Integer.parseInt(s); } catch (Exception ignore) {}
        String t = s.toLowerCase(Locale.ROOT);
        if (t.startsWith("0x")) { try { return Integer.parseInt(t.substring(2), 16); } catch (Exception ignore2) {} }
        else { try { return Integer.parseInt(t, 16); } catch (Exception ignore3) {} }
        return 0;
    }
}
