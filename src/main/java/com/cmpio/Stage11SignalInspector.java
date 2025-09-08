package com.cmpio;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Stage 11 - Inspeção de sinal (autocorrelação/DFT/histogramas) para apoiar a decodificação final.
 *
 * Entradas esperadas (geradas pelos Stages 9 e 10):
 *  - stage9_series.csv       (uma série por ciclo; cabeçalho: cycle_id,value_idx,value)
 *  - stage10_mapped.csv      (opcional; mapeamento discreto/normalizado por linha)
 *
 * Saídas (em cmp_stage11_out):
 *  - stage11_summary.txt
 *  - stage11_autocorr.csv    (colunas: lag, autocorr)
 *  - stage11_fft.csv         (colunas: k, freq_norm, magnitude)
 */
public final class Stage11SignalInspector {

    public static final class Result {
        public final Path outDir;
        public final Path summaryTxt;
        public final Path autocorrCsv;
        public final Path fftCsv;

        public final int seriesLen;
        public final List<Integer> topLags;     // lags de maior autocorrelação (excluindo 0)
        public final List<Integer> topFreqBins; // bins de maior magnitude no DFT

        Result(Path outDir, Path summaryTxt, Path autocorrCsv, Path fftCsv,
               int seriesLen, List<Integer> topLags, List<Integer> topFreqBins) {
            this.outDir = outDir;
            this.summaryTxt = summaryTxt;
            this.autocorrCsv = autocorrCsv;
            this.fftCsv = fftCsv;
            this.seriesLen = seriesLen;
            this.topLags = topLags;
            this.topFreqBins = topFreqBins;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT,
                    "Stage11.Result{seriesLen=%d, outDir=%s, summary=%s, autocorr=%s, fft=%s, topLags=%s, topFreqBins=%s}",
                    seriesLen, outDir, summaryTxt, autocorrCsv, fftCsv, topLags, topFreqBins);
        }
    }

    private Stage11SignalInspector() {}

    /** Executa o Stage 11. */
    public static Result run(Path baseDir) throws IOException {
        Path outDir = (baseDir.getParent() == null)
                ? Paths.get("cmp_stage11_out")
                : baseDir.getParent().resolve("cmp_stage11_out");
        Files.createDirectories(outDir);

        // Preferimos a série consolidada do Stage 9:
        Path seriesCsv = outDir.getParent().resolve("cmp_stage9_out").resolve("stage9_series.csv");
        if (!Files.exists(seriesCsv)) {
            // fallback: concatenado
            seriesCsv = outDir.getParent().resolve("cmp_stage9_out").resolve("stage9_concat.csv");
        }
        if (!Files.exists(seriesCsv)) {
            throw new IOException("Stage 11: não encontrei stage9_series.csv nem stage9_concat.csv.");
        }

        double[] series = loadSeries(seriesCsv);
        int N = series.length;

        // Histogramas simples (valor -> contagem)
        Map<Double, Integer> hist = histogram(series, 64);

        // Autocorrelação (até lag máx razoável: N/2)
        int maxLag = Math.max(4, Math.min(256, N / 2));
        double[] ac = autocorr(series, maxLag);

        // DFT simples (magnitude) – bins 0..N-1, mas reportamos só 0..N/2
        double[] mag = dftMagnitude(series);
        int half = mag.length / 2;

        // Escolher top lags e top bins
        List<Integer> topLags = topKIndices(ac, 5, 1); // ignora lag 0
        List<Integer> topBins = topKIndices(Arrays.copyOfRange(mag, 1, half), 5, 0)
                .stream().map(i -> i + 1).collect(Collectors.toList());

        // Gravar arquivos
        Path summary = outDir.resolve("stage11_summary.txt");
        Path acCsv   = outDir.resolve("stage11_autocorr.csv");
        Path fftCsv  = outDir.resolve("stage11_fft.csv");

        try (BufferedWriter w = Files.newBufferedWriter(summary, StandardCharsets.UTF_8)) {
            w.write("=== Stage 11 Summary ===\n");
            w.write("Series length: " + N + "\n");
            w.write("Histogram (approx 64 bins):\n");
            for (Map.Entry<Double,Integer> e : hist.entrySet()) {
                w.write(String.format(Locale.ROOT, "  binCenter=%.6f, count=%d%n", e.getKey(), e.getValue()));
            }
            w.write("Top autocorrelation lags (excluindo lag=0): " + topLags + "\n");
            w.write("Top FFT bins (1..N/2): " + topBins + "\n");
            if (!topLags.isEmpty()) {
                w.write("Sugestão de período candidato (lag dominante): " + topLags.get(0) + "\n");
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(acCsv, StandardCharsets.UTF_8)) {
            w.write("lag,autocorr\n");
            for (int lag = 0; lag < ac.length; lag++) {
                w.write(lag + "," + String.format(Locale.ROOT, "%.9f", ac[lag]) + "\n");
            }
        }

        try (BufferedWriter w = Files.newBufferedWriter(fftCsv, StandardCharsets.UTF_8)) {
            w.write("k,freq_norm,magnitude\n");
            for (int k = 0; k <= half; k++) {
                double f = (double) k / N;
                w.write(k + "," + String.format(Locale.ROOT, "%.9f", f) + "," +
                        String.format(Locale.ROOT, "%.9f", mag[k]) + "\n");
            }
        }

        System.out.println("[Stage 11] " + outDir);
        return new Result(outDir, summary, acCsv, fftCsv, N, topLags, topBins);
    }

    // ---------- Utilidades ----------

    /** Lê stage9_series.csv ou stage9_concat.csv (coluna 'value'). */
    private static double[] loadSeries(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        if (lines.isEmpty()) return new double[0];

        // Detecta header e índice da coluna "value"
        String header = lines.get(0);
        int valueIdx = findFirstInt(header.split(","), "value");
        int start = 0;
        if (header.toLowerCase(Locale.ROOT).contains("value")) {
            start = 1; // pula header
        } else {
            valueIdx = -1; // fallback: última coluna
        }

        List<Double> vals = new ArrayList<>();
        for (int i = start; i < lines.size(); i++) {
            String ln = lines.get(i).trim();
            if (ln.isEmpty()) continue;
            String[] cols = ln.split(",");
            String tok = (valueIdx >= 0 && valueIdx < cols.length) ? cols[valueIdx] : cols[cols.length - 1];
            try {
                vals.add(Double.parseDouble(tok));
            } catch (NumberFormatException nfe) {
                // tenta int
                try {
                    vals.add((double) Integer.parseInt(tok));
                } catch (NumberFormatException nfe2) {
                    // ignora linhas inválidas
                }
            }
        }
        double[] arr = new double[vals.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = vals.get(i);
        return arr;
    }

    /** Procura pela coluna com nome exato (case-insensitive) e retorna seu índice, ou -1. */
    private static int findFirstInt(String[] headers, String wantedName) {
        String w = wantedName.toLowerCase(Locale.ROOT);
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().toLowerCase(Locale.ROOT);
            if (h.equals(w)) return i;
        }
        return -1;
    }

    /** Histograma aproximado com 'bins' caixas, retorna centro da caixa -> contagem. */
    private static Map<Double, Integer> histogram(double[] x, int bins) {
        Map<Double, Integer> out = new LinkedHashMap<>();
        if (x.length == 0) return out;
        double min = Arrays.stream(x).min().orElse(0);
        double max = Arrays.stream(x).max().orElse(0);
        if (max <= min) {
            out.put(min, x.length);
            return out;
        }
        double step = (max - min) / bins;
        for (double v : x) {
            int b = (int) Math.floor((v - min) / step);
            if (b >= bins) b = bins - 1;
            double center = min + (b + 0.5) * step;
            out.put(center, out.getOrDefault(center, 0) + 1);
        }
        return out;
    }

    /** Autocorrelação normalizada por variância, lags 0..maxLag. */
    private static double[] autocorr(double[] x, int maxLag) {
        int N = x.length;
        double mean = Arrays.stream(x).average().orElse(0.0);
        double var = 0.0;
        for (double v : x) { double d = v - mean; var += d * d; }
        if (var == 0) var = 1e-12;
        double[] out = new double[Math.max(1, Math.min(maxLag, N - 1)) + 1];
        for (int lag = 0; lag < out.length; lag++) {
            double s = 0.0;
            for (int i = 0; i + lag < N; i++) {
                s += (x[i] - mean) * (x[i + lag] - mean);
            }
            out[lag] = s / var;
        }
        return out;
    }

    /** DFT (magnitude) O(N^2). Para N=234 é aceitável. */
    private static double[] dftMagnitude(double[] x) {
        int N = x.length;
        double[] mag = new double[N];
        double twoPiOverN = 2 * Math.PI / N;
        for (int k = 0; k < N; k++) {
            double re = 0.0;
            double im = 0.0;
            for (int n = 0; n < N; n++) {
                double ang = twoPiOverN * k * n;
                re += x[n] * Math.cos(ang);
                im -= x[n] * Math.sin(ang);
            }
            mag[k] = Math.hypot(re, im);
        }
        return mag;
    }

    /** Retorna índices dos K maiores valores (desc), com offset aplicado (p.ex., pular lag 0). */
    private static List<Integer> topKIndices(double[] arr, int K, int skipFirst) {
        List<Integer> idx = new ArrayList<>();
        for (int i = skipFirst; i < arr.length; i++) idx.add(i);
        idx.sort((a, b) -> Double.compare(arr[b], arr[a]));
        if (idx.size() > K) return idx.subList(0, K);
        return idx;
    }
}
