package com.cmpio;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Stage 11 - Inspeção de sinal (autocorrelação/DFT/histogramas).
 *
 * Entradas esperadas:
 *  - stage9_series.csv (preferido) ou stage9_concat.csv no diretório do Stage 9.
 *
 * Saídas:
 *  - stage11_summary.txt
 *  - stage11_autocorr.csv
 *  - stage11_fft.csv
 */
public final class Stage11SignalInspector {

    public static final class Result {
        public final Path outDir;
        public final Path summaryTxt;
        public final Path autocorrCsv;
        public final Path fftCsv;

        public final int seriesLen;
        public final List<Integer> topLags;     // lags de maior autocorrelação (exclui 0)
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

    /** NOVA assinatura preferida: você informa onde está o Stage 9 e onde quer as saídas. */
    public static Result run(Path stage9Dir, Path outDir) throws IOException {
        if (stage9Dir == null) throw new IOException("Stage 11: stage9Dir == null");
        if (!Files.isDirectory(stage9Dir)) {
            throw new IOException("Stage 11: diretório do Stage 9 não existe: " + stage9Dir);
        }
        if (outDir == null) {
            // default ao lado do stage9Dir
            outDir = stage9Dir.getParent() == null
                    ? Paths.get("cmp_stage11_out")
                    : stage9Dir.getParent().resolve("cmp_stage11_out");
        }
        Files.createDirectories(outDir);

        // Escolhe a série
        Path seriesCsv = stage9Dir.resolve("stage9_series.csv");
        if (!Files.exists(seriesCsv)) {
            seriesCsv = stage9Dir.resolve("stage9_concat.csv");
        }
        if (!Files.exists(seriesCsv)) {
            throw new IOException("Stage 11: não encontrei stage9_series.csv nem stage9_concat.csv em " + stage9Dir);
        }

        double[] series = loadSeries(seriesCsv);
        int N = series.length;
        if (N == 0) {
            // Ainda assim escrevemos um summary mínimo para não quebrar o pipeline
            Path summary = outDir.resolve("stage11_summary.txt");
            Files.writeString(summary, "=== Stage 11 Summary ===\nSeries length: 0\n");
            Path acCsv = outDir.resolve("stage11_autocorr.csv");
            Files.writeString(acCsv, "lag,autocorr\n");
            Path fftCsv = outDir.resolve("stage11_fft.csv");
            Files.writeString(fftCsv, "k,freq_norm,magnitude\n");
            System.out.println("[Stage 11] Série vazia. Artefatos mínimos gerados em " + outDir);
            return new Result(outDir, summary, acCsv, fftCsv, 0, Collections.emptyList(), Collections.emptyList());
        }

        Map<Double, Integer> hist = histogram(series, 64);
        int maxLag = Math.max(4, Math.min(256, N / 2));
        double[] ac = autocorr(series, maxLag);

        double[] mag = dftMagnitude(series);
        int half = mag.length / 2;

        List<Integer> topLags = topKIndices(ac, 5, 1);
        List<Integer> topBins = topKIndices(Arrays.copyOfRange(mag, 1, half), 5, 0)
                .stream().map(i -> i + 1).collect(Collectors.toList());

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

        Path peaksCsv = outDir.resolve("stage11_peaks.csv");
        try (BufferedWriter w = Files.newBufferedWriter(peaksCsv, StandardCharsets.UTF_8)) {
            w.write("type,index,value\n");
            for (int lag : topLags) w.write("autocorr," + lag + "," + String.format(Locale.ROOT,"%.9f", (lag < ac.length ? ac[lag] : Double.NaN)) + "\n");
            for (int k : topBins)  w.write("fft," + k + "," + String.format(Locale.ROOT,"%.9f", (k < mag.length ? mag[k] : Double.NaN)) + "\n");
        }

        System.out.println("[Stage 11] " + outDir);
        return new Result(outDir, summary, acCsv, fftCsv, N, topLags, topBins);
    }

    /** Assinatura compatível: se baseDir == cmp_stage9_out usa direto; senão tenta baseDir/cmp_stage9_out. */
    public static Result run(Path baseDir) throws IOException {
        if (baseDir == null) throw new IOException("Stage 11: baseDir == null");
        final String name = baseDir.getFileName() != null ? baseDir.getFileName().toString() : "";
        Path stage9Dir = name.equalsIgnoreCase("cmp_stage9_out")
                ? baseDir
                : baseDir.resolve("cmp_stage9_out");
        Path outDir = baseDir.resolve("cmp_stage11_out");
        return run(stage9Dir, outDir);
    }

    // ---------- Utilidades ----------

    /** Lê stage9_series.csv ou stage9_concat.csv (coluna 'value'). */
    private static double[] loadSeries(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        if (lines.isEmpty()) return new double[0];

        String header = lines.get(0);
        int valueIdx = findHeaderIndex(header.split(","), "value");
        int start = header.toLowerCase(Locale.ROOT).contains("value") ? 1 : 0;
        if (start == 0) valueIdx = -1;

        List<Double> vals = new ArrayList<>();
        for (int i = start; i < lines.size(); i++) {
            String ln = lines.get(i).trim();
            if (ln.isEmpty()) continue;
            String[] cols = ln.split(",");
            String tok = (valueIdx >= 0 && valueIdx < cols.length) ? cols[valueIdx] : cols[cols.length - 1];
            if (!tok.isEmpty() && tok.charAt(0) == '\uFEFF') tok = tok.substring(1);
            tok = tok.trim();
            try {
                vals.add(Double.parseDouble(tok));
            } catch (NumberFormatException nfe) {
                try { vals.add((double) Integer.parseInt(tok)); } catch (NumberFormatException ignore) {}
            }
        }
        double[] arr = new double[vals.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = vals.get(i);
        return arr;
    }

    private static int findHeaderIndex(String[] headers, String wantedName) {
        String w = wantedName.toLowerCase(Locale.ROOT);
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().toLowerCase(Locale.ROOT);
            if (h.equals(w)) return i;
        }
        return -1;
    }

    private static Map<Double, Integer> histogram(double[] x, int bins) {
        Map<Double, Integer> out = new LinkedHashMap<>();
        if (x.length == 0) return out;
        double min = Arrays.stream(x).min().orElse(0);
        double max = Arrays.stream(x).max().orElse(0);
        if (max <= min) { out.put(min, x.length); return out; }
        double step = (max - min) / bins;
        for (double v : x) {
            int b = (int) Math.floor((v - min) / step);
            if (b >= bins) b = bins - 1;
            double center = min + (b + 0.5) * step;
            out.put(center, out.getOrDefault(center, 0) + 1);
        }
        return out;
    }

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

    private static double[] dftMagnitude(double[] x) {
        int N = x.length;
        double[] mag = new double[N];
        double twoPiOverN = 2 * Math.PI / N;
        for (int k = 0; k < N; k++) {
            double re = 0.0, im = 0.0;
            for (int n = 0; n < N; n++) {
                double ang = twoPiOverN * k * n;
                re += x[n] * Math.cos(ang);
                im -= x[n] * Math.sin(ang);
            }
            mag[k] = Math.hypot(re, im);
        }
        return mag;
    }

    private static List<Integer> topKIndices(double[] arr, int K, int skipFirst) {
        List<Integer> idx = new ArrayList<>();
        for (int i = skipFirst; i < arr.length; i++) idx.add(i);
        idx.sort((a, b) -> Double.compare(arr[b], arr[a]));
        return (idx.size() > K) ? idx.subList(0, K) : idx;
    }
}
