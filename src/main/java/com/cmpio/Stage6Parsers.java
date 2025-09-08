package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilitários do Stage 6 que operam SOMENTE sobre os artefatos do Stage 5:
 * - cycle_XX_tokens.csv
 * - cycle_XX_rle.csv
 *
 * Parser robusto: ignora cabeçalhos e colunas não numéricas; extrai inteiros por regex.
 */
public final class Stage6Parsers {

    /** Representa um ciclo exportado pelo Stage 5 (tokens e RLE já persistidos). */
    public static final class CycleData {
        public final int index;                 // número do ciclo (00, 01, 02, ...)
        public final List<Integer> tokens;      // sequência de tokens do ciclo (na ordem)
        public final List<RleItem> rle;         // RLE por símbolo (como exportado)

        public CycleData(int index, List<Integer> tokens, List<RleItem> rle) {
            this.index = index;
            this.tokens = tokens;
            this.rle = rle;
        }
    }

    /** Linha típica do RLE exportado pelo Stage 5 (símbolo + tamanho da run). */
    public static final class RleItem {
        public final int symbol;
        public final int runLength;

        public RleItem(int symbol, int runLength) {
            this.symbol = symbol;
            this.runLength = runLength;
        }
    }

    /** “Proto-trace” montada por heurística: blocos de dados contínuos de 91 com marcadores nas quebras. */
    public static final class ReconstructedTrace {
        public final int cycleIndex;
        public final List<Segment> segments = new ArrayList<>();
        public int totalSamples;               // número total de amostras aproximado (heurístico)
        public int totalMarkers;               // contagem de marcadores (195/249/63/210/61)

        public ReconstructedTrace(int cycleIndex) {
            this.cycleIndex = cycleIndex;
        }

        public static final class Segment {
            public final int startTokenIdx;    // índice no vetor de tokens
            public final int length;           // quantos tokens pertencem ao “bloco”
            public final String kind;          // "PAD/RLE", "CTRL", "DATA?"
            public Segment(int startTokenIdx, int length, String kind) {
                this.startTokenIdx = startTokenIdx;
                this.length = length;
                this.kind = kind;
            }
        }
    }

    /** Lê todos os cycles_*.csv existentes em outDir e devolve a lista. */
    public static List<CycleData> loadCycles(Path outDir) throws IOException {
        if (!Files.isDirectory(outDir)) {
            throw new IOException("Diretório não encontrado: " + outDir);
        }
        // procura ambos os padrões
        List<Path> tokenFiles = glob(outDir, "cycle_*_tokens.csv");
        tokenFiles.sort(Comparator.naturalOrder());

        List<CycleData> cycles = new ArrayList<>();
        for (Path tokFile : tokenFiles) {
            int idx = extractIndex(tokFile.getFileName().toString());
            Path rleFile = outDir.resolve(String.format("cycle_%02d_rle.csv", idx));

            List<Integer> tokens = readTokensCsv(tokFile);
            List<RleItem> rle = Files.exists(rleFile) ? readRleCsv(rleFile) : Collections.emptyList();

            cycles.add(new CycleData(idx, tokens, rle));
        }
        return cycles;
    }

    /** Reconstrói uma “proto-trace” a partir dos tokens de um ciclo (heurística não destrutiva). */
    public static ReconstructedTrace reconstruct(CycleData cycle) {
        ReconstructedTrace rt = new ReconstructedTrace(cycle.index);
        final List<Integer> t = cycle.tokens;

        // Heurística mínima: 91 domina = “samples”; 195/249/63/210/61 = marcadores/controle/headers.
        final Set<Integer> ctrl = new HashSet<>(Arrays.asList(195, 249, 63, 210, 61));

        int i = 0;
        while (i < t.size()) {
            int v = t.get(i);
            if (v == 91) {
                int j = i + 1;
                while (j < t.size() && t.get(j) == 91) j++;
                int len = j - i;
                rt.segments.add(new ReconstructedTrace.Segment(i, len, "PAD/RLE"));
                rt.totalSamples += len;
                i = j;
            } else {
                // marcador/controle curto
                String kind = ctrl.contains(v) ? "CTRL" : "DATA?";
                int j = i + 1;
                while (j < t.size() && t.get(j) != 91 && j - i < 8) j++;
                for (int k = i; k < j; k++) if (ctrl.contains(t.get(k))) rt.totalMarkers++;
                rt.segments.add(new ReconstructedTrace.Segment(i, j - i, kind));
                i = j;
            }
        }
        return rt;
    }

    /** Exporta a proto-trace para CSV para inspeção/QA. */
    public static Path exportTraceCsv(ReconstructedTrace rt, List<Integer> tokens, Path outDir) throws IOException {
        Files.createDirectories(outDir);
        Path out = outDir.resolve(String.format("stage6_recon_cycle_%02d.csv", rt.cycleIndex));
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write("cycle,seg_idx,start_token_idx,length,kind,preview\n");
            for (int idx = 0; idx < rt.segments.size(); idx++) {
                ReconstructedTrace.Segment s = rt.segments.get(idx);
                String preview = preview(tokens, s.startTokenIdx, Math.min(8, s.length));
                w.write(String.format(Locale.ROOT, "%d,%d,%d,%d,%s,%s%n",
                        rt.cycleIndex, idx, s.startTokenIdx, s.length, s.kind, preview));
            }
        }
        return out;
    }

    // ---------------- auxiliares ----------------

    private static List<Path> glob(Path dir, String pattern) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, pattern)) {
            List<Path> list = new ArrayList<>();
            for (Path p : ds) list.add(p);
            return list;
        }
    }

    private static int extractIndex(String filename) {
        // cycle_XX_tokens.csv  -> XX
        // cycle_XX_rle.csv     -> XX
        int us = filename.indexOf('_');
        int us2 = filename.indexOf('_', us + 1);
        if (us < 0 || us2 < 0) return 0;
        String mid = filename.substring(us + 1, us2);
        try { return Integer.parseInt(mid); } catch (Exception e) { return 0; }
    }

    /** Regex para obter inteiros (aceita sinais). */
    private static final Pattern INT_PATTERN = Pattern.compile("[-+]?\\d+");

    private static List<Integer> readTokensCsv(Path file) throws IOException {
        List<Integer> out = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String header = r.readLine(); // descarta cabeçalho
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Captura o ÚLTIMO inteiro da linha (funciona para "idx,value" ou "value" ou linhas com texto)
                Matcher m = INT_PATTERN.matcher(line);
                int last = Integer.MIN_VALUE;
                while (m.find()) {
                    last = Integer.parseInt(m.group());
                }
                if (last != Integer.MIN_VALUE) {
                    out.add(last);
                } // senão ignora a linha
            }
        }
        return out;
    }

    private static List<RleItem> readRleCsv(Path file) throws IOException {
        List<RleItem> out = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String header = r.readLine(); // descarta cabeçalho
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Extraia OS DOIS PRIMEIROS inteiros da linha (símbolo, runLength)
                Matcher m = INT_PATTERN.matcher(line);
                Integer first = null, second = null;
                while (m.find()) {
                    int v = Integer.parseInt(m.group());
                    if (first == null) {
                        first = v;
                    } else {
                        second = v;
                        break;
                    }
                }
                if (first != null && second != null) {
                    out.add(new RleItem(first, second));
                }
                // Caso contrário, a linha não possui 2 inteiros -> ignorar silenciosamente
            }
        }
        return out;
    }

    private static String preview(List<Integer> tokens, int start, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < Math.min(tokens.size(), start + n); i++) {
            if (i > start) sb.append(' ');
            sb.append(tokens.get(i));
        }
        return "\"" + sb + "\"";
    }

    private Stage6Parsers() {}
}
