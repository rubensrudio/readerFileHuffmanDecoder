package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Stage 16 – Export final
 *
 * Lê os artefatos do Stage 15 (preferencialmente stage15_records_series.csv,
 * caindo para stage15_records_all.csv) e gera um CSV final consolidado:
 *
 *   cmp_stage16_out/stage16_final.csv
 *
 * Objetivo: entregar um "flat" simples e direto para consumo externo.
 * Não depende de libs externas (Parquet, etc.).
 */
public final class Stage16Exporter {

    private Stage16Exporter() {}

    public static final class Result {
        public final Path outDir;
        public final Path finalCsv;
        public final int rows;
        public final int cols;
        public final List<String> headers;

        public Result(Path outDir, Path finalCsv, int rows, int cols, List<String> headers) {
            this.outDir = outDir;
            this.finalCsv = finalCsv;
            this.rows = rows;
            this.cols = cols;
            this.headers = headers;
        }

        @Override
        public String toString() {
            return "Stage16.Result{rows=" + rows + ", cols=" + cols +
                    ", outDir=" + outDir + ", finalCsv=" + finalCsv + "}";
        }
    }

    /**
     * Executa o Stage 16.
     * @param baseDir diretório-base do CMP (pai das pastas cmp_stageXX_out)
     */
    public static Result run(Path baseDir) throws IOException {
        Path s15 = baseDir.resolve("cmp_stage15_out");
        if (!Files.isDirectory(s15)) {
            throw new FileNotFoundException("Stage 15 dir não encontrado: " + s15);
        }

        Path src = s15.resolve("stage15_records_series.csv");
        if (!Files.exists(src)) {
            src = s15.resolve("stage15_records_all.csv");
        }
        if (!Files.exists(src)) {
            throw new FileNotFoundException("Stage 15 CSV não encontrado (nem series nem all): " + s15);
        }

        Path outDir = baseDir.resolve("cmp_stage16_out");
        Files.createDirectories(outDir);
        Path outCsv = outDir.resolve("stage16_final.csv");

        List<String> lines = Files.readAllLines(src, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            // cria um CSV vazio porém válido
            try (BufferedWriter w = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8)) {
                w.write("idx\n");
            }
            Result r = new Result(outDir, outCsv, 0, 1, Collections.singletonList("idx"));
            System.out.println("[Stage 16] " + r);
            return r;
        }

        // Descobrir header/cols
        String header = lines.get(0);
        String[] hs = header.split(",", -1);
        int cols = hs.length;
        List<String> headers = new ArrayList<>();
        for (String h : hs) headers.add(h.trim());

        // Apenas copiamos (podemos, no futuro, reordenar/renomear se necessário)
        try (BufferedWriter w = Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8)) {
            for (String ln : lines) {
                w.write(ln);
                w.write('\n');
            }
        }

        int rows = Math.max(0, lines.size() - 1);
        Result r = new Result(outDir, outCsv, rows, cols, headers);
        System.out.println("[Stage 16] " + r);
        return r;
    }
}
