package com.cmpio;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Driver do Stage 6:
 * - lê os CSVs do Stage 5
 * - reconstrói proto-traces por ciclo
 * - exporta CSVs de inspeção por ciclo
 *
 * Uso:
 *   java com.cmpio.Stage6Driver D:\tmp\cmp_dir\cmp_stage5_out
 */
public final class Stage6Driver {

    public static void main(String[] args) throws Exception {
        Path outDir;
        if (args.length >= 1) {
            outDir = Paths.get(args[0]);
        } else {
            // fallback: mesmo diretório padrão do seu log anterior
            outDir = Paths.get("D:\\tmp\\cmp_dir\\cmp_stage5_out");
        }

        System.out.println("=== Stage 6 ===");
        System.out.println("Lendo artefatos do Stage 5 em: " + outDir);

        List<Stage6Parsers.CycleData> cycles = Stage6Parsers.loadCycles(outDir);
        if (cycles.isEmpty()) {
            System.out.println("Nenhum cycle_*_tokens.csv encontrado. Nada a fazer.");
            return;
        }

        for (Stage6Parsers.CycleData c : cycles) {
            Stage6Parsers.ReconstructedTrace rt = Stage6Parsers.reconstruct(c);
            Stage6Parsers.exportTraceCsv(rt, c.tokens, outDir);

            // Sumário no console
            System.out.printf(Locale.ROOT,
                    "Cycle %02d: tokens=%d, segments=%d, totalSamples=%d, markers=%d -> stage6_recon_cycle_%02d.csv%n",
                    c.index, c.tokens.size(), rt.segments.size(), rt.totalSamples, rt.totalMarkers, c.index);
        }
        System.out.println("Stage 6: concluído.");
    }

    private Stage6Driver() {}
}
