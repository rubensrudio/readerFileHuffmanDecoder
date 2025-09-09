package com.cmpio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stage 17 – Manifest
 *
 * Consolida metadados e caminhos dos artefatos em um JSON simples:
 *   cmp_stage17_out/manifest.json
 *
 * Coleta:
 *  - período (Stage 12)
 *  - estatísticas básicas (opcional)
 *  - caminhos relevantes de saída (Stages 4..16)
 *  - layout (Stage 14) se disponível
 */
public final class Stage17Manifest {

    private Stage17Manifest() {}

    public static final class Result {
        public final Path outDir;
        public final Path manifestJson;

        public Result(Path outDir, Path manifestJson) {
            this.outDir = outDir;
            this.manifestJson = manifestJson;
        }

        @Override
        public String toString() {
            return "Stage17.Result{outDir=" + outDir + ", manifest=" + manifestJson + "}";
        }
    }

    public static Result run(Path baseDir) throws IOException {
        Path outDir = baseDir.resolve("cmp_stage17_out");
        Files.createDirectories(outDir);
        Path manifest = outDir.resolve("manifest.json");

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("baseDir", baseDir.toString());

        // Paths relevantes
        putDir(doc, "stage4SchemaDir", baseDir.resolve("cmp_stage4_schema"));
        putDir(doc, "stage5Dir",       baseDir.resolve("cmp_stage5_out"));
        putDir(doc, "stage7Dir",       baseDir.resolve("cmp_stage7_out"));
        putDir(doc, "stage8Dir",       baseDir.resolve("cmp_stage8_out"));
        putDir(doc, "stage9Dir",       baseDir.resolve("cmp_stage9_out"));
        putDir(doc, "stage10Dir",      baseDir.resolve("cmp_stage10_out"));
        putDir(doc, "stage11Dir",      baseDir.resolve("cmp_stage11_out"));
        putDir(doc, "stage12Dir",      baseDir.resolve("cmp_stage12_out"));
        putDir(doc, "stage13Dir",      baseDir.resolve("cmp_stage13_out"));
        putDir(doc, "stage14Dir",      baseDir.resolve("cmp_stage14_out"));
        putDir(doc, "stage15Dir",      baseDir.resolve("cmp_stage15_out"));
        putDir(doc, "stage16Dir",      baseDir.resolve("cmp_stage16_out"));

        // Best period (Stage 12)
        Integer bestPeriod = readBestPeriod(baseDir.resolve("cmp_stage12_out").resolve("stage12_best_period.txt"));
        if (bestPeriod != null) {
            doc.put("bestPeriod", bestPeriod);
        }

        // Layout final (Stage 14)
        Path layoutTxt = baseDir.resolve("cmp_stage14_out").resolve("stage14_layout_final.txt");
        if (Files.exists(layoutTxt)) {
            List<String> layoutLines = Files.readAllLines(layoutTxt, StandardCharsets.UTF_8);
            doc.put("layoutFinalTxt", layoutLines);
        } else {
            // fallback: Stage 13
            Path s13 = baseDir.resolve("cmp_stage13_out").resolve("stage13_layout.txt");
            if (Files.exists(s13)) {
                List<String> layoutLines = Files.readAllLines(s13, StandardCharsets.UTF_8);
                doc.put("layoutInferTxt", layoutLines);
            }
        }

        // Stage 16 final CSV
        Path s16 = baseDir.resolve("cmp_stage16_out").resolve("stage16_final.csv");
        if (Files.exists(s16)) {
            doc.put("finalCsv", s16.toString());
        }

        // Escreve JSON simples (manual)
        try (BufferedWriter w = Files.newBufferedWriter(manifest, StandardCharsets.UTF_8)) {
            w.write(toJson(doc));
        }

        Result res = new Result(outDir, manifest);
        System.out.println("[Stage 17] " + res);
        return res;
    }

    // ---------- util ----------

    private static void putDir(Map<String, Object> doc, String key, Path dir) {
        if (dir != null && Files.isDirectory(dir)) {
            doc.put(key, dir.toString());
        }
    }

    private static Integer readBestPeriod(Path txt) {
        if (!Files.exists(txt)) return null;
        Pattern P = Pattern.compile("\\bperiod\\s*=\\s*(\\d+)");
        try {
            for (String ln : Files.readAllLines(txt, StandardCharsets.UTF_8)) {
                Matcher m = P.matcher(ln);
                if (m.find()) {
                    return Integer.parseInt(m.group(1));
                }
            }
        } catch (IOException | NumberFormatException ignore) {}
        return null;
    }

    /** JSON bem simples para Map<String,Object>, List, String, Number e List<String>. */
    private static String toJson(Object o) {
        if (o == null) return "null";
        if (o instanceof String) return quote((String) o);
        if (o instanceof Number || o instanceof Boolean) return String.valueOf(o);
        if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) o;
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean first = true;
            for (Map.Entry<String, Object> e : m.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append(quote(e.getKey())).append(":").append(toJson(e.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }
        if (o instanceof Iterable) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            boolean first = true;
            for (Object it : (Iterable<?>) o) {
                if (!first) sb.append(",");
                first = false;
                sb.append(toJson(it));
            }
            sb.append("]");
            return sb.toString();
        }
        // fallback: toString com aspas
        return quote(String.valueOf(o));
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder();
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"':  sb.append("\\\""); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int)c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    private static void putFileIfExists(Map<String, Object> doc, String key, Path file) {
        if (file != null && Files.exists(file)) {
            doc.put(key, file.toString());
        }
    }

}
