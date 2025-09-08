package com.cmpio;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Stage7Decoder {

    private Stage7Decoder() {}

    // ====== API ======

    public enum RunMode {
        ZERO_RUN,     // trata todos tokens como literais
        REPEAT_LAST   // 91 = repete último literal; 91s consecutivos viram um RUN
    }

    public static final class Stats {
        public int tokens;
        public int ops;
        public int literals;
        public int marks;        // não usamos marcadores aqui, deixado para futura expansão
        public int runs;
        public int samples;
        public int bestRun91;
        public int totalRun91;
        public Map<Integer,Integer> markerFreq = new HashMap<>();

        @Override
        public String toString() {
            return String.format(
                    "Stats{tokens=%d, ops=%d, literals=%d, marks=%d, runs=%d, samples=%d, bestRun91=%d, totalRun91=%d, markerFreq=%s}",
                    tokens, ops, literals, marks, runs, samples, bestRun91, totalRun91, markerFreq
            );
        }
    }

    public static final class Result {
        public int ops;
        public int samples;
        public Stats stats;

        @Override
        public String toString() {
            return "Stage7.Result{ops=" + ops + ", samples=" + samples + ", stats=" + stats + "}";
        }
    }

    public static final class Op {
        public enum Kind { LITERAL, RUN91 }
        public Kind kind;
        public int value;   // para LITERAL
        public int runLen;  // para RUN91

        public Op(Kind k, int v, int r) {
            this.kind = k; this.value = v; this.runLen = r;
        }
    }

    // ====== Entrada → Tokens ======

    /** Extrai todos os inteiros de uma linha (robusto contra cabeçalhos/colunas mistas). */
    private static final Pattern INT_PATTERN = Pattern.compile("[-+]?\\d+");

    public static List<Integer> readTokensCsv(Path csv) throws IOException {
        List<Integer> out = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher m = INT_PATTERN.matcher(line);
                // heurística: pegue o primeiro número da linha como o token.
                if (m.find()) {
                    int val = Integer.parseInt(m.group());
                    out.add(val);
                }
            }
        }
        return out;
    }

    // ====== Decodificação ======

    /** Converte lista de tokens em lista de operações (literal/run) segundo o modo. */
    public static List<Op> tokensToOps(List<Integer> tokens, RunMode mode) {
        List<Op> ops = new ArrayList<>();
        if (tokens == null || tokens.isEmpty()) return ops;

        if (mode == RunMode.ZERO_RUN) {
            for (int t : tokens) ops.add(new Op(Op.Kind.LITERAL, t, 0));
            return ops;
        }

        // REPEAT_LAST: 91 indica "repita o último literal"; 91s consecutivos viram uma única RUN
        int lastLiteral = 0;
        boolean hasLast = false;
        int i = 0;
        while (i < tokens.size()) {
            int t = tokens.get(i);
            if (t == 91) {
                // agrupar sequência de 91s
                int j = i;
                while (j < tokens.size() && tokens.get(j) == 91) j++;
                int len = j - i;
                ops.add(new Op(Op.Kind.RUN91, 0, len));
                i = j;
            } else {
                ops.add(new Op(Op.Kind.LITERAL, t, 0));
                lastLiteral = t; hasLast = true;
                i++;
            }
        }
        return ops;
    }

    /** Expande as operações em amostras (para ambos modos). */
    public static List<Integer> opsToSamples(List<Op> ops) {
        List<Integer> samples = new ArrayList<>();
        int last = 0;
        boolean hasLast = false;
        for (Op op : ops) {
            if (op.kind == Op.Kind.LITERAL) {
                samples.add(op.value);
                last = op.value; hasLast = true;
            } else {
                // RUN91
                int len = Math.max(0, op.runLen);
                int v = hasLast ? last : 0;
                for (int k = 0; k < len; k++) samples.add(v);
            }
        }
        return samples;
    }

    // ====== Persistência ======

    /** Salva operações detalhadas (uma por linha). */
    public static void writeOpsCsv(Path out, List<Op> ops) throws IOException {
        Files.createDirectories(out.getParent());
        try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write("idx,kind,value,runLen,outSampleIndex\n");
            int sampleIdx = 0;
            for (int i = 0; i < ops.size(); i++) {
                Op op = ops.get(i);
                if (op.kind == Op.Kind.LITERAL) {
                    bw.write(String.format(Locale.ROOT, "%d,LITERAL,%d,0,%d%n", i, op.value, sampleIdx));
                    sampleIdx += 1;
                } else {
                    bw.write(String.format(Locale.ROOT, "%d,RUN91,0,%d,%d%n", i, op.runLen, sampleIdx));
                    sampleIdx += op.runLen;
                }
            }
        }
    }

    /** Salva amostras como uma coluna única. */
    public static void writeSamplesCsv(Path out, List<Integer> samples) throws IOException {
        Files.createDirectories(out.getParent());
        try (BufferedWriter bw = Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            bw.write("sample\n");
            for (int v : samples) {
                bw.write(Integer.toString(v));
                bw.write('\n');
            }
        }
    }

    // ====== Métricas ======

    public static Stats computeStats(List<Integer> tokens, List<Op> ops, List<Integer> samples) {
        Stats s = new Stats();
        s.tokens = (tokens != null) ? tokens.size() : 0;
        s.ops    = (ops    != null) ? ops.size()    : 0;
        s.samples= (samples!= null) ? samples.size(): 0;

        int runs = 0, totalRun = 0, bestRun = 0, lits = 0;
        if (ops != null) {
            for (Op op : ops) {
                if (op.kind == Op.Kind.LITERAL) lits++;
                else {
                    runs++;
                    totalRun += op.runLen;
                    if (op.runLen > bestRun) bestRun = op.runLen;
                }
            }
        }
        s.literals    = lits;
        s.runs        = runs;
        s.totalRun91  = totalRun;
        s.bestRun91   = bestRun;
        s.marks       = 0; // não extraímos marcadores neste estágio
        return s;
    }

    // ====== Execução de alto nível ======

    /** Executa Stage 7 a partir de um CSV de tokens (Stage 5) e grava ops/samples por ciclo. */
    public static Result quickRunCsv(Path tokensCsv, Path outDir, RunMode mode) throws IOException {
        Files.createDirectories(outDir);

        // 1) ler tokens
        List<Integer> tokens = readTokensCsv(tokensCsv);

        // 2) tokens -> ops -> samples
        List<Op> ops = tokensToOps(tokens, mode);
        List<Integer> samples = opsToSamples(ops);

        // 3) salvar
        writeOpsCsv(outDir.resolve("ops.csv"), ops);
        writeSamplesCsv(outDir.resolve("samples.csv"), samples);

        // 4) stats/result
        Stats stats = computeStats(tokens, ops, samples);
        Result r = new Result();
        r.ops = stats.ops;
        r.samples = stats.samples;
        r.stats = stats;
        System.out.println("[Stage7] " + r);
        return r;
    }

    // Utilitário opcional (não mais usado no parser atual, mantido para compatibilidade)
    public static Integer findFirstInt(String line) {
        if (line == null) return null;
        Matcher m = INT_PATTERN.matcher(line);
        return m.find() ? Integer.parseInt(m.group()) : null;
    }
}
