package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Stage 4.3 - Schema Extractor
 *
 *  - Monta bitstream (PayloadAssembler)
 *  - Auto-probe (LSB/MSB, invert, shift)
 *  - Decodifica e separa super-grupos por 251 (EOB forte)
 *  - Extrai features por SG (tamanho, densidade 91, runs, pos de 63/195/210/249, pares 195)
 *  - Classifica SGs em tipos heurísticos: P/A/B/C/D/E/T
 *  - Detecta ciclos A→B→C→D→E
 *  - Exporta JSON com SGs + resumo de ciclos
 *
 * Uso:
 *   Stage4SchemaExtractor.run(file, rec.recStart, order, rec,
 *                             outDir Path.of("cmp_stage4_schema"),
 *                             padMergeThreshold 0.70);
         */
public final class Stage4SchemaExtractor {

    // símbolos
    private static final int SEP_STRONG = 251; // EOB
    private static final int SEP_SOFT   = 249; // separador fraco
    private static final int FILL       = 91;  // padding/RLE
    private static final int OP63       = 63;  // parâmetro imediato
    private static final int OP195      = 195; // controle (aparece em par)
    private static final int OP210      = 210; // marcador raro
    private static final int OP61       = 61;  // raro

    private Stage4SchemaExtractor() {}

    // ---------------------------------------------------------------------
    // Entrada principal

    public static void run(ByteBuffer file, int recStart, ByteOrder order,
                           SegmentRecord rec, Path outDir, double padMergeThreshold) {
        try {
            if (outDir != null) Files.createDirectories(outDir);

            // 1) montar bitstream multi-record
            long requiredBits = rec.md.totalBits;
            PayloadAssembler.Assembled assembled =
                    PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

            byte[] stream = assembled.bytes;
            long limitBits = Math.min(requiredBits, (long) stream.length * 8L);

            // 2) auto-probe
            Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, stream, limitBits);
            System.out.printf(Locale.ROOT,
                    "Stage 4.3: using bits-%s, invert=%s, shift=%d%n",
                    pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);

            // 3) decodificador
            HuffmanStreamDecoder dec = HuffmanStreamDecoder.fromCanonical(
                    rec.huffman.symbols, rec.huffman.lens, stream, requiredBits,
                    pick.lsb, pick.invert, pick.shift);

            // 4) super-grupos por 251
            List<List<Integer>> sgs = splitBy251(dec);
            System.out.printf(Locale.ROOT, "Stage 4.3: %d super-grupos%n", sgs.size());

            // 5) (opcional) merge de padding vizinho (mesma ideia do Stage 4 v2)
            sgs = mergePaddingNeighbors(sgs, padMergeThreshold);

            // 6) extrai features e classifica
            List<SGInfo> infos = new ArrayList<>();
            for (int i = 0; i < sgs.size(); i++) {
                SGInfo info = analyzeSG(sgs.get(i));
                info.index = i;
                info.type  = classify(info, i, sgs.size());
                infos.add(info);
            }

            // 7) detecta ciclos A→B→C→D→E
            CycleSummary cycle = detectCycles(infos);

            // 8) imprime resumo e exporta JSON
            printSummary(infos, cycle);
            String json = toJson(rec, infos, cycle);
            Path jsonOut = (outDir == null)
                    ? Path.of("schema_stage4.json")
                    : outDir.resolve("schema_stage4.json");
            Files.writeString(jsonOut, json);
            System.out.println("Schema JSON salvo em: " + jsonOut.toAbsolutePath());

        } catch (Exception e) {
            System.err.println("Stage4SchemaExtractor: erro: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    // ---------------------------------------------------------------------
    // Split por 251 (EOB)

    private static List<List<Integer>> splitBy251(HuffmanStreamDecoder dec) {
        List<List<Integer>> out = new ArrayList<>();
        List<Integer> cur = new ArrayList<>();
        int s;
        while ((s = dec.next()) >= 0) {
            if (s == SEP_STRONG) {
                if (!cur.isEmpty()) out.add(cur);
                cur = new ArrayList<>();
            } else {
                cur.add(s);
            }
        }
        if (!cur.isEmpty()) out.add(cur);
        return out;
    }

    // Merge de padding vizinho (SG com alta densidade de 91) ao anterior
    private static List<List<Integer>> mergePaddingNeighbors(List<List<Integer>> sgs, double padThreshold) {
        if (sgs.isEmpty()) return sgs;
        List<List<Integer>> out = new ArrayList<>();
        out.add(new ArrayList<>(sgs.get(0)));
        for (int i = 1; i < sgs.size(); i++) {
            List<Integer> g = sgs.get(i);
            SGInfo tmp = analyzeSG(g);
            boolean padHeavy = tmp.fillDensity >= padThreshold && tmp.size > 0;
            if (padHeavy) {
                out.get(out.size()-1).addAll(g);
            } else {
                out.add(new ArrayList<>(g));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // Análise por SG

    private static final class SGInfo {
        int index;
        int size;
        double fillDensity;
        int bestRun91;
        List<Integer> pos63 = new ArrayList<>();
        List<Integer> pos195 = new ArrayList<>();
        List<Integer> pos210 = new ArrayList<>();
        List<Integer> pos61  = new ArrayList<>();
        int count249;
        boolean has195Pair;
        Map<Integer, Long> hist = new HashMap<>();
        String type; // P / A / B / C / D / E / T / M (mixed)
    }

    private static SGInfo analyzeSG(List<Integer> sg) {
        SGInfo info = new SGInfo();
        info.size = sg.size();

        int run91 = 0, best = 0;
        int prev = -1;
        int c249 = 0;

        for (int i = 0; i < sg.size(); i++) {
            int t = sg.get(i);
            info.hist.merge(t, 1L, Long::sum);
            if (t == FILL) {
                run91 = (prev == FILL) ? run91 + 1 : 1;
                best = Math.max(best, run91);
            } else {
                run91 = 0;
            }
            prev = t;

            if (t == OP63) info.pos63.add(i);
            if (t == OP195) info.pos195.add(i);
            if (t == OP210) info.pos210.add(i);
            if (t == OP61)  info.pos61.add(i);
            if (t == SEP_SOFT) c249++;
        }
        info.bestRun91 = best;
        info.count249 = c249;
        long c91 = info.hist.getOrDefault(FILL, 0L);
        info.fillDensity = (info.size == 0) ? 0.0 : (double) c91 / (double) info.size;
        info.has195Pair = containsPair(sg, OP195);

        return info;
    }

    private static boolean containsPair(List<Integer> g, int sym) {
        for (int i = 1; i < g.size(); i++) if (g.get(i) == sym && g.get(i-1) == sym) return true;
        return false;
    }

    // ---------------------------------------------------------------------
    // Classificação heurística (tipos P/A/B/C/D/E/T/M)

    private static String classify(SGInfo sg, int idx, int total) {
        // Preambulo: primeiro SG com 210 no meio e 195 nas bordas, densidade alta de 91
        if (idx == 0 && sg.pos210.size() == 1 && sg.pos195.size() >= 2 && sg.fillDensity >= 0.7) {
            return "P"; // preâmbulo
        }

        // Tipo A: size ~33, 195-pair em [8,9] e [18,23]; 63 em [24]
        if (sg.size >= 28 && sg.size <= 38
                && sg.has195Pair
                && sg.pos63.contains(Integer.valueOf(24))
                && sg.pos195.contains(Integer.valueOf(8))
                && sg.pos195.contains(Integer.valueOf(9))) {
            return "A";
        }

        // Tipo B: size ~21, 210 em [0], 195 em [4] e [6]
        if (sg.size >= 18 && sg.size <= 24
                && sg.pos210.contains(Integer.valueOf(0))
                && sg.pos195.contains(Integer.valueOf(4))
                && sg.pos195.contains(Integer.valueOf(6))) {
            return "B";
        }

        // Tipo C: size ~7, 195 em [3], dois 249, densidade ~0.5..0.7
        if (sg.size >= 6 && sg.size <= 8
                && sg.pos195.contains(Integer.valueOf(3))
                && sg.count249 >= 2
                && sg.fillDensity >= 0.45 && sg.fillDensity <= 0.75) {
            return "C";
        }

        // Tipo D: size ~75, densidade ~0.70, 63 em [31], 210 em [34] e [40], vários 195
        if (sg.size >= 65 && sg.size <= 85
                && sg.fillDensity >= 0.65 && sg.fillDensity <= 0.8
                && sg.pos63.contains(Integer.valueOf(31))
                && sg.pos210.contains(Integer.valueOf(34))
                && sg.pos210.contains(Integer.valueOf(40))
                && sg.pos195.size() >= 6) {
            return "D";
        }

        // Tipo E: size ~98, 63 em [19,35,70], >=10 vezes 195, ~11 vezes 249
        if (sg.size >= 90 && sg.size <= 110
                && sg.pos63.contains(Integer.valueOf(19))
                && sg.pos63.contains(Integer.valueOf(35))
                && sg.pos63.contains(Integer.valueOf(70))
                && sg.pos195.size() >= 9
                && sg.count249 >= 8) {
            return "E";
        }

        // Tail: últimos SGs com controle e sem 210; tamanho médio/baixo
        if (idx >= total - 3 && sg.pos210.isEmpty() && sg.pos195.size() >= 3) {
            return "T";
        }

        // fallback
        if (sg.fillDensity >= 0.7 && sg.bestRun91 >= 3) return "D?"; // padding-like
        return "M"; // mixed
    }

    // ---------------------------------------------------------------------
    // Ciclos (busca por A,B,C,D,E repetidos)

    private static final class CycleSummary {
        final List<int[]> cycles = new ArrayList<>(); // [startIndex, length(=5)]
    }

    private static CycleSummary detectCycles(List<SGInfo> infos) {
        CycleSummary sum = new CycleSummary();
        String[] want = {"A","B","C","D","E"};
        for (int i = 0; i + want.length <= infos.size(); i++) {
            boolean ok = true;
            for (int k = 0; k < want.length; k++) {
                if (!want[k].equals(infos.get(i + k).type)) { ok = false; break; }
            }
            if (ok) sum.cycles.add(new int[]{ i, want.length });
        }
        return sum;
    }

    // ---------------------------------------------------------------------
    // Impressão & JSON

    private static void printSummary(List<SGInfo> infos, CycleSummary cycle) {
        System.out.println("=== Stage 4.3 Schema ===");
        for (SGInfo sg : infos) {
            System.out.printf(Locale.ROOT,
                    "SG#%d: type=%s, size=%d, 91%%=%.0f%%, bestRun91=%d, 63=%d, 195=%d, 210=%d, 249=%d%s%n",
                    sg.index, sg.type, sg.size, 100.0 * sg.fillDensity, sg.bestRun91,
                    sg.pos63.size(), sg.pos195.size(), sg.pos210.size(), sg.count249,
                    sg.has195Pair ? " (195-pair)" : "");
        }
        if (!cycle.cycles.isEmpty()) {
            System.out.print("Ciclos A→B→C→D→E em: ");
            for (int i = 0; i < cycle.cycles.size(); i++) {
                if (i > 0) System.out.print(", ");
                System.out.print("[" + cycle.cycles.get(i)[0] + ".." + (cycle.cycles.get(i)[0]+cycle.cycles.get(i)[1]-1) + "]");
            }
            System.out.println();
        } else {
            System.out.println("Nenhum ciclo A→B→C→D→E detectado explicitamente (pode haver variações leves).");
        }
    }

    private static String toJson(SegmentRecord rec, List<SGInfo> infos, CycleSummary cycle) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"huffman\": {");
        sb.append("\"N\": ").append(rec.huffman.N).append(", ");
        sb.append("\"payloadStartByte\": ").append(rec.md.payloadStartByte).append(", ");
        sb.append("\"totalBits\": ").append(rec.md.totalBits).append("},\n");
        sb.append("  \"superGroups\": [\n");
        for (int i = 0; i < infos.size(); i++) {
            SGInfo s = infos.get(i);
            sb.append("    {");
            sb.append("\"index\": ").append(s.index).append(", ");
            sb.append("\"type\": \"").append(s.type).append("\", ");
            sb.append("\"size\": ").append(s.size).append(", ");
            sb.append("\"fillPct\": ").append(String.format(Locale.ROOT, "%.4f", s.fillDensity)).append(", ");
            sb.append("\"bestRun91\": ").append(s.bestRun91).append(", ");
            sb.append("\"pos63\": ").append(listToJson(s.pos63)).append(", ");
            sb.append("\"pos195\": ").append(listToJson(s.pos195)).append(", ");
            sb.append("\"pos210\": ").append(listToJson(s.pos210)).append(", ");
            sb.append("\"pos61\": ").append(listToJson(s.pos61)).append(", ");
            sb.append("\"count249\": ").append(s.count249).append(", ");
            sb.append("\"has195Pair\": ").append(s.has195Pair).append(", ");
            sb.append("\"top\": ").append(histTopJson(s.hist, 8));
            sb.append("}");
            if (i + 1 < infos.size()) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ],\n");
        sb.append("  \"cycles\": ").append(cyclesToJson(cycle)).append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static String listToJson(List<Integer> lst) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < lst.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(lst.get(i));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String histTopJson(Map<Integer, Long> hist, int topK) {
        List<Map.Entry<Integer, Long>> list = new ArrayList<>(hist.entrySet());
        list.sort((a,b)-> Long.compare(b.getValue(), a.getValue()));
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        for (Map.Entry<Integer, Long> e : list) {
            if (shown >= topK) break;
            if (shown > 0) sb.append(",");
            sb.append("{\"sym\":").append(e.getKey()).append(",\"count\":").append(e.getValue()).append("}");
            shown++;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String cyclesToJson(CycleSummary c) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < c.cycles.size(); i++) {
            if (i > 0) sb.append(",");
            int[] it = c.cycles.get(i);
            sb.append("{\"start\":").append(it[0]).append(",\"len\":").append(it[1]).append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    // ---------------------------------------------------------------------
    // Auto-probe (copiado do Stage 4)

    private static Probe autoProbe(int[] symbols, int[] lens, byte[] stream, long limitBits) {
        boolean[] orders = { true, false }; // lsb?
        boolean[] invs   = { false, true };
        Probe best = null;
        for (boolean lsb : orders) {
            for (boolean inv : invs) {
                for (int shift = 0; shift <= 3; shift++) {
                    int score = scoreConfig(symbols, lens, stream, limitBits, lsb, inv, shift);
                    if (best == null || score > best.score) best = new Probe(lsb, inv, shift, score);
                }
            }
        }
        if (best == null) best = new Probe(false, false, 0, 0);
        return best;
    }

    private static int scoreConfig(int[] symbols, int[] lens, byte[] stream, long limitBits,
                                   boolean lsb, boolean invert, int shift) {
        HuffmanStreamDecoder d = HuffmanStreamDecoder.fromCanonical(
                symbols, lens, stream, Math.min(limitBits, 4096), lsb, invert, shift);
        int ok = 0, distinct = 0;
        int[] seen = new int[256];
        Arrays.fill(seen, 0);
        for (int i = 0; i < 256; i++) {
            int s = d.next();
            if (s < 0) break;
            ok++;
            if (s >= 0 && s < 256 && seen[s] == 0) { seen[s] = 1; distinct++; }
        }
        return ok + 2 * distinct;
    }

    private static final class Probe {
        final boolean lsb; final boolean invert; final int shift; final int score;
        Probe(boolean lsb, boolean invert, int shift, int score) {
            this.lsb = lsb; this.invert = invert; this.shift = shift; this.score = score;
        }
    }
}
