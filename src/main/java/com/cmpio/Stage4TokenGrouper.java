package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Stage 4 v2:
 *  - Monta bitstream multi-record (via PayloadAssembler)
 *  - Auto-probe do alinhamento de bits (LSB/MSB, invert, shift)
 *  - Quebra por separador forte (251) quando splitOnly251=true
 *  - Faz "merge" de grupos majoritariamente padding (símbolo 91) com vizinhos
 *  - Emite sumário de "super-grupos" (entre 251), incluindo shape/heurísticas
 *
 * Uso:
 *   Stage4TokenGrouper.run(file, recStart, order, rec, splitOnly251=true, padMergeThreshold=0.70);
        */
public final class Stage4TokenGrouper {

    // Tokens de interesse (mantemos os mesmos do v1)
    public static final int SEP_STRONG = 251; // EOB forte
    public static final int SEP_SOFT   = 249; // separador fraco interno
    public static final int FILL       = 91;  // padding/RLE
    public static final int OP63       = 63;  // campo imediato/comprimento
    public static final int OP195      = 195; // controle (freq. em par)
    public static final int OP210      = 210; // marcador raro
    public static final int OP61       = 61;  // raro

    private Stage4TokenGrouper() {}

    // ---------------------------------------------------------------------
    // API

    public static void run(ByteBuffer file, int recStart, ByteOrder order,
                           SegmentRecord rec, boolean splitOnly251, double padMergeThreshold) {

        // 1) montar bitstream multi-record
        long requiredBits = rec.md.totalBits;
        PayloadAssembler.Assembled assembled =
                PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

        byte[] stream = assembled.bytes;
        long limitBits = Math.min(requiredBits, (long) stream.length * 8L);

        // 2) auto-probe
        Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, stream, limitBits);
        System.out.printf(Locale.ROOT,
                "Stage 4: using bits-%s, invert=%s, shift=%d%n",
                pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);

        // 3) decoder
        HuffmanStreamDecoder dec = HuffmanStreamDecoder.fromCanonical(
                rec.huffman.symbols, rec.huffman.lens, stream, requiredBits,
                pick.lsb, pick.invert, pick.shift);

        // 4) tokeniza
        List<List<Integer>> groups = split(dec, splitOnly251);
        System.out.printf(Locale.ROOT, "Stage 4: %d grupos identificados (%s)%n",
                groups.size(), splitOnly251 ? "splitOnly251" : "251+249");

        // 5) se splitOnly251==false, ainda podemos promover "super-grupos" entre 251
        //    mas no modo 251-only cada grupo já é um super-grupo.
        List<List<Integer>> superGroups = splitOnly251 ? groups : promoteToSuperGroups(groups);

        // --- dense-mode detector: 1 grupo e nenhum marcador “antigo”
        if (superGroups.size() == 1) {
            List<Integer> all = superGroups.get(0);
            Set<Integer> uniq = new HashSet<>(all);
            boolean hasOldMarkers = containsAny(uniq, OP63, FILL, OP195, OP210, SEP_SOFT, SEP_STRONG);
            if (!hasOldMarkers) {
                System.out.println("[Stage 4] dense stream detected (no 63/91/195/210/249/251).");
                printDenseDiag(all);
            }
        }


        // 6) merge de padding contíguo (dentro da lista de super-grupos)
        List<List<Integer>> merged = mergePaddingNeighbors(superGroups, padMergeThreshold);

        // 7) sumariza super-grupos
        summarizeSuperGroups(merged);
    }

    private static Map<Integer, Long> hist(List<Integer> xs) {
        Map<Integer, Long> h = new HashMap<>();
        for (int v : xs) h.merge(v, 1L, Long::sum);
        return h;
    }
    private static List<int[]> topNGrams(List<Integer> xs, int n, int topK) {
        Map<String, Long> freq = new HashMap<>();
        for (int i = 0; i + n <= xs.size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int k = 0; k < n; k++) { if (k>0) sb.append('_'); sb.append(xs.get(i+k)); }
            freq.merge(sb.toString(), 1L, Long::sum);
        }
        List<Map.Entry<String, Long>> list = new ArrayList<>(freq.entrySet());
        list.sort((a,b)->Long.compare(b.getValue(), a.getValue()));
        List<int[]> out = new ArrayList<>();
        for (int i = 0; i < Math.min(topK, list.size()); i++) {
            String[] parts = list.get(i).getKey().split("_");
            int[] ngram = new int[parts.length];
            for (int j=0;j<parts.length;j++) ngram[j] = Integer.parseInt(parts[j]);
            out.add(ngram);
        }
        return out;
    }
    private static boolean containsAny(Set<Integer> set, int... wanted) {
        for (int w : wanted) if (set.contains(w)) return true;
        return false;
    }
    private static void printDenseDiag(List<Integer> tokens) {
        Map<Integer, Long> h = hist(tokens);
        List<Map.Entry<Integer, Long>> top = new ArrayList<>(h.entrySet());
        top.sort((a,b)->Long.compare(b.getValue(), a.getValue()));
        System.out.print("[Stage 4] dense-mode alphabet top: ");
        for (int i=0; i<Math.min(12, top.size()); i++) {
            Map.Entry<Integer, Long> e = top.get(i);
            System.out.print(e.getKey() + "(" + e.getValue() + ") ");
        }
        System.out.println();
        List<int[]> bi = topNGrams(tokens, 2, 8);
        List<int[]> tri= topNGrams(tokens, 3, 8);
        System.out.print("[Stage 4] top bi-grams: ");
        for (int[] g : bi) System.out.print(Arrays.toString(g) + " ");
        System.out.println();
        System.out.print("[Stage 4] top tri-grams: ");
        for (int[] g : tri) System.out.print(Arrays.toString(g) + " ");
        System.out.println();
    }

    // ---------------------------------------------------------------------
    // Split helpers

    /** Split por 251-only (forte) ou por 251/249 (forte+fraco). */
    private static List<List<Integer>> split(HuffmanStreamDecoder dec, boolean only251) {
        List<List<Integer>> out = new ArrayList<>();
        List<Integer> cur = new ArrayList<>();
        int s;
        while ((s = dec.next()) >= 0) {
            if (s == SEP_STRONG) {
                // fecha sempre
                if (!cur.isEmpty()) out.add(cur);
                cur = new ArrayList<>();
            } else if (!only251 && s == SEP_SOFT) {
                // fecha macio (apenas se only251=false)
                if (!cur.isEmpty()) out.add(cur);
                cur = new ArrayList<>();
            } else {
                cur.add(s);
            }
        }
        if (!cur.isEmpty()) out.add(cur);
        return out;
    }

    /** Promove uma sequência de grupos (quebrados por 251/249) para super-grupos por 251. */
    private static List<List<Integer>> promoteToSuperGroups(List<List<Integer>> groups) {
        // Aqui assumimos que a lista 'groups' é a saída do split(only251=false),
        // isto é, já quebrada por 249 e 251 indistintamente. Como não retivemos os 251,
        // vamos colar blocos consecutivos até que um grupo "pareça" fim de supergrupo.
        // Heurística simples: encerra super-grupo a cada K grupos (~média dos tamanhos)
        // Para manter determinístico e estável, usamos um limiar por densidade de 91:
        // quando um grupo tem baixa densidade de 91 e contém OP195/OP63, encaramos
        // como “fronteira” de sub-estrutura — mas sem 251 não há garantia de EOB real.
        // Como o modo principal agora é splitOnly251=true, isto vira fallback.
        List<List<Integer>> superG = new ArrayList<>();
        List<Integer> cur = new ArrayList<>();
        for (List<Integer> g : groups) {
            if (!cur.isEmpty()) cur.add(SEP_SOFT); // preserva separador lógico
            cur.addAll(g);
            // heurística: se grupo atual tem sinais de meta/controle e comprimento moderado,
            // fechamos o super-grupo
            GroupSummary gs = analyzeGroup(g);
            boolean boundary = (gs.has195Pair && gs.size <= 16) || gs.has63Between;
            if (boundary) {
                superG.add(cur);
                cur = new ArrayList<>();
            }
        }
        if (!cur.isEmpty()) superG.add(cur);
        return superG;
    }

    // ---------------------------------------------------------------------
    // Padding merge

    private static List<List<Integer>> mergePaddingNeighbors(List<List<Integer>> groups, double padThreshold) {
        if (groups.isEmpty()) return groups;
        List<List<Integer>> out = new ArrayList<>();
        out.add(new ArrayList<>(groups.get(0)));
        for (int i = 1; i < groups.size(); i++) {
            List<Integer> g = groups.get(i);
            GroupSummary gs = analyzeGroup(g);
            boolean isPadHeavy = gs.fillDensity >= padThreshold && gs.size > 0;

            if (isPadHeavy) {
                // preferimos "absorver" padding no vizinho anterior (se existir)
                List<Integer> prev = out.get(out.size() - 1);
                prev.addAll(g);
            } else {
                out.add(new ArrayList<>(g));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // Super-group summary

    private static void summarizeSuperGroups(List<List<Integer>> superGroups) {
        System.out.printf(Locale.ROOT, "Stage 4 (v2): %d super-grupos%n", superGroups.size());

        int idx = 0;
        int min = Integer.MAX_VALUE, max = 0, total = 0;
        for (List<Integer> sg : superGroups) {
            GroupSummary gs = analyzeGroup(sg);

            int c249 = 0;
            for (int v : sg) if (v == SEP_SOFT) c249++;

            System.out.printf(Locale.ROOT,
                    "SG#%d: size=%d, 91%%=%.0f%%, bestRun91=%d, 63=%d, 195=%d, 210=%d, 249=%d, kind=%s%s%n",
                    idx, gs.size, 100.0 * gs.fillDensity, gs.bestRun91,
                    gs.pos63.size(), gs.pos195.size(), gs.pos210.size(), c249, classifySuper(gs, c249),
                    gs.has195Pair ? " (195-pair)" : "");

            if (!gs.pos63.isEmpty())
                System.out.println("  pos(63): " + preview(gs.pos63));
            if (!gs.pos195.isEmpty())
                System.out.println("  pos(195): " + preview(gs.pos195));
            if (!gs.pos210.isEmpty())
                System.out.println("  pos(210): " + preview(gs.pos210));

            // Top símbolos do SG
            List<Map.Entry<Integer, Long>> top = new ArrayList<>(gs.hist.entrySet());
            top.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
            System.out.print("  top: ");
            for (int i = 0; i < Math.min(6, top.size()); i++) {
                Map.Entry<Integer, Long> e = top.get(i);
                System.out.printf(Locale.ROOT, "%d(%d) ", e.getKey(), e.getValue());
            }
            System.out.println();

            min = Math.min(min, gs.size);
            max = Math.max(max, gs.size);
            total += gs.size;
            idx++;
        }
        if (superGroups.size() > 0) {
            double avg = (double) total / superGroups.size();
            double med = median(superGroups.stream().mapToInt(List::size).sorted().toArray());
            System.out.printf(Locale.ROOT, "Super-grupos: min=%d, med=%.1f, avg=%.1f, max=%d%n",
                    min, med, avg, max);
        }
    }

    private static String classifySuper(GroupSummary gs, int c249) {
        // Heurística de alto nível (para leitura humana):
        // - header/meta: tem 63 e baixa densidade de 91
        // - control: par de 195
        // - padding: densidade alta de 91 e runs grandes
        // - mixed: caso contrário
        if (gs.has63Between && gs.fillDensity < 0.5) return "header/meta";
        if (gs.has195Pair) return "control";
        if (gs.fillDensity >= 0.7 && gs.bestRun91 >= 3) return "padding/RLE";
        // Se há muitos 249 internos, provavelmente é um bloco composto
        if (c249 >= 2 && gs.fillDensity < 0.8) return "composite";
        return "mixed";
    }

    // ---------------------------------------------------------------------
    // Group analysis (reutilizado do v1, com pequenos ajustes)

    private static class GroupSummary {
        int size;
        Map<Integer, Long> hist = new HashMap<>();
        int bestRun91;
        double fillDensity;
        List<Integer> pos63 = new ArrayList<>();
        List<Integer> pos195 = new ArrayList<>();
        List<Integer> pos210 = new ArrayList<>();
        List<Integer> pos61  = new ArrayList<>();
        boolean has195Pair;
        boolean has63Between;
    }

    private static GroupSummary analyzeGroup(List<Integer> g) {
        GroupSummary gs = new GroupSummary();
        gs.size = g.size();

        int run91 = 0, bestRun91 = 0;
        int prev = -1;
        for (int i = 0; i < g.size(); i++) {
            int t = g.get(i);
            gs.hist.merge(t, 1L, Long::sum);

            if (t == FILL) {
                run91 = (prev == FILL) ? run91 + 1 : 1;
                bestRun91 = Math.max(bestRun91, run91);
            } else {
                run91 = 0;
            }
            prev = t;

            if (t == OP63)  gs.pos63.add(i);
            if (t == OP195) gs.pos195.add(i);
            if (t == OP210) gs.pos210.add(i);
            if (t == OP61)  gs.pos61.add(i);
        }
        gs.bestRun91 = bestRun91;

        // padrões locais simples
        gs.has195Pair = containsPair(g, OP195);
        gs.has63Between = hasPattern63Between(g);

        long c91 = gs.hist.getOrDefault(FILL, 0L);
        gs.fillDensity = (gs.size == 0) ? 0.0 : (double) c91 / (double) gs.size;
        return gs;
    }

    private static boolean containsPair(List<Integer> g, int sym) {
        for (int i = 1; i < g.size(); i++) if (g.get(i) == sym && g.get(i - 1) == sym) return true;
        return false;
    }

    private static boolean hasPattern63Between(List<Integer> g) {
        for (int i = 0; i < g.size(); i++) if (g.get(i) == OP63) return true;
        return false;
    }

    private static String preview(List<Integer> pos) {
        int k = Math.min(8, pos.size());
        return pos.subList(0, k).toString() + (pos.size() > k ? " ..." : "");
    }

    private static double median(int[] s) {
        if (s.length == 0) return 0;
        int n = s.length;
        if (n % 2 == 1) return s[n / 2];
        return 0.5 * (s[n / 2 - 1] + s[n / 2]);
    }

    // ---------------------------------------------------------------------
    // Auto-probe (igual ao v1)

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
