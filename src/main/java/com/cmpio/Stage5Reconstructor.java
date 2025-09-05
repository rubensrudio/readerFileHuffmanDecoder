package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Stage 5 – Reconstructor (v1.1)
 *
 * - Monta bitstream multi-record
 * - Auto-probe (LSB/MSB, invert, shift)
 * - Decodifica tokens e separa super-grupos por 251 (EOB forte)
 * - **NOVO**: aplica o mesmo merge de padding do Stage 4 v2 / 4.3
 * - Classifica SGs em P/A/B/C/D/E/T (heurístico)
 * - Detecta ciclos A→B→C→D→E
 * - Exporta:
 *     * cycle_k_tokens.csv  : tokens brutos por SG do ciclo
 *     * cycle_k_rle.csv     : RLE simples (runs) para ver o padrão de 91
 */
public final class Stage5Reconstructor {

    private static final int SEP_STRONG = 251; // EOB forte
    private static final int SEP_SOFT   = 249; // separador fraco
    private static final int FILL       = 91;  // padding/RLE
    private static final int OP63       = 63;  // parâmetro imediato
    private static final int OP195      = 195; // controle
    private static final int OP210      = 210; // raro
    private static final int OP61       = 61;  // raro

    private Stage5Reconstructor() {}

    public static void run(ByteBuffer file, int recStart, ByteOrder order,
                           SegmentRecord rec, Path outDir) {
        try {
            if (outDir != null) Files.createDirectories(outDir);

            // 1) Montar bitstream multi-record
            long requiredBits = rec.md.totalBits;
            PayloadAssembler.Assembled assembled =
                    PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

            byte[] stream = assembled.bytes;
            long limitBits = Math.min(requiredBits, (long) stream.length * 8L);

            // 2) Auto-probe
            Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, stream, limitBits);
            System.out.printf(Locale.ROOT,
                    "Stage 5: using bits-%s, invert=%s, shift=%d%n",
                    pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);

            // 3) Decoder
            HuffmanStreamDecoder dec = HuffmanStreamDecoder.fromCanonical(
                    rec.huffman.symbols, rec.huffman.lens, stream, requiredBits,
                    pick.lsb, pick.invert, pick.shift);

            // 4) SGs por 251
            List<List<Integer>> sgs = splitBy251(dec);
            System.out.printf(Locale.ROOT, "Stage 5: %d super-grupos (antes do merge)%n", sgs.size());

            // 5) **Merge de padding** (mesma regra do Stage 4 v2/4.3)
            double padMergeThreshold = 0.70; // >=70% de 91 => padding
            sgs = mergePaddingNeighbors(sgs, padMergeThreshold);
            System.out.printf(Locale.ROOT, "Stage 5: %d super-grupos (após merge)%n", sgs.size());

            // 6) Classificar SGs
            List<SGInfo> infos = new ArrayList<>();
            for (int i = 0; i < sgs.size(); i++) {
                SGInfo info = analyzeSG(sgs.get(i));
                info.index = i;
                info.type  = classify(info, i, sgs.size());
                infos.add(info);
            }

            // 7) Ciclos A→B→C→D→E
            List<int[]> cycles = detectCycles(infos);
            if (cycles.isEmpty()) {
                System.out.println("Stage 5: nenhum ciclo A→B→C→D→E detectado após o merge. " +
                        "Exportando um bloco único como fallback.");
            } else {
                System.out.print("Stage 5: ciclos A→B→C→D→E em: ");
                for (int i = 0; i < cycles.size(); i++) {
                    if (i > 0) System.out.print(", ");
                    int[] c = cycles.get(i);
                    System.out.print("[" + c[0] + ".." + (c[0] + c[1] - 1) + "]");
                }
                System.out.println();
            }

            // 8) Export
            Path out = (outDir == null)
                    ? Paths.get("cmp_stage5_out")
                    : outDir;
            Files.createDirectories(out);

            if (cycles.isEmpty()) {
                exportCycle(out, 0, infos, sgs, Math.max(0, findFirstABegin(infos)), Math.min(5, sgs.size()));
            } else {
                int k = 0;
                for (int[] c : cycles) exportCycle(out, k++, infos, sgs, c[0], c[1]);
            }

            System.out.println("Stage 5: export concluído em " + out.toAbsolutePath());

        } catch (Exception e) {
            System.err.println("Stage5Reconstructor: erro: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    // ===== Split / Merge =====

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

    // ===== SG analysis / classify =====

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
        String type;
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

            if (t == FILL) { run91 = (prev == FILL) ? run91 + 1 : 1; best = Math.max(best, run91); }
            else { run91 = 0; }

            if (t == OP63)  info.pos63.add(i);
            if (t == OP195) info.pos195.add(i);
            if (t == OP210) info.pos210.add(i);
            if (t == OP61)  info.pos61.add(i);
            if (t == SEP_SOFT) c249++;

            prev = t;
        }
        info.bestRun91 = best;
        info.count249 = c249;

        long c91 = info.hist.getOrDefault(FILL, 0L);
        info.fillDensity = (info.size == 0) ? 0.0 : (double) c91 / (double) info.size;
        info.has195Pair = containsPair(sg, OP195);
        return info;
    }

    private static boolean containsPair(List<Integer> g, int sym) {
        for (int i = 1; i < g.size(); i++) if (g.get(i) == sym && g.get(i - 1) == sym) return true;
        return false;
    }

    private static String classify(SGInfo sg, int idx, int total) {
        if (idx == 0 && sg.pos210.size() == 1 && sg.pos195.size() >= 2 && sg.fillDensity >= 0.7) return "P";
        if (sg.size >= 28 && sg.size <= 38 && sg.has195Pair &&
                sg.pos63.contains(24) && sg.pos195.contains(8) && sg.pos195.contains(9)) return "A";
        if (sg.size >= 18 && sg.size <= 24 &&
                sg.pos210.contains(0) && sg.pos195.contains(4) && sg.pos195.contains(6)) return "B";
        if (sg.size >= 6 && sg.size <= 8 &&
                sg.pos195.contains(3) && sg.count249 >= 2 &&
                sg.fillDensity >= 0.45 && sg.fillDensity <= 0.75) return "C";
        if (sg.size >= 65 && sg.size <= 85 &&
                sg.fillDensity >= 0.65 && sg.fillDensity <= 0.8 &&
                sg.pos63.contains(31) && sg.pos210.contains(34) && sg.pos210.contains(40) &&
                sg.pos195.size() >= 6) return "D";
        if (sg.size >= 90 && sg.size <= 110 &&
                sg.pos63.contains(19) && sg.pos63.contains(35) && sg.pos63.contains(70) &&
                sg.pos195.size() >= 9 && sg.count249 >= 8) return "E";
        if (idx >= total - 3 && sg.pos210.isEmpty() && sg.pos195.size() >= 3) return "T";
        if (sg.fillDensity >= 0.7 && sg.bestRun91 >= 3) return "D?"; // padding-like
        return "M";
    }

    private static List<int[]> detectCycles(List<SGInfo> infos) {
        List<int[]> cycles = new ArrayList<>();
        String[] want = {"A","B","C","D","E"};
        for (int i = 0; i + want.length <= infos.size(); i++) {
            boolean ok = true;
            for (int k = 0; k < want.length; k++) {
                if (!want[k].equals(infos.get(i + k).type)) { ok = false; break; }
            }
            if (ok) cycles.add(new int[]{ i, want.length });
        }
        return cycles;
    }

    // fallback: localizar primeiro 'A' para exportar algo coerente
    private static int findFirstABegin(List<SGInfo> infos) {
        for (int i = 0; i < infos.size(); i++) if ("A".equals(infos.get(i).type)) return i;
        return 0;
    }

    // ===== Export =====

    private static void exportCycle(Path outDir, int cycleIdx,
                                    List<SGInfo> infos, List<List<Integer>> sgs,
                                    int start, int len) throws Exception {

        List<SGInfo> sliceInfo = new ArrayList<>();
        List<List<Integer>> sliceSgs = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            int idx = start + i;
            if (idx >= 0 && idx < sgs.size()) {
                sliceInfo.add(infos.get(idx));
                sliceSgs.add(sgs.get(idx));
            }
        }

        Path tokensCsv = outDir.resolve(String.format(Locale.ROOT, "cycle_%02d_tokens.csv", cycleIdx));
        List<String> lines = new ArrayList<>();
        lines.add("sg_index,sg_type,pos,sym");
        for (int i = 0; i < sliceSgs.size(); i++) {
            int sgIndex = start + i;
            String sgType = sliceInfo.get(i).type;
            List<Integer> g = sliceSgs.get(i);
            for (int p = 0; p < g.size(); p++) {
                lines.add(sgIndex + "," + sgType + "," + p + "," + g.get(p));
            }
        }
        Files.write(tokensCsv, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("  cycle_" + cycleIdx + ": tokens -> " + tokensCsv.toAbsolutePath());

        Path rleCsv = outDir.resolve(String.format(Locale.ROOT, "cycle_%02d_rle.csv", cycleIdx));
        List<String> rle = new ArrayList<>();
        rle.add("sg_index,sg_type,run_sym,run_len");
        for (int i = 0; i < sliceSgs.size(); i++) {
            int sgIndex = start + i;
            String sgType = sliceInfo.get(i).type;
            List<int[]> runs = runsOf(sliceSgs.get(i));
            for (int[] rr : runs) {
                rle.add(sgIndex + "," + sgType + "," + rr[0] + "," + rr[1]);
            }
        }
        Files.write(rleCsv, rle, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("  cycle_" + cycleIdx + ": RLE -> " + rleCsv.toAbsolutePath());
    }

    private static List<int[]> runsOf(List<Integer> g) {
        List<int[]> out = new ArrayList<>();
        if (g.isEmpty()) return out;
        int prev = g.get(0), len = 1;
        for (int i = 1; i < g.size(); i++) {
            int cur = g.get(i);
            if (cur == prev) { len++; }
            else { out.add(new int[]{ prev, len }); prev = cur; len = 1; }
        }
        out.add(new int[]{ prev, len });
        return out;
    }

    // ===== Auto-probe =====

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
