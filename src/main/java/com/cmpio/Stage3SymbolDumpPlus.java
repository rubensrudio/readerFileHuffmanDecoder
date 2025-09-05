package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Stage 3 (Plus): instrumentação de tokenização.
 *
 * Mede:
 *  - Histograma de símbolos
 *  - Bigrams e Trigrams
 *  - Runs (por símbolo), com destaque para 91 (suspeita de RLE)
 *  - Matriz de transições (top-N)
 *  - Posições de tokens “raros” (63,95,195,210,249,251) – amostra
 *
 * Também exporta CSVs opcionais: hist.csv, bigrams.csv, trigrams.csv, runs.csv
 *
 * Uso:
 *   Stage3SymbolDumpPlus.run(file, recStart, order, rec, exportCsv=true, Path.of("out-dir"));
        */
public final class Stage3SymbolDumpPlus {

    private Stage3SymbolDumpPlus(){}

    public static final int DEFAULT_TOPN = 20;
    public static final int RARE_POS_SAMPLES = 64;

    public static void run(ByteBuffer file, int recStart, ByteOrder order,
                           SegmentRecord rec, boolean exportCsv, Path outDir) {
        // 1) bitstream multi-record
        long requiredBits = rec.md.totalBits;
        PayloadAssembler.Assembled assembled =
                PayloadAssembler.assemble(file, recStart, order, rec, requiredBits);

        byte[] stream = assembled.bytes;
        long availableBits = Math.min(requiredBits, (long) stream.length * 8L);

        // 2) auto-probe
        Probe pick = autoProbe(rec.huffman.symbols, rec.huffman.lens, stream, availableBits);
        System.out.printf("Stage3+ auto-probe: bits-%s, invert=%s, shift=%d%n",
                pick.lsb ? "LSB" : "MSB", pick.invert, pick.shift);

        // 3) decodificador completo
        HuffmanStreamDecoder dec = HuffmanStreamDecoder.fromCanonical(
                rec.huffman.symbols, rec.huffman.lens, stream, requiredBits,
                pick.lsb, pick.invert, pick.shift);

        // 4) estatísticas
        Stats s = new Stats();
        int prev1 = -1, prev2 = -1;
        long pos = 0;

        int sym;
        while ((sym = dec.next()) >= 0) {
            s.feed(sym, pos, prev1, prev2);
            prev2 = prev1;
            prev1 = sym;
            pos++;
        }

        // 5) relatório
        report(rec, s, DEFAULT_TOPN);

        // 6) CSV opcional
        if (exportCsv) {
            try {
                if (outDir != null) Files.createDirectories(outDir);
                Path base = (outDir == null) ? Path.of(".") : outDir;

                Files.write(base.resolve("hist.csv"), s.toCsvHistogram().getBytes());
                Files.write(base.resolve("bigrams.csv"), s.toCsvBigrams().getBytes());
                Files.write(base.resolve("trigrams.csv"), s.toCsvTrigrams().getBytes());
                Files.write(base.resolve("runs.csv"), s.toCsvRuns().getBytes());

                System.out.println("CSV exportado em: " + base.toAbsolutePath());
            } catch (Exception e) {
                System.out.println("Falha ao exportar CSV: " + e);
            }
        }
    }

    // =====================================================================
    // Auto-probe (leve)

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
            this.lsb=lsb; this.invert=invert; this.shift=shift; this.score=score;
        }
    }

    // =====================================================================
    // Estatísticas

    private static final class Stats {
        // histograma de 0..255
        final long[] hist = new long[256];

        // bigrams: matriz 256x256 (pode ser esparsa, mas nosso alfabeto é pequeno)
        final long[][] bi = new long[256][256];

        // trigrams: (a<<16 | b<<8 | c) -> contagem
        final HashMap<Integer, Long> tri = new HashMap<>();

        // runs por símbolo: mapa sym -> (runLen -> contagem)
        final HashMap<Integer, HashMap<Integer, Long>> runs = new HashMap<>();
        int lastSym = -1; int runLen = 0;

        // amostras de posição para tokens “raros”
        final int[] rareSet = {63, 95, 195, 210, 249, 251};
        final HashMap<Integer, ArrayList<Long>> rarePos = new HashMap<>();

        long totalTokens = 0;

        void feed(int sym, long pos, int prev1, int prev2) {
            totalTokens++;
            if (sym >= 0 && sym < 256) hist[sym]++;

            // bigrams
            if (prev1 >= 0) bi[prev1][sym]++;

            // trigrams
            if (prev2 >= 0) {
                int key = (prev2 << 16) | (prev1 << 8) | sym;
                tri.merge(key, 1L, Long::sum);
            }

            // runs
            if (sym == lastSym) {
                runLen++;
            } else {
                if (lastSym >= 0 && runLen > 0) {
                    runs.computeIfAbsent(lastSym, k -> new HashMap<>())
                            .merge(runLen, 1L, Long::sum);
                }
                lastSym = sym;
                runLen = 1;
            }

            // raros
            for (int r : rareSet) {
                if (sym == r) {
                    rarePos.computeIfAbsent(r, k -> new ArrayList<>());
                    ArrayList<Long> lst = rarePos.get(r);
                    if (lst.size() < RARE_POS_SAMPLES) lst.add(pos);
                    break;
                }
            }
        }

        // flush final (para última run)
        void finish() {
            if (lastSym >= 0 && runLen > 0) {
                runs.computeIfAbsent(lastSym, k -> new HashMap<>())
                        .merge(runLen, 1L, Long::sum);
                lastSym = -1; runLen = 0;
            }
        }

        // ================= Report helpers =================

        void printTopHistogram(int topN) {
            int[] idx = argsortDesc(hist);
            System.out.println("Top " + topN + " símbolos:");
            for (int i = 0, shown = 0; i < idx.length && shown < topN; i++) {
                if (hist[idx[i]] == 0) break;
                System.out.printf("  %3d: %d%n", idx[i], hist[idx[i]]);
                shown++;
            }
        }

        void printRunsFor(int sym, int topN) {
            HashMap<Integer, Long> m = runs.get(sym);
            if (m == null || m.isEmpty()) { System.out.println("Sem runs para " + sym); return; }
            List<Map.Entry<Integer, Long>> list = new ArrayList<>(m.entrySet());
            list.sort((a,b) -> Long.compare(b.getValue(), a.getValue()));
            System.out.println("Top runs para símbolo " + sym + ":");
            int shown = 0;
            for (Map.Entry<Integer, Long> e : list) {
                System.out.printf("  len=%d -> %d%n", e.getKey(), e.getValue());
                if (++shown >= topN) break;
            }
        }

        void printRarePositions() {
            for (int r : rareSet) {
                ArrayList<Long> lst = rarePos.get(r);
                if (lst == null || lst.isEmpty()) continue;
                System.out.print("Posições (amostra) de " + r + ": ");
                for (int i = 0; i < lst.size(); i++) {
                    if (i > 0) System.out.print(", ");
                    System.out.print(lst.get(i));
                }
                System.out.println();
            }
        }

        void printTopTransitionsFrom(int sym, int topN) {
            long[] row = bi[sym];
            if (row == null) return;
            Integer[] idx = new Integer[256];
            for (int i=0;i<256;i++) idx[i] = i;
            Arrays.sort(idx, (a,b)-> Long.compare(row[b], row[a]));
            System.out.println("Transições a partir de " + sym + " (top " + topN + "):");
            int shown=0;
            for (int i=0;i<256 && shown<topN;i++) {
                if (row[idx[i]] == 0) break;
                System.out.printf("  %3d -> %3d : %d%n", sym, idx[i], row[idx[i]]);
                shown++;
            }
        }

        void printTopTrigrams(int topN) {
            List<Map.Entry<Integer, Long>> list = new ArrayList<>(tri.entrySet());
            list.sort((a,b) -> Long.compare(b.getValue(), a.getValue()));
            System.out.println("Top " + topN + " trigrams:");
            int shown=0;
            for (Map.Entry<Integer, Long> e : list) {
                int key = e.getKey();
                int a = (key >>> 16) & 0xFF;
                int b = (key >>> 8) & 0xFF;
                int c = key & 0xFF;
                System.out.printf("  [%3d,%3d,%3d] : %d%n", a,b,c,e.getValue());
                if (++shown >= topN) break;
            }
        }

        // ================= CSV =================

        String toCsvHistogram() {
            StringBuilder sb = new StringBuilder("symbol,count\n");
            for (int i=0;i<256;i++) if (hist[i]>0) sb.append(i).append(',').append(hist[i]).append('\n');
            return sb.toString();
        }
        String toCsvBigrams() {
            StringBuilder sb = new StringBuilder("a,b,count\n");
            for (int a=0;a<256;a++) {
                long[] row = bi[a];
                for (int b=0;b<256;b++) if (row[b]>0) sb.append(a).append(',').append(b).append(',').append(row[b]).append('\n');
            }
            return sb.toString();
        }
        String toCsvTrigrams() {
            StringBuilder sb = new StringBuilder("a,b,c,count\n");
            for (Map.Entry<Integer, Long> e : tri.entrySet()) {
                int key = e.getKey();
                int a = (key >>> 16) & 0xFF;
                int b = (key >>> 8) & 0xFF;
                int c = key & 0xFF;
                sb.append(a).append(',').append(b).append(',').append(c).append(',').append(e.getValue()).append('\n');
            }
            return sb.toString();
        }
        String toCsvRuns() {
            StringBuilder sb = new StringBuilder("symbol,runLen,count\n");
            for (Map.Entry<Integer, HashMap<Integer, Long>> e : runs.entrySet()) {
                int sym = e.getKey();
                for (Map.Entry<Integer, Long> r : e.getValue().entrySet()) {
                    sb.append(sym).append(',').append(r.getKey()).append(',').append(r.getValue()).append('\n');
                }
            }
            return sb.toString();
        }
    }

    private static int[] argsortDesc(long[] v) {
        Integer[] idx = new Integer[v.length];
        for (int i=0;i<v.length;i++) idx[i]=i;
        Arrays.sort(idx, (a,b)-> Long.compare(v[b], v[a]));
        int[] out = new int[v.length];
        for (int i=0;i<v.length;i++) out[i]=idx[i];
        return out;
    }

    private static void report(SegmentRecord rec, Stats s, int topN) {
        s.finish();

        System.out.println("=== Stage 3+ Summary ===");
        System.out.printf("Tokens decodificados: %d%n", s.totalTokens);
        System.out.printf("Huffman: N=%d, maxLen=%d, payloadStart=%d%n",
                rec.huffman.symbols.length, rec.huffman.maxLen, rec.payloadStart);
        System.out.println();

        s.printTopHistogram(topN);
        System.out.println();
        s.printTopTrigrams(Math.min(topN, 30));
        System.out.println();

        // foco em 91 (suspeita de RLE)
        System.out.println("---- Runs por símbolo ----");
        s.printRunsFor(91, topN);
        System.out.println();

        // transições a partir dos tokens mais frequentes (pegar top 3 do hist)
        int[] top = argsortDesc(s.hist);
        int shown = 0;
        for (int i=0;i<top.length && shown<3;i++) {
            if (s.hist[top[i]] == 0) break;
            s.printTopTransitionsFrom(top[i], topN);
            System.out.println();
            shown++;
        }

        // posições dos raros (amostra)
        s.printRarePositions();

        // heurísticas
        int uniques = 0;
        for (long c : s.hist) if (c>0) uniques++;
        long zero = (s.hist.length > 0) ? s.hist[0] : 0;
        long one  = (s.hist.length > 1) ? s.hist[1] : 0;
        System.out.printf("Heurísticas: zero=%d, one=%d, únicos=%d%n", zero, one, uniques);
        System.out.println("Se 91 domina e há runs longas, provável RLE (ex.: 91 = ZERO_RUN).");
        System.out.println("Tokens 63/95/195/210/249/251 sugerem marcadores de bloco/controle.");
    }
}
