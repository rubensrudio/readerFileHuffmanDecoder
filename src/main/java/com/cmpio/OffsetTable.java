package com.cmpio;

public final class OffsetTable {
    private final int n1, n2, n3;
    private final int min1, min2, min3;
    private final long[] offsets;

    public OffsetTable(int min1, int max1, int min2, int max2, int min3, int max3) {
        this.min1 = min1;
        this.min2 = min2;
        this.min3 = min3;
        this.n1 = max1 - min1 + 1;
        this.n2 = max2 - min2 + 1;
        this.n3 = max3 - min3 + 1;
        this.offsets = new long[(int) ((long) n1 * n2 * n3)];
    }

    public int size() { return offsets.length; }

    public long getOffset(int seg1, int seg2, int seg3) {
        int idx = linearIndex(seg1, seg2, seg3);
        if (idx < 0 || idx >= offsets.length) return 0L;
        return offsets[idx];
    }

    /** Índice linear (ordem: dim3 mais lenta, depois dim2, depois dim1). */
    public int linearIndex(int seg1, int seg2, int seg3) {
        int i1 = seg1 - min1;
        int i2 = seg2 - min2;
        int i3 = seg3 - min3;
        return i3 * (n1 * n2) + i2 * n1 + i1;
    }

    public int n1() { return n1; }
    public int n2() { return n2; }
    public int n3() { return n3; }
    public int min1() { return min1; }
    public int min2() { return min2; }
    public int min3() { return min3; }

    // package-private (leitor preenche/consulta bruto)
    void setRaw(int index, long val) { this.offsets[index] = val; }
    long getRaw(int index) { return this.offsets[index]; }

    /** Retorna o MENOR offset estritamente maior que {@code off} entre TODAS as entradas; -1 se não houver. */
    public long nextHigherOffset(long off) {
        long best = Long.MAX_VALUE;
        for (long o : offsets) {
            if (o > off && o > 0 && o < best) best = o;
        }
        return (best == Long.MAX_VALUE) ? -1L : best;
    }
}
