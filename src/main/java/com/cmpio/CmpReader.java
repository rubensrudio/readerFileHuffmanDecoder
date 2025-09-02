package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Leitor robusto do formato CMP.
 * - Detecta endianness por plausibilidade do File Header (sem fixar VERSION/HDR_len).
 * - Lê File Header e Data Header de acordo com offsets indicados.
 * - Suporta tabela de offsets iniciando em OT_pos ou OT_pos+8 (lead-in opcional).
 * - Fornece utilidades para localizar e ler Segment Records (8192 bytes).
 */
public final class CmpReader implements AutoCloseable {

    // ==== Constantes de layout a partir da especificação ====
    private static final int FILE_HEADER_SIZE = 1024;
    private static final int SEGMENT_RECORD_SIZE = 8192; // cada segmento ocupa 8192 bytes
    // Offsets dentro do File Header (sem padding, ints seguidos por longs etc.)
    private static final int FH_DIRTY    = 0;                // int
    private static final int FH_IDENT    = FH_DIRTY + 4;     // int (pode não bater em arquivos antigos)
    private static final int FH_VERSION  = FH_IDENT + 4;     // int
    private static final int FH_OT_POS   = FH_VERSION + 4;   // long
    private static final int FH_HDR_POS  = FH_OT_POS + 8;    // long
    private static final int FH_REC_POS0 = FH_HDR_POS + 8;   // long
    private static final int FH_REC_POS1 = FH_REC_POS0 + 8;  // long
    private static final int FH_HDR_LEN  = FH_REC_POS1 + 8;  // int
    private static final int FH_REC_LEN  = FH_HDR_LEN + 4;   // int (esperado 8192)
    private static final int FH_MIN1     = FH_REC_LEN + 4;   // int
    private static final int FH_MAX1     = FH_MIN1 + 4;      // int
    private static final int FH_MIN2     = FH_MAX1 + 4;      // int
    private static final int FH_MAX2     = FH_MIN2 + 4;      // int
    private static final int FH_MIN3     = FH_MAX2 + 4;      // int
    private static final int FH_MAX3     = FH_MIN3 + 4;      // int
    private static final int FH_FAST     = FH_MAX3 + 4;      // int (não usamos)
    private static final int FH_MIDDLE   = FH_FAST + 4;      // int (não usamos)
    private static final int FH_SLOW     = FH_MIDDLE + 4;    // int (não usamos)

    // ==== Estado do reader ====
    private final Path basePath;
    private FileChannel ch;
    private ByteBuffer fileBuf;
    private ByteOrder order;

    // File Header (campos relevantes)
    private long otPos, hdrPos, recPos0, recPos1;
    private int hdrLen, recLen;
    private int min1, max1, min2, max2, min3, max3;

    // Base efetiva da tabela (OT_pos ou OT_pos+8) escolhida por plausibilidade
    private long offsetTableBase;

    public CmpReader(Path basePath) {
        this.basePath = basePath;
    }

    // ============ Abertura ============

    public void open() throws IOException {
        if (!Files.isRegularFile(basePath)) {
            throw new IOException("CMP file not found: " + basePath);
        }
        this.ch = FileChannel.open(basePath, StandardOpenOption.READ);
        long size = ch.size();
        if (size < FILE_HEADER_SIZE) {
            throw new IOException("File too small to be CMP: " + size + " bytes");
        }
        this.fileBuf = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);

        // Detecta endianness por plausibilidade do FileHeader
        HeaderCandidate be = readHeaderCandidate(ByteOrder.BIG_ENDIAN, size);
        HeaderCandidate le = readHeaderCandidate(ByteOrder.LITTLE_ENDIAN, size);

        int beScore = be.score(size);
        int leScore = le.score(size);

        if (beScore <= 0 && leScore <= 0) {
            throw new IOException("Cannot detect byte order: FileHeader not plausible in BE nor LE.\n" +
                    "BE=" + be + "  LE=" + le);
        }
        HeaderCandidate winner = (beScore >= leScore) ? be : le;
        this.order = winner.order;

        // Copia campos vencedores
        this.otPos   = winner.otPos;
        this.hdrPos  = winner.hdrPos;
        this.recPos0 = winner.recPos0;
        this.recPos1 = winner.recPos1;
        this.hdrLen  = winner.hdrLen;
        this.recLen  = winner.recLen;
        this.min1    = winner.min1; this.max1 = winner.max1;
        this.min2    = winner.min2; this.max2 = winner.max2;
        this.min3    = winner.min3; this.max3 = winner.max3;

        // Sanidade adicional: HDR deve estar entre OT e REC0.
        if (!(hdrPos > otPos && hdrPos < recPos0)) {
            throw new IOException(String.format(
                    "Inconsistent header positions: OT_pos=%d, HDR_pos=%d, REC_pos_0=%d",
                    otPos, hdrPos, recPos0));
        }

        // Decide a base real da tabela de offsets: OT_pos ou OT_pos+8
        this.offsetTableBase = chooseOffsetTableBase(size);

        // Opcional: se recLen == 0 em alguns arquivos antigos, normalizamos para 8192
        if (this.recLen <= 0) this.recLen = SEGMENT_RECORD_SIZE;
    }

    private HeaderCandidate readHeaderCandidate(ByteOrder ord, long fileSize) {
        ByteBuffer bb = fileBuf.duplicate().order(ord);
        HeaderCandidate h = new HeaderCandidate();
        h.order   = ord;
        h.dirty   = getInt(bb, FH_DIRTY);
        h.ident   = getInt(bb, FH_IDENT);
        h.version = getInt(bb, FH_VERSION);
        h.otPos   = getLong(bb, FH_OT_POS);
        h.hdrPos  = getLong(bb, FH_HDR_POS);
        h.recPos0 = getLong(bb, FH_REC_POS0);
        h.recPos1 = getLong(bb, FH_REC_POS1);
        h.hdrLen  = getInt(bb, FH_HDR_LEN);
        h.recLen  = getInt(bb, FH_REC_LEN);
        h.min1    = getInt(bb, FH_MIN1); h.max1 = getInt(bb, FH_MAX1);
        h.min2    = getInt(bb, FH_MIN2); h.max2 = getInt(bb, FH_MAX2);
        h.min3    = getInt(bb, FH_MIN3); h.max3 = getInt(bb, FH_MAX3);
        h.fileSize = fileSize;
        return h;
    }

    private static int getInt(ByteBuffer bb, int off) {
        if (off < 0 || off + 4 > bb.limit()) return 0;
        return bb.getInt(off);
    }
    private static long getLong(ByteBuffer bb, int off) {
        if (off < 0 || off + 8 > bb.limit()) return 0L;
        return bb.getLong(off);
    }

    private static final class HeaderCandidate {
        ByteOrder order;
        long fileSize;
        int dirty, ident, version;
        long otPos, hdrPos, recPos0, recPos1;
        int hdrLen, recLen;
        int min1, max1, min2, max2, min3, max3;

        long count() {
            long nx = (long) (max1 - min1 + 1);
            long ny = (long) (max2 - min2 + 1);
            long nz = (long) (max3 - min3 + 1);
            if (nx <= 0 || ny <= 0 || nz <= 0) return -1;
            return nx * ny * nz;
        }
        int score(long size) {
            int s = 0;
            // posições dentro do arquivo
            if (otPos >= FILE_HEADER_SIZE && otPos < size) s += 2;
            if (hdrPos > otPos && hdrPos < size) s += 2;
            if (recPos0 >= hdrPos && recPos0 < size) s += 2;
            // REC_pos_1 pode exceder o tamanho do primeiro extent (multi-arquivo),
            // então permitimos uma folga grande.
            if (recPos1 > recPos0 && recPos1 <= Math.max(size, recPos0) + (64L << 20)) s += 2;

            // Tamanhos plausíveis
            if (recLen == SEGMENT_RECORD_SIZE) s += 2;
            if (hdrLen >= 1024 && hdrLen <= (1 << 16)) s += 1;

            // Contagem plausível
            long n = count();
            if (n > 0 && n < 1_000_000_000L) s += 2;

            // Coerência com REC_pos_0 ~ OT_pos + [0|8] + n*8 + hdrLen
            if (n > 0) {
                long t0 = otPos + n * 8L + hdrLen;
                long t1 = otPos + 8L + n * 8L + hdrLen; // com lead-in
                if (Math.abs(recPos0 - t0) <= 32 || Math.abs(recPos0 - t1) <= 32) s += 3;
            }
            return s;
        }
        @Override public String toString() {
            return "Header{ver=" + version +
                    ", OT_pos=" + otPos +
                    ", HDR_pos=" + hdrPos +
                    ", REC_pos_0=" + recPos0 +
                    ", REC_pos_1=" + recPos1 +
                    ", HDR_len=" + hdrLen +
                    ", REC_len=" + recLen + "}";
        }
    }

    private long chooseOffsetTableBase(long fileSize) {
        long n = getSegmentsCount();
        long base0 = otPos;
        long base8 = otPos + 8;

        int score0 = probeOffsetBase(base0, n, fileSize);
        int score8 = probeOffsetBase(base8, n, fileSize);

        // escolhe a que tiver mais hits plausíveis
        long base = (score8 > score0) ? base8 : base0;

        // como sanity-check final, garanta que recPos0 esteja depois da tabela escolhida + hdrLen
        long expectedMin = base + n * 8L + hdrLen;
        if (recPos0 < expectedMin - 64) {
            // se a escolha ficou inconsistente, volte para a outra
            base = (base == base8) ? base0 : base8;
        }
        return base;
    }

    // Verifica as primeiras/últimas 16 entradas dessa base: quantas são (0) ou offsets válidos (< recPos1).
    private int probeOffsetBase(long base, long n, long fileSize) {
        int hits = 0;
        ByteBuffer bb = fileBuf.duplicate().order(order);

        // amostra do início
        int take = (int) Math.min(16, n);
        for (int i = 0; i < take; i++) {
            long pos = base + i * 8L;
            if (pos < 0 || pos + 8 > fileSize) break;
            bb.position((int) pos);
            long off = bb.getLong();
            if (off == 0 || (off > 0 && off < recPos1)) hits++;
        }

        // amostra do fim
        for (int i = 0; i < take; i++) {
            long pos = base + (n - 1 - i) * 8L;
            if (pos < 0 || pos + 8 > fileSize) break;
            bb.position((int) pos);
            long off = bb.getLong();
            if (off == 0 || (off > 0 && off < recPos1)) hits++;
        }
        return hits;
    }

    // ============ API pública usada pelo AnalyzeSegment / MainDemo ============

    public Path getBasePath() { return basePath; }
    public ByteOrder getByteOrder() { return order; }
    public long getOtPos() { return otPos; }
    public long getHdrPos() { return hdrPos; }
    public long getRecPos0() { return recPos0; }
    public long getRecPos1() { return recPos1; }
    public int getHdrLen() { return hdrLen; }
    public int getRecLen() { return recLen; }
    public int getMin1() { return min1; }
    public int getMax1() { return max1; }
    public int getMin2() { return min2; }
    public int getMax2() { return max2; }
    public int getMin3() { return min3; }
    public int getMax3() { return max3; }

    public long getSegmentsCount() {
        long nx = (long) (max1 - min1 + 1);
        long ny = (long) (max2 - min2 + 1);
        long nz = (long) (max3 - min3 + 1);
        return nx * ny * nz;
    }

    /** Procura o primeiro segmento com offset > 0. */
    public int[] findFirstNonEmpty() {
        long n = getSegmentsCount();
        if (n <= 0) return null;

        int nx = (max1 - min1 + 1);
        int ny = (max2 - min2 + 1);

        ByteBuffer bb = fileBuf.duplicate().order(order);
        for (long idx = 0; idx < n; idx++) {
            long pos = offsetTableBase + idx * 8L;
            if (pos < 0 || pos + 8 > fileBuf.limit()) break;
            bb.position((int) pos);
            long off = bb.getLong();
            if (off > 0) {
                long i3 = idx / (nx * (long) ny);
                long rem = idx - i3 * (long) nx * (long) ny;
                long i2 = rem / nx;
                long i1 = rem - i2 * (long) nx;
                return new int[]{ (int)(min1 + i1), (int)(min2 + i2), (int)(min3 + i3) };
            }
        }
        return null;
    }

    /** Lê e interpreta o registro (8192 bytes) do segmento (seg1,seg2,seg3). */
    public SegmentRecord readSegmentRecord(int seg1, int seg2, int seg3) {
        if (seg1 < min1 || seg1 > max1 || seg2 < min2 || seg2 > max2 || seg3 < min3 || seg3 > max3) {
            throw new IllegalArgumentException(
                    String.format("Segment out of bounds: (%d,%d,%d) not in [%d..%d]x[%d..%d]x[%d..%d]",
                            seg1, seg2, seg3, min1, max1, min2, max2, min3, max3));
        }
        long nx = (long) (max1 - min1 + 1);
        long ny = (long) (max2 - min2 + 1);
        long i1 = seg1 - (long) min1;
        long i2 = seg2 - (long) min2;
        long i3 = seg3 - (long) min3;
        long idx = i3 * nx * ny + i2 * nx + i1; // ordem 3,2,1 (lento->rápido). :contentReference[oaicite:7]{index=7}

        long entryPos = offsetTableBase + idx * 8L;
        if (entryPos < 0 || entryPos + 8 > fileBuf.limit()) {
            throw new IllegalStateException("Offset-table entry out of bounds: " + entryPos);
        }
        ByteBuffer bb = fileBuf.duplicate().order(order);
        bb.position((int) entryPos);
        long recStart = bb.getLong();

        if (recStart <= 0) {
            throw new IllegalStateException(String.format("Null/empty segment at (%d,%d,%d)", seg1, seg2, seg3));
        }
        if (recStart + SEGMENT_RECORD_SIZE > fileBuf.limit()) {
            throw new IllegalStateException("Segment exceeds mapped file: start=" + recStart);
        }

        // Delega o parse para o SegmentRecord (ele lida com metadados + huffman + payload). :contentReference[oaicite:8]{index=8}
        return SegmentRecord.parse(fileBuf, (int) recStart, order);
    }

    @Override public void close() throws IOException {
        if (ch != null && ch.isOpen()) ch.close();
    }
}
