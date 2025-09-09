package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Leitor robusto do formato CMP.
 * - Detecta endianness por plausibilidade do File Header (sem fixar VERSION/HDR_len).
 * - Lê File Header e Data Header de acordo com offsets indicados.
 * - Suporta tabela de offsets iniciando em OT_pos ou OT_pos+8 (lead-in opcional).
 * - Fornece utilidades para localizar e ler Segment Records (8192 bytes).
 *
 * API compatível com AnalyzeSegment/MainDemo/Stage2Analyzer:
 *   - CmpReader.open(String)
 *   - getOrder(), getFileBuffer()
 *   - getRecPos0() (int)  e getRecPos0Long() (long)
 *   - closeQuietly()
 */
public final class CmpReader implements AutoCloseable {

    // ==== Constantes de layout a partir da especificação ====
    private static final int FILE_HEADER_SIZE = 1024;
    public  static final int SEGMENT_RECORD_SIZE = 8192; // cada segmento ocupa 8192 bytes

    // ----- Configuração de sondagem (pode virar parâmetros/flags) -----
    private static final int DEFAULT_MAX_S1 = Integer.getInteger("cmp.s1.max", 64);
    private static final int DEFAULT_MAX_S2 = Integer.getInteger("cmp.s2.max", 64);
    private static final int DEFAULT_MAX_S3 = Integer.getInteger("cmp.s3.max", 64);

    // Encerrar um eixo quando X fatias consecutivas vierem 100% vazias
    private static final int EMPTY_STRIPES_TO_STOP_S3 = Integer.getInteger("cmp.stop.s3.after.empty", 4);
    private static final int EMPTY_STRIPES_TO_STOP_S2 = Integer.getInteger("cmp.stop.s2.after.empty", 3);
    private static final int EMPTY_STRIPES_TO_STOP_S1 = Integer.getInteger("cmp.stop.s1.after.empty", 2);

    // Offsets dentro do File Header (ints/longs encadeados)
    private static final int FH_DIRTY    = 0;                // int
    private static final int FH_IDENT    = FH_DIRTY + 4;     // int
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

    /* ========= Fábrica compatível com AnalyzeSegment ========= */
    public static CmpReader open(String path) throws IOException {
        CmpReader r = new CmpReader(Paths.get(path));
        r.open();
        return r;
    }

    /* ========= Construtor/abertura ========= */
    public CmpReader(Path basePath) {
        this.basePath = basePath;
    }

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

        // Normaliza recLen se necessário
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

        long base = (score8 > score0) ? base8 : base0;

        // sanity: recPos0 deve estar depois da tabela escolhida + hdrLen
        long expectedMin = base + n * 8L + hdrLen;
        if (recPos0 < expectedMin - 64) {
            base = (base == base8) ? base0 : base8;
        }
        return base;
    }

    // Verifica as primeiras/últimas entradas: quantas são (0) ou offsets válidos (< recPos1).
    private int probeOffsetBase(long base, long n, long fileSize) {
        int hits = 0;
        ByteBuffer bb = fileBuf.duplicate().order(order);

        int take = (int) Math.min(16, n);
        // início
        for (int i = 0; i < take; i++) {
            long pos = base + i * 8L;
            if (pos < 0 || pos + 8 > fileSize) break;
            bb.position((int) pos);
            long off = bb.getLong();
            if (off == 0 || (off > 0 && off < recPos1)) hits++;
        }
        // fim
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

    /** Compatível com AnalyzeSegment */
    public ByteOrder getOrder()     { return order; }
    public ByteOrder getByteOrder() { return order; }

    // Sempre retorna um ByteBuffer válido; se ainda não estiver mapeado, mapeia agora.
    public ByteBuffer getFileBuffer() {
        if (fileBuf == null) {
            ensureMapped();
        }
        return fileBuf.duplicate();
    }

    // Versão que falha com mensagem clara (use esta no AnalyzeSegment)
    public ByteBuffer requireFileBuffer() {
        if (fileBuf == null) {
            ensureMapped();
            if (fileBuf == null) {
                throw new IllegalStateException("CmpReader: fileBuf ainda é null após tentar mapear. " +
                        "Verifique se CmpReader.open(path) foi chamado e se o arquivo existe/é legível.");
            }
        }
        return fileBuf.duplicate();
    }

    // Re-mapeia se necessário
    private void ensureMapped() {
        try {
            if (ch != null && ch.isOpen() && fileBuf == null) {
                long size = ch.size();
                fileBuf = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);
            }
        } catch (Exception e) {
            throw new IllegalStateException("CmpReader.ensureMapped: falha ao mapear arquivo", e);
        }
    }

    // (opcional) utilitário
    public boolean isOpen() {
        return ch != null && ch.isOpen();
    }

    public long  getOtPos()  { return otPos; }
    public long  getHdrPos() { return hdrPos; }

    /** AnalyzeSegment espera int; fornecemos ambos. */
    public int   getRecPos0()     { return (int) recPos0; }
    public long  getRecPos0Long() { return recPos0; }

    public long  getRecPos1() { return recPos1; }
    public int   getHdrLen()  { return hdrLen; }
    public int   getRecLen()  { return recLen; }
    public int   getMin1()    { return min1; }
    public int   getMax1()    { return max1; }
    public int   getMin2()    { return min2; }
    public int   getMax2()    { return max2; }
    public int   getMin3()    { return min3; }
    public int   getMax3()    { return max3; }

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
        long idx = i3 * nx * ny + i2 * nx + i1;

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

        return SegmentRecord.parse(fileBuf, (int) recStart, order);
    }

    /* Utilitário para pegar os bytes de um record arbitrário (útil em montagens). */
    public ByteBuffer sliceRecordBytes(long recStart) {
        ByteBuffer d = fileBuf.duplicate();
        d.position((int) recStart);
        d.limit((int) recStart + SEGMENT_RECORD_SIZE);
        return d.slice();
    }

    @Override public void close() throws IOException {
        if (ch != null && ch.isOpen()) ch.close();
    }

    public void closeQuietly() {
        try { close(); } catch (Exception ignore) {}
    }

    /** Leitura “segura”: devolve null se inválido/fora de range/erro. */
    private SegmentRecord safeReadSegmentRecord(int s1, int s2, int s3) {
        try {
            SegmentRecord rec = readSegmentRecord(s1, s2, s3);
            if (rec == null || rec.md == null) return null;
            if (rec.md.totalBits <= 0 || rec.md.payloadBytes <= 0) return null;
            // Também valida posição de payload se quiser:
            if (rec.md.payloadStartByte <= 0) return null;
            return rec;
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Enumera segmentos não-vazios por sondagem, com limites e paradas inteligentes.
     * Ajuste os limites via -Dcmp.s{1,2,3}.max=...
     */
    public List<int[]> listNonEmptySegments() {
        List<int[]> out = new ArrayList<>();
        final int S1_MAX = DEFAULT_MAX_S1;
        final int S2_MAX = DEFAULT_MAX_S2;
        final int S3_MAX = DEFAULT_MAX_S3;

        int emptyS1Stripes = 0;

        for (int s1 = 0; s1 < S1_MAX; s1++) {
            int foundInS1 = 0;
            int emptyS2Stripes = 0;

            for (int s2 = 0; s2 < S2_MAX; s2++) {
                int foundInS2 = 0;
                int emptyS3Stripes = 0;

                for (int s3 = 0; s3 < S3_MAX; s3++) {
                    SegmentRecord rec = safeReadSegmentRecord(s1, s2, s3);
                    if (rec != null) {
                        out.add(new int[]{s1, s2, s3});
                        foundInS1++;
                        foundInS2++;
                        emptyS3Stripes = 0; // reset — achamos algo nesta “faixa” s3
                    } else {
                        emptyS3Stripes++;
                        // muitas posições s3 consecutivas vazias → provável fim do eixo s3 para este (s1,s2)
                        if (emptyS3Stripes >= EMPTY_STRIPES_TO_STOP_S3) break;
                    }
                }

                if (foundInS2 == 0) {
                    emptyS2Stripes++;
                    // nenhuma posição s3 com dados para este s2, repetido várias vezes → encerra s2 cedo
                    if (emptyS2Stripes >= EMPTY_STRIPES_TO_STOP_S2) break;
                } else {
                    emptyS2Stripes = 0; // reset — houve dados em algum s3 deste s2
                }
            }

            if (foundInS1 == 0) {
                emptyS1Stripes++;
                // nenhum s2/s3 com dados para este s1, repetido algumas vezes → encerra s1 cedo
                if (emptyS1Stripes >= EMPTY_STRIPES_TO_STOP_S1) break;
            } else {
                emptyS1Stripes = 0;
            }
        }

        return out;
    }

}
