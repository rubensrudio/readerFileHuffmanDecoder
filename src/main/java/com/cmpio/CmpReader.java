package com.cmpio;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Leitor do arquivo CMP. Suporta registros fixos (8192) e variáveis (REC_len==0). */
public final class CmpReader implements AutoCloseable {
    private final Path baseFile;
    private final ByteOrder order;
    private final FileChannel file;
    private final MultiExtentInput input;
    private final CmpFileHeader fileHeader;
    private final CmpDataHeader dataHeader;
    private final OffsetTable offsetTable;

    private CmpReader(Path baseFile, ByteOrder order, FileChannel file, MultiExtentInput input,
                      CmpFileHeader fh, CmpDataHeader dh, OffsetTable ot) {
        this.baseFile   = baseFile;
        this.order      = order;
        this.file       = file;
        this.input      = input;
        this.fileHeader = fh;
        this.dataHeader = dh;
        this.offsetTable= ot;
    }

    public static CmpReader open(Path baseFile) throws IOException {
        final ByteOrder order = ByteOrderUtils.detectFileHeaderByteOrder(baseFile);
        final FileChannel ch = FileChannel.open(baseFile, StandardOpenOption.READ);

        final CmpFileHeader fh = CmpFileHeader.readFrom(ch, order);
        // Abrimos todos os extents até REC_pos_1; offsets são absolutos (ver §3.2) :contentReference[oaicite:3]{index=3}
        final MultiExtentInput in = new MultiExtentInput(baseFile, fh.REC_pos_1);
        final CmpDataHeader dh = CmpDataHeader.readFrom(ch, fh.HDR_pos, fh.HDR_len, order);

        // Offset table (valores absolutos para o início do registro do segmento) :contentReference[oaicite:4]{index=4}
        final OffsetTable ot = new OffsetTable(fh.MIN_1, fh.MAX_1, fh.MIN_2, fh.MAX_2, fh.MIN_3, fh.MAX_3);
        final int entries = ot.size();
        final ByteBuffer buf = ByteBuffer.allocate(entries * 8).order(order);
        int n = ch.read(buf, fh.OT_pos);
        if (n < buf.capacity())
            throw new IOException("Offset table truncated: " + n + "/" + buf.capacity());
        buf.flip();
        for (int i = 0; i < entries; i++) {
            long off = buf.getLong();
            if (off > 0 && off >= fh.REC_pos_1)
                throw new IOException("Offset out of range at index " + i + ": " + off);
            ot.setRaw(i, off);
        }

        return new CmpReader(baseFile, order, ch, in, fh, dh, ot);
    }

    @Override public void close() throws IOException { file.close(); input.close(); }

    public CmpFileHeader fileHeader()  { return fileHeader; }
    public CmpDataHeader dataHeader()  { return dataHeader; }
    public OffsetTable   offsetTable() { return offsetTable; }
    public ByteOrder     byteOrder()   { return order; }

    /**
     * Lê um segmento.
     * Se REC_len>0 → registro fixo (normalmente 8192). Se REC_len==0 → registro variável: faz probe
     * para obter N e a soma dos bits e, então, relê o tamanho exato necessário.
     */
    public SegmentRecord readSegmentRecord(int seg1, int seg2, int seg3) throws IOException {
        final int idx = offsetTable.linearIndex(seg1, seg2, seg3);
        if (idx < 0) return null;

        final long off = offsetTable.getRaw(idx);
        if (off <= 0) return null;

        if (fileHeader.REC_len > 0) {
            // Caminho de registro fixo (spec clássica) :contentReference[oaicite:5]{index=5}
            final int recordLen = fileHeader.REC_len;
            final ByteBuffer rec = ByteBuffer.allocate(recordLen).order(order);
            input.readFully(off, rec);
            rec.flip();
            return SegmentRecord.parse(rec, order); // valida bits ≤ espaço útil
        }

        // REC_len == 0 → registro variável. Fazemos 2 passos: probe → read exact.
        // 1) ler no mínimo 8192 para conseguir parsear metadata+tabela
        final int firstRead = 8192;
        ByteBuffer tmp = ByteBuffer.allocate(firstRead).order(order);
        input.readFully(off, tmp);
        tmp.flip();

        SegmentRecord.Probe p = SegmentRecord.probe(tmp, order);
        // neededLen = 264 + (2+2N) + ceil(requiredBits/8)
        int neededLen = p.payloadStart + (int)((p.requiredBits + 7L) >>> 3);

        if (neededLen <= firstRead) {
            // já cabe no primeiro buffer
            tmp.position(0).limit(neededLen);
            return SegmentRecord.parse(tmp, order);
        }

        if (neededLen > Integer.MAX_VALUE)
            throw new IOException("Segment too large to buffer: " + neededLen + " bytes");

        ByteBuffer full = ByteBuffer.allocate(neededLen).order(order);
        input.readFully(off, full);
        full.flip();
        return SegmentRecord.parse(full, order);
    }

    /** Procura o primeiro segmento não-vazio (offset > 0). */
    public int[] findFirstNonEmpty() {
        final int n1 = offsetTable.n1(), n2 = offsetTable.n2(), n3 = offsetTable.n3();
        final int min1 = offsetTable.min1(), min2 = offsetTable.min2(), min3 = offsetTable.min3();
        int idx = 0;
        for (int k = 0; k < n3; k++) {
            for (int j = 0; j < n2; j++) {
                for (int i = 0; i < n1; i++, idx++) {
                    if (offsetTable.getRaw(idx) > 0)
                        return new int[] { min1 + i, min2 + j, min3 + k };
                }
            }
        }
        return null;
    }
}
