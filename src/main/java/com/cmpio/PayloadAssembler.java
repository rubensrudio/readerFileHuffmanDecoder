package com.cmpio;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Monta o bitstream de um segmento concatenando:
 *   - o payload do record atual (a partir de payloadStart);
 *   - depois, bytes dos PRÓXIMOS records, SEM PARSE, sempre a partir do byte 0,
 *     até cobrir requiredBits.
 *
 * Observação: este comportamento foi validado empiricamente para o seu arquivo,
 * onde o bitstream do segmento continua no início do próximo record.
 */
public final class PayloadAssembler {

    public static final class Assembled {
        public final byte[] bytes;
        public final long   bits;
        public Assembled(byte[] bytes, long bits) {
            this.bytes = bytes;
            this.bits  = bits;
        }
    }

    private PayloadAssembler() {}

    /**
     * @param fileBuf       buffer mapeado do arquivo CMP (não nulo)
     * @param firstRecStart offset (em bytes) do record atual dentro do arquivo
     * @param order         ordem dos metadados (mantida por compatibilidade; não usada aqui)
     * @param first         record já parseado (contém payloadSlice e metadata)
     * @param requiredBits  total de bits exigidos pelo metadata (sumBits)
     */
    public static Assembled assemble(ByteBuffer fileBuf,
                                     int firstRecStart,
                                     ByteOrder order,
                                     SegmentRecord first,
                                     long requiredBits) {

        if (fileBuf == null) {
            throw new IllegalStateException("PayloadAssembler: fileBuf é null.");
        }
        if (first == null || first.payloadSlice == null) {
            throw new IllegalStateException("PayloadAssembler: SegmentRecord/payloadSlice inválido.");
        }

        final int requiredBytes = (int) ((requiredBits + 7) >>> 3);
        final ByteArrayOutputStream out = new ByteArrayOutputStream(requiredBytes + 16);

        // 1) Copia o payload do record atual (a partir de payloadStart)
        {
            ByteBuffer p = first.payloadSlice.duplicate();
            byte[] buf = new byte[p.remaining()];
            p.get(buf);
            out.write(buf, 0, buf.length);
        }

        // 2) Copia dos PRÓXIMOS records, SEM PARSE, sempre do byte 0..8191
        int nextRecStart = firstRecStart + SegmentRecord.RECORD_SIZE;
        while (out.size() < requiredBytes &&
                nextRecStart + SegmentRecord.RECORD_SIZE <= fileBuf.capacity()) {

            ByteBuffer full = slice(fileBuf, nextRecStart, SegmentRecord.RECORD_SIZE);
            byte[] buf = new byte[full.remaining()];
            full.get(buf);
            out.write(buf, 0, buf.length);

            nextRecStart += SegmentRecord.RECORD_SIZE;
        }

        // 3) Trunca para exatamente requiredBytes
        byte[] all = out.toByteArray();
        if (all.length > requiredBytes) {
            byte[] cut = new byte[requiredBytes];
            System.arraycopy(all, 0, cut, 0, requiredBytes);
            all = cut;
        }

        return new Assembled(all, requiredBits);
    }

    private static ByteBuffer slice(ByteBuffer src, int pos, int len) {
        if (src == null) {
            throw new IllegalStateException("slice: src(ByteBuffer) é null.");
        }
        if (pos < 0 || len < 0 || pos + len > src.capacity()) {
            throw new IllegalArgumentException(String.format(
                    "slice fora dos limites: pos=%d len=%d cap=%d", pos, len, src.capacity()));
        }
        ByteBuffer d = src.duplicate();
        d.position(pos);
        d.limit(pos + len);
        return d.slice();
    }
}
