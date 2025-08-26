
package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/** 4120-byte Data Header. */
public final class CmpDataHeader {
    public static final int SIZE = 4120;
    public int IDENT;
    public int VERSION;
    public float NULLVALUE;
    public int HPOS, HLEN; // unused
    public int MIN_1, MAX_1, MIN_2, MAX_2, MIN_3, MAX_3;
    public int AMIN_1, AMAX_1, AMIN_2, AMAX_2, AMIN_3, AMAX_3;
    public int CMP_METHOD;
    public int DISTORTION;
    public double[] HUFFMAN = new double[256];
    public double[] METERED = new double[256];

    public static CmpDataHeader readFrom(FileChannel ch, long hdrPos, long hdrLen, ByteOrder order) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate((int) hdrLen).order(order);
        int n = ch.read(buf, hdrPos);
        if (n < hdrLen) throw new IOException("Failed to read CMP Data Header: read="+n);
        buf.flip();

        CmpDataHeader h = new CmpDataHeader();
        h.IDENT = buf.getInt();
        h.VERSION = buf.getInt();
        h.CMP_METHOD = buf.getInt();
        h.HPOS = buf.getInt();
        h.HLEN = buf.getInt();
        h.DISTORTION = buf.getInt();
        h.NULLVALUE = buf.getFloat();
        h.MIN_1 = buf.getInt();
        h.MAX_1 = buf.getInt();
        h.MIN_2 = buf.getInt();
        h.MAX_2 = buf.getInt();
        h.MIN_3 = buf.getInt();
        h.MAX_3 = buf.getInt();
        h.AMIN_1 = buf.getInt();
        h.AMAX_1 = buf.getInt();
        h.AMIN_2 = buf.getInt();
        h.AMAX_2 = buf.getInt();
        h.AMIN_3 = buf.getInt();
        h.AMAX_3 = buf.getInt();
        for(int i=0; i<256; i++) h.HUFFMAN[i] = buf.getDouble();
        for(int i=0; i<256; i++) h.METERED[i] = buf.getDouble();

        /*byte[] huffman = new byte[2048];
        buf.get(huffman);
        h.HUFFMAN = huffman;

        byte[] metered = new byte[2048];
        buf.get(metered);
        h.METERED = metered;*/

        return h;
    }

    @Override public String toString() {
        return "CmpDataHeader{" +
                "IDENT=" + IDENT +
                ", VER=" + VERSION +
                ", NULLVALUE=" + NULLVALUE +
                ", MIN_1..MAX_1=" + MIN_1 + ".." + MAX_1 +
                ", MIN_2..MAX_2=" + MIN_2 + ".." + MAX_2 +
                ", MIN_3..MAX_3=" + MIN_3 + ".." + MAX_3 +
                ", AMIN_1..AMAX_1=" + AMIN_1 + ".." + AMAX_1 +
                ", AMIN_2..AMAX_2=" + AMIN_2 + ".." + AMAX_2 +
                ", AMIN_3..AMAX_3=" + AMIN_3 + ".." + AMAX_3 +
                ", CMP_METHOD=" + CMP_METHOD +
                ", DISTORTION=" + DISTORTION +
                '}';
    }
}
