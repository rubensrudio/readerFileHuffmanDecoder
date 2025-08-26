
package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static java.nio.charset.StandardCharsets.US_ASCII;

/** 1024-byte File Header. */
public final class CmpFileHeader {
    public int DIRTY;
    public int IDENT;
    public int VERSION;
    public long OT_pos;
    public long HDR_pos;
    public long REC_pos_0;
    public long REC_pos_1;
    public int  HDR_len;
    public int  REC_len;
    public int MIN_1, MAX_1, MIN_2, MAX_2, MIN_3, MAX_3;
    public int FAST, MIDDLE, SLOW;

    public static final int SIZE = 1024;

    public static CmpFileHeader readFrom(FileChannel ch, ByteOrder order) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(SIZE).order(order);
        int n = ch.read(buf, 0);
        if (n < SIZE) throw new IOException("Failed to read CMP File Header: read="+n);
        buf.flip();

        CmpFileHeader h = new CmpFileHeader();
        h.DIRTY = buf.getInt();
        h.IDENT = buf.getInt();
        h.VERSION = buf.getInt();
        h.OT_pos = buf.getLong();
        h.HDR_pos = buf.getLong();
        h.REC_pos_0 = buf.getLong();
        h.REC_pos_1 = buf.getLong();
        h.HDR_len = buf.getInt();
        h.REC_len = buf.getInt();
        h.MIN_1 = buf.getInt();
        h.MAX_1 = buf.getInt();
        h.MIN_2 = buf.getInt();
        h.MAX_2 = buf.getInt();
        h.MIN_3 = buf.getInt();
        h.MAX_3 = buf.getInt();
        h.FAST = buf.getInt();
        h.MIDDLE = buf.getInt();
        h.SLOW = buf.getInt();

        // basic validation
        if (h.IDENT != ByteOrderUtils.FILE_IDENT) {
            throw new IOException("Bad IDENT in File Header: " + h.IDENT);
        }
        if (h.HDR_len != 8192) {
            throw new IOException("Unexpected HDR_len (segment record size): " + h.HDR_len);
        }
        return h;
    }

    public int segCount1() { return MAX_1 - MIN_1 + 1; }
    public int segCount2() { return MAX_2 - MIN_2 + 1; }
    public int segCount3() { return MAX_3 - MIN_3 + 1; }
    public long totalSegments() { return (long) segCount1() * segCount2() * segCount3(); }

    @Override public String toString() {
        return "CmpFileHeader{" +
                "VER=" + VERSION +
                ", OT_pos=" + OT_pos +
                ", HDR_pos=" + HDR_pos +
                ", REC_pos_0=" + REC_pos_0 +
                ", REC_pos_1=" + REC_pos_1 +
                ", HDR_len=" + HDR_len +
                ", REC_len=" + REC_len +
                ", MIN_1..MAX_1=" + MIN_1 + ".." + MAX_1 +
                ", MIN_2..MAX_2=" + MIN_2 + ".." + MAX_2 +
                ", MIN_3..MAX_3=" + MIN_3 + ".." + MAX_3 +
                ", FAST=" + FAST + ", MIDDLE=" + MIDDLE + ", SLOW=" + SLOW +
                '}';
    }
}
