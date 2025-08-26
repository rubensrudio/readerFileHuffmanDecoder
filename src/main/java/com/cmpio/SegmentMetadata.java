
package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class SegmentMetadata {
    public float minDelta;
    public float maxDelta;
    public int[] quantizedDeltas = new int[64]; // uint16 -> int
    public int[] blockSizesBits = new int[64];  // uint16 -> int

    public static final int SIZE = 264;

    public static SegmentMetadata parse(ByteBuffer buf, ByteOrder order) {
        buf.order(order);
        SegmentMetadata m = new SegmentMetadata();
        m.minDelta = buf.getFloat();
        m.maxDelta = buf.getFloat();
        for (int i = 0; i < 64; i++) {
            int v = Short.toUnsignedInt(buf.getShort());
            m.quantizedDeltas[i] = v;
        }
        for (int i = 0; i < 64; i++) {
            int v = Short.toUnsignedInt(buf.getShort());
            m.blockSizesBits[i] = v;
        }
        return m;
    }
}
