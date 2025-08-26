
package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class HuffmanTable {
    public int symbolCount;      // uint16
    public byte[] codeLengths;   // N bytes
    public byte[] symbols;       // N bytes

    public static HuffmanTable parse(ByteBuffer buf, ByteOrder order) {
        buf.order(order);
        HuffmanTable t = new HuffmanTable();
        int sc = Short.toUnsignedInt(buf.getShort());
        t.symbolCount = sc;
        t.codeLengths = new byte[sc];
        t.symbols = new byte[sc];
        buf.get(t.codeLengths);
        buf.get(t.symbols);
        return t;
    }

    public int byteSize() {
        return 2 + symbolCount + symbolCount; // symbolCount (2 bytes) + codeLengths + symbols
    }
}
