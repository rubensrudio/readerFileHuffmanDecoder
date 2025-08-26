
package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Utilities for endianness detection and primitive reads. */
public final class ByteOrderUtils {
    private ByteOrderUtils() {}

    public static final int FILE_IDENT = 390179341; // from spec
    public static final int FILE_VERSION_MIN = 1000;
    public static final int FILE_VERSION_MAX = 1100;

    /** Detects byte order by reading the IDENT and VERSION fields in the first 12 bytes of the header. */
    public static ByteOrder detectFileHeaderByteOrder(Path baseFile) throws IOException {
        try (FileChannel ch = FileChannel.open(baseFile, StandardOpenOption.READ)) {
            byte[] arr = new byte[12];
            ByteBuffer tmp = ByteBuffer.wrap(arr);
            int n = ch.read(tmp, 0);
            if (n < 12) throw new IOException("Header too small to detect byte order: read " + n + " bytes");
            int identBE = getIntAt(arr, 4, ByteOrder.BIG_ENDIAN);
            int verBE = getIntAt(arr, 8, ByteOrder.BIG_ENDIAN);
            if (identBE == FILE_IDENT && verBE >= FILE_VERSION_MIN && verBE <= FILE_VERSION_MAX) {
                return ByteOrder.BIG_ENDIAN;
            }
            int identLE = getIntAt(arr, 4, ByteOrder.LITTLE_ENDIAN);
            int verLE = getIntAt(arr, 8, ByteOrder.LITTLE_ENDIAN);
            if (identLE == FILE_IDENT && verLE >= FILE_VERSION_MIN && verLE <= FILE_VERSION_MAX) {
                return ByteOrder.LITTLE_ENDIAN;
            }
            throw new IOException("Cannot detect byte order: IDENT/VER mismatch (BE ident=" + identBE + " ver=" + verBE +
                                  ", LE ident=" + identLE + " ver=" + verLE + ")");
        }
    }

    public static int getIntAt(byte[] a, int off, ByteOrder order) {
        if (order == ByteOrder.BIG_ENDIAN) {
            return ((a[off] & 0xff) << 24) | ((a[off+1] & 0xff) << 16) | ((a[off+2] & 0xff) << 8) | (a[off+3] & 0xff);
        } else {
            return ((a[off+3] & 0xff) << 24) | ((a[off+2] & 0xff) << 16) | ((a[off+1] & 0xff) << 8) | (a[off] & 0xff);
        }
    }
}
