
package com.cmpio;

import java.nio.ByteOrder;
import java.nio.file.Path;

public final class MainDemo {
    public static void main(String[] args) throws Exception {
        Path p = Path.of("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");
        try (CmpReader r = CmpReader.open(p)) {
            System.out.println("Base file: " + p.toAbsolutePath());
            System.out.println("Detected byte order: " + r.byteOrder());
            System.out.println("FileHeader: " + r.fileHeader());
            System.out.println("DataHeader: " + r.dataHeader());

            int[] first = r.findFirstNonEmpty();
            if (first == null) {
                System.out.println("No non-empty segments found in offset table.");
                return;
            }
            System.out.printf("First non-empty segment at (%d,%d,%d)%n", first[0], first[1], first[2]);

            SegmentRecord rec = r.readSegmentRecord(first[0], first[1], first[2]);
            if (rec == null) {
                System.out.println("Segment offset indicates empty record.");
                return;
            }
            System.out.printf("Segment metadata: minDelta=%.6f maxDelta=%.6f%n", rec.metadata.minDelta, rec.metadata.maxDelta);

            int nonZeroBlocks = 0;
            long totalBits = 0;
            for (int i = 0; i < 64; i++) {
                if (rec.metadata.blockSizesBits[i] > 0) nonZeroBlocks++;
                totalBits += rec.metadata.blockSizesBits[i];
            }
            System.out.printf("Blocks with data: %d / 64, compressed bitstream length: %d bits (%.2f bytes)%n",
                    nonZeroBlocks, totalBits, totalBits/8.0);

            System.out.printf("Huffman table: symbols=%d, tableBytes=%d%n",
                    rec.huffman.symbolCount, rec.huffman.byteSize());
        }
    }
}
