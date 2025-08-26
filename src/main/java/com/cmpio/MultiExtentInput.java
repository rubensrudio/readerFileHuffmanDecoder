
package com.cmpio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.READ;

/** Transparent reader across multi-extent CMP files. */
public final class MultiExtentInput implements AutoCloseable {
    private final Path baseFile;
    private final List<FileChannel> channels = new ArrayList<>();
    private final List<Long> extentStarts = new ArrayList<>(); // cumulative start offsets
    private long totalSize;

    public MultiExtentInput(Path baseFile, long expectedTotalSize) throws IOException {
        this.baseFile = baseFile.toAbsolutePath();
        openExtents(expectedTotalSize);
    }

    private void openExtents(long expectedTotalSize) throws IOException {
        long cum = 0L;
        int idx = -1;
        while (true) {
            Path p = (idx < 0) ? baseFile : numberedExtent(baseFile, idx+1);
            if (!Files.exists(p)) break;
            FileChannel ch = FileChannel.open(p, READ);
            channels.add(ch);
            extentStarts.add(cum);
            long size = ch.size();
            cum += size;
            idx++;
            if (expectedTotalSize > 0 && cum >= expectedTotalSize) break; // we have enough extents to cover virtual space
        }
        totalSize = cum;
        if (channels.isEmpty()) {
            throw new NoSuchFileException("Base CMP not found: " + baseFile);
        }
    }

    public static Path numberedExtent(Path base, int oneBased) {
        String name = base.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String stem = (dot < 0) ? name : name.substring(0, dot);
        String ext  = (dot < 0) ? "" : name.substring(dot);
        String numbered = String.format("%s%05d%s", stem, oneBased, ext);
        return base.getParent() == null ? Paths.get(numbered) : base.getParent().resolve(numbered);
    }

    /** Reads fully into dst from a virtual absolute offset. */
    public void readFully(long absoluteOffset, ByteBuffer dst) throws IOException {
        long remaining = dst.remaining();
        long pos = absoluteOffset;
        while (remaining > 0) {
            int extIndex = extentIndexFor(pos);
            if (extIndex < 0) throw new IOException("Offset out of range: " + pos + " (total=" + totalSize + ")");
            FileChannel ch = channels.get(extIndex);
            long extStart = extentStarts.get(extIndex);
            long within = pos - extStart;
            long canRead = Math.min(remaining, ch.size() - within);
            if (canRead <= 0) throw new IOException("Cannot read further at " + pos + " (extent " + extIndex + ")");
            ByteBuffer slice = dst.slice();
            slice.limit((int) Math.min(Integer.MAX_VALUE, canRead));
            int n = ch.read(slice, within);
            if (n <= 0) throw new IOException("Short read at pos " + pos);
            dst.position(dst.position() + n);
            remaining -= n;
            pos += n;
        }
    }

    private int extentIndexFor(long absolute) {
        if (absolute < 0 || absolute >= totalSize) return -1;
        int lo = 0, hi = extentStarts.size()-1;
        while (lo <= hi) {
            int mid = (lo+hi) >>> 1;
            long start = extentStarts.get(mid);
            long nextStart = (mid+1 < extentStarts.size()) ? extentStarts.get(mid+1) : Long.MAX_VALUE;
            if (absolute >= start && absolute < nextStart) return mid;
            if (absolute < start) hi = mid - 1; else lo = mid + 1;
        }
        return -1;
    }

    public long totalSize() { return totalSize; }

    @Override public void close() throws IOException {
        IOException first = null;
        for (FileChannel ch : channels) {
            try { ch.close(); } catch (IOException ex) { if (first == null) first = ex; }
        }
        if (first != null) throw first;
    }
}
