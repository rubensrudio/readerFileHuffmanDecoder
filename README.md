# CMP IO Starter (Stage 1)

This project implements Stage 1 of a CMP reader:

- Detect file byte order (endianness) from the File Header magic/version
- Parse **File Header** (1024 bytes)
- Seamlessly read across **multi-extent** files (`dataset.cmp`, `dataset00001.cmp`, ...)
- Load **Segment Offset Table**
- Parse **Data Header** (4120 bytes)
- Read a single **8192-byte segment record** and parse its **Segment Metadata** (264 bytes) and **Huffman table prelude**
- (No decompression yet)

## Build & Run

```bash
mvn -q -e -DskipTests package
java -cp target/cmp-io-starter-0.1.0.jar com.cmpio.MainDemo /path/to/your/dataset.cmp
```

The demo prints header summaries, segment grid size, and inspects the first non-empty segment record (if any).
