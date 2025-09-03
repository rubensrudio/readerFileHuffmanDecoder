package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * Executa a análise de um único segmento.
 * Por padrão, usa o primeiro record do arquivo (o segmento (0,0,0) no seu caso).
 * Se desejar, você pode estender para aceitar seg1/seg2/seg3 por linha de comando
 * e mapear esses índices para o record correto.
 */
public final class AnalyzeSegment {

    private AnalyzeSegment() {}

    public static void main(String[] args) throws Exception {
        final Path path = Path.of("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");

        CmpReader reader = CmpReader.open(path.toAbsolutePath().toString());
        try {
            ByteOrder order = reader.getOrder();

            System.out.println("=== CMP Header Summary ===");
            System.out.println("Arquivo: " + path);
            System.out.println("Byte order: " + (order == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));
            System.out.println("=======================================");

            System.out.println("Nenhum (seg1,seg2,seg3) informado. Usando o primeiro não-vazio: (0,0,0)");

            final int recStart = reader.getRecPos0();       // início do primeiro record
            final ByteBuffer file = reader.requireFileBuffer(); // <- garante não-null

            // Parse do record e Stage 2
            SegmentRecord rec = SegmentRecord.parse(file, recStart, order);
            Stage2Analyzer.analyze(file, recStart, order, rec);

        } finally {
            reader.closeQuietly();
        }
    }
}
