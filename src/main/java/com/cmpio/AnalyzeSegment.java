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
            // 1) Header
            ByteOrder order = reader.getOrder();
            System.out.println("=== CMP Header Summary ===");
            System.out.println("Arquivo: " + path);
            System.out.println("Byte order: " + (order == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));
            System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n",
                    reader.getOtPos(), reader.getHdrPos(), reader.getRecPos0(), reader.getRecPos1());
            System.out.println("==============================================");

            // 2) Buffer do arquivo (garante não-nulo)
            ByteBuffer file = reader.getFileBuffer();
            if (file == null) {
                throw new IllegalStateException("CmpReader.getFileBuffer() retornou null. " +
                        "Verifique se CmpReader.open(path) foi chamado com sucesso.");
            }

            // 3) Parse do record inicial (REC_pos_0)
            final int recStart = reader.getRecPos0();
            SegmentRecord rec = SegmentRecord.parse(file, recStart, order);

            // 4) Stage 2: monta bitstream multi-record e auto-prova bit order/invert/shift
            Stage2Analyzer.analyze(file, recStart, order, rec);

            // após obter 'rec' no AnalyzeSegment:
            Stage3SymbolDump.run(file, recStart, order, rec);

            // Depois de obter o 'rec' e antes de sair:
            Stage3SymbolDumpPlus.run(file, recStart, order, rec, /*exportCsv=*/true, Path.of("cmp_stage3_out"));


        } finally {
            reader.closeQuietly();
        }
    }
}
