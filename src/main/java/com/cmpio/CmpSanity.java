package com.cmpio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

/**
 * Sanity checker do pipeline CMP (Stages 1–2–3 básicos):
 * - Header e offsets
 * - Offset Table base (plausibilidade OT_pos / OT_pos+8)
 * - Primeiro segmento não-vazio: Huffman, payloadStart, requiredBits
 * - Montagem com PayloadAssembler: checa se bits montados >= requiredBits
 *
 * Uso rápido:
 *   boolean ok = CmpSanity.run(Path.of("arquivo.cmp"));
 */
public final class CmpSanity {

    private CmpSanity() {}

    // tolerâncias / parâmetros
    private static final int SEGMENT_RECORD_SIZE = 8192;
    private static final int REC0_TOLERANCE_BYTES = 64;   // tolerância no cálculo de REC_pos_0 esperado
    private static final int MIN_PAYLOAD_START = 512;

    public static boolean run(Path cmpPath) {
        Locale.setDefault(Locale.ROOT);

        try (CmpReader r = new CmpReader(cmpPath)) {
            r.open();

            // ===== Stage 1: Header =====
            if (!checkHeader(r)) return false;

            // ===== Primeiro segmento não-vazio =====
            int[] first = r.findFirstNonEmpty();
            if (first == null) {
                System.err.println("[Sanity] Nenhum segmento com offset>0 na tabela.");
                return false;
            }
            final int s1 = first[0], s2 = first[1], s3 = first[2];
            System.out.printf("[Sanity] Primeiro segmento não-vazio: (%d,%d,%d)%n", s1, s2, s3);

            // ===== Stage 2/3: Parse do SegmentRecord =====
            SegmentRecord rec = r.readSegmentRecord(s1, s2, s3);
            if (!checkSegmentBasics(rec)) return false;

            // ===== Montagem multi-record =====
            if (!checkAssembly(r, rec)) return false;

            System.out.println("[Sanity] OK: arquivo passou nas verificações básicas.");
            return true;
        } catch (Throwable t) {
            System.err.println("[Sanity] Falha: " + t.getMessage());
            t.printStackTrace(System.err);
            return false;
        }
    }

    // ---------------------------------------------------------------------
    // Header & layout

    private static boolean checkHeader(CmpReader r) {
        final ByteOrder order = r.getByteOrder();
        final long ot = r.getOtPos();
        final long hdr = r.getHdrPos();
        final long rec0 = r.getRecPos0();
        final long rec1 = r.getRecPos1();
        final int hdrLen = r.getHdrLen();
        final int recLen = r.getRecLen();

        System.out.println("=== CMP Header Summary ===");
        System.out.println("Arquivo: " + r.getBasePath());
        System.out.println("Byte order: " + (order == ByteOrder.BIG_ENDIAN ? "BIG_ENDIAN" : "LITTLE_ENDIAN"));
        System.out.printf("OT_pos=%d, HDR_pos=%d, REC_pos_0=%d, REC_pos_1=%d%n", ot, hdr, rec0, rec1);
        System.out.printf("HDR_len=%d, REC_len=%d%n", hdrLen, recLen);
        System.out.printf("Ranges: [%d..%d] x [%d..%d] x [%d..%d] => segments=%d%n",
                r.getMin1(), r.getMax1(), r.getMin2(), r.getMax2(), r.getMin3(), r.getMax3(),
                r.getSegmentsCount());
        System.out.println("==============================================");

        // Checks básicos
        if (recLen != SEGMENT_RECORD_SIZE) {
            System.out.printf("[Sanity] Aviso: REC_len=%d (esperado %d). Seguiremos com %d.%n",
                    recLen, SEGMENT_RECORD_SIZE, SEGMENT_RECORD_SIZE);
        }
        if (!(ot >= 1024 && hdr > ot && rec0 >= hdr)) {
            System.err.println("[Sanity] Header: ordem de offsets incoerente (esperado: OT < HDR < REC0).");
            return false;
        }
        if (r.getSegmentsCount() <= 0) {
            System.err.println("[Sanity] Contagem de segmentos <= 0.");
            return false;
        }
        if (hdrLen < 1024 || hdrLen > (1 << 16)) {
            System.out.println("[Sanity] Aviso: HDR_len fora da faixa típica: " + hdrLen);
        }

        // Coerência aproximada de REC_pos_0 = OT_pos + [0|8] + n*8 + HDR_len
        long n = r.getSegmentsCount();
        long t0 = ot + (n * 8L) + hdrLen;
        long t1 = ot + 8L + (n * 8L) + hdrLen;
        boolean ok0 = Math.abs(rec0 - t0) <= REC0_TOLERANCE_BYTES;
        boolean ok1 = Math.abs(rec0 - t1) <= REC0_TOLERANCE_BYTES;

        if (!ok0 && !ok1) {
            System.out.printf("[Sanity] Aviso: REC_pos_0 inesperado (dif=%d / difLeadIn=%d).%n",
                    Math.abs(rec0 - t0), Math.abs(rec0 - t1));
            // não reprovamos aqui; vários arquivos antigos têm desalinhamentos leves
        } else {
            System.out.printf("[Sanity] REC_pos_0 consistente com %s (Δ=%d bytes).%n",
                    ok0 ? "OT_pos" : "OT_pos+8",
                    ok0 ? Math.abs(rec0 - t0) : Math.abs(rec0 - t1));
        }
        return true;
    }

    // ---------------------------------------------------------------------
    // Segmento: Huffman e payload

    private static boolean checkSegmentBasics(SegmentRecord rec) {
        // Huffman
        int N = rec.huffman.N;
        int[] lens = rec.huffman.lens;
        int nonZero = 0, maxLen = 0;
        for (int L : lens) {
            if (L < 0 || L > 15) {
                System.err.println("[Sanity] Huffman: comprimento fora de 0..15: L=" + L);
                return false;
            }
            if (L > 0) nonZero++;
            if (L > maxLen) maxLen = L;
        }
        if (!rec.huffman.kraftOk) {
            System.err.println("[Sanity] Huffman: falhou teste de Kraft.");
            return false;
        }
        if (N < 2) {
            System.err.println("[Sanity] Huffman: N muito pequeno (" + N + ").");
            return false;
        }

        // Hist de comprimentos
        int[] hist = new int[maxLen + 1];
        for (int L : lens) if (L >= 0 && L < hist.length) hist[L]++;
        System.out.print("[Sanity] Lengths histogram:");
        for (int L = 1; L < hist.length; L++) if (hist[L] > 0) System.out.print(" " + L + ":" + hist[L]);
        System.out.println();

        // Payload
        int payloadStart = rec.md.payloadStartByte;
        int payloadBytes  = rec.md.payloadBytes;
        if (payloadStart < MIN_PAYLOAD_START || (payloadStart & 0xF) != 0) {
            System.err.printf("[Sanity] payloadStart inesperado: %d (mín=%d, align16).%n",
                    payloadStart, MIN_PAYLOAD_START);
            return false;
        }
        if (payloadBytes <= 0 || payloadBytes > SEGMENT_RECORD_SIZE) {
            System.err.println("[Sanity] payloadBytes inválido: " + payloadBytes);
            return false;
        }

        long requiredBits = rec.md.totalBits;
        long availableBits = (long) payloadBytes * 8L;
        System.out.printf("[Sanity] requiredBits=%d, availableBits(single)=%d%n",
                requiredBits, availableBits);

        if (requiredBits <= 0) {
            System.err.println("[Sanity] requiredBits <= 0.");
            return false;
        }
        return true;
    }

    // ---------------------------------------------------------------------
    // Montagem multi-record

    private static boolean checkAssembly(CmpReader r, SegmentRecord rec) {
        try {
            ByteBuffer file = r.getFileBuffer();
            ByteOrder order = r.getByteOrder();

            long requiredBits = rec.md.totalBits;
            PayloadAssembler.Assembled as =
                    PayloadAssembler.assemble(file, rec.recStart, order, rec, requiredBits);

            long haveBits = (long) as.bytes.length * 8L;
            System.out.printf("[Sanity] Assembled: bytes=%d (bits=%d), need=%d%n",
                    as.bytes.length, haveBits, requiredBits);

            if (haveBits < requiredBits) {
                System.err.printf("[Sanity] Montagem insuficiente: have=%d < need=%d%n",
                        haveBits, requiredBits);
                return false;
            }
            // um smoke test leve: decodificar alguns símbolos para confirmar que a árvore funciona
            HuffmanStreamDecoder d = HuffmanStreamDecoder.fromCanonical(
                    rec.huffman.symbols, rec.huffman.lens, as.bytes, Math.min(haveBits, 4096),
                    /*lsb=*/false, /*invert=*/true, /*shift=*/0
            );
            int ok = 0;
            for (int i = 0; i < 64; i++) {
                int s = d.next();
                if (s < 0) break;
                ok++;
            }
            if (ok < 8) {
                System.out.printf("[Sanity] Aviso: o preview decodificou poucos símbolos (%d).%n", ok);
            } else {
                System.out.printf("[Sanity] Preview OK: decodificou %d símbolos de teste.%n", ok);
            }
            return true;
        } catch (Throwable t) {
            System.err.println("[Sanity] Falha na montagem/preview: " + t.getMessage());
            t.printStackTrace(System.err);
            return false;
        }
    }
}
