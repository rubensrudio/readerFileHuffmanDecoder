package com.cmpio;

/** Leitor de bits com seleção de ordem dentro do byte (MSB-first ou LSB-first). */
final class BitReader {
    private final byte[] data;
    private int bitPos;        // posição absoluta em bits
    private final int endBit;  // uma posição além do último bit legível
    private final boolean msbFirst;

    BitReader(byte[] whole) {
        this(whole, 0, whole.length * 8, true);
    }

    BitReader(byte[] data, int startBit, int bitLength) {
        this(data, startBit, bitLength, true);
    }

    BitReader(byte[] data, int startBit, int bitLength, boolean msbFirst) {
        this.data = data;
        this.bitPos = startBit;
        this.endBit = Math.min(startBit + bitLength, data.length * 8);
        this.msbFirst = msbFirst;
    }

    int bitPosition() { return bitPos; }

    int remainingBits() { return Math.max(0, endBit - bitPos); }

    /** Lê 1 bit (0/1) respeitando a ordem configurada no construtor. */
    int read1() {
        if (bitPos >= endBit) return -1;
        int byteIdx = bitPos >>> 3;
        int bitInByte = msbFirst ? (7 - (bitPos & 7)) : (bitPos & 7);
        int b = data[byteIdx] & 0xFF;
        int bit = (b >>> bitInByte) & 1;
        bitPos++;
        return bit;
    }

    /** Cria um "slice" limitado a bitLength bits a partir da posição atual e avança o leitor pai. */
    BitReader sliceBits(int bitLength) {
        BitReader sub = new BitReader(data, bitPos, bitLength, msbFirst);
        bitPos += bitLength;
        return sub;
    }

    /** Retorna um novo leitor a partir da posição atual com a ordem de bits desejada. */
    BitReader withBitOrder(boolean msbFirst) {
        return new BitReader(this.data, this.bitPos, this.endBit - this.bitPos, msbFirst);
    }
}
