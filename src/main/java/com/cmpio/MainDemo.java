package com.cmpio;

/**
 * Thin wrapper para facilitar a execução. Encaminha para AnalyzeSegment.
 * Útil para manter IDE/configs que chamam "MainDemo".
 */
public final class MainDemo {
    private MainDemo() {}

    public static void main(String[] args) throws Exception {
        AnalyzeSegment.main(args);
    }
}
