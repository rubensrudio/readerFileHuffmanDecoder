package com.cmpio;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class Stage11Driver {
    private Stage11Driver() {}

    public static void main(String[] args) throws Exception {
        Path cmpPath = Paths.get("D:\\tmp\\cmp_dir\\S_ANALYTIC_ZERODATUM_13.cmp");
        Stage11SignalInspector.Result r = Stage11SignalInspector.run(cmpPath);
        System.out.println("[Stage 11] " + r);
    }
}
