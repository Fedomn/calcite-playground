package com.github.fedomn.util;

import org.apache.calcite.util.Sources;

import java.io.IOException;
import java.nio.file.Files;

public abstract class Util {
    public static String filePath(String filename) {
        return Sources.of(Util.class.getResource("/" + filename)).file().getAbsolutePath();
    }

    public static String fileContent(String filename) throws IOException {
        return Files.readString(Sources.of(Util.class.getResource("/" + filename)).file().toPath());
    }
}
