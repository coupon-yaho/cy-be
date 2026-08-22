package com.kafkick.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class CommittedConfigStager {

    private CommittedConfigStager() {
    }

    public static void stage(Path target, String classpathResource) throws IOException {
        Files.createDirectories(target.getParent());
        try (InputStream source = resource(classpathResource)) {
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static InputStream resource(String name) throws IOException {
        InputStream source = CommittedConfigStager.class.getClassLoader().getResourceAsStream(name);
        if (source == null) {
            throw new IOException("Classpath resource not found: " + name);
        }
        return source;
    }
}
