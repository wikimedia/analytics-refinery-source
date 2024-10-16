package org.wikimedia.analytics.refinery.core;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import javax.annotation.Nullable;

public final class PathUtils {

    private static final DeleteVisitor DELETE_VISITOR = new DeleteVisitor();

    private PathUtils() {
        // Utility class, should never be constructed
        throw new UnsupportedOperationException();
    }

    public static void recursiveDelete(@Nullable Path path) throws IOException {
        if (path == null) return;
        if (!Files.exists(path)) return;

        Files.walkFileTree(path, DELETE_VISITOR);
    }

    private static final class DeleteVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return CONTINUE;
        }
    }
}
