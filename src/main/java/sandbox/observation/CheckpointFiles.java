package sandbox.observation;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * 把 checkpoint 落盘目录的现状压成几行 log。
 *
 * <p>「盘上有什么」是观察项 ② 与 ③ 的共同前提：只有盘上的东西才跨进程活得下来。
 */
public final class CheckpointFiles {

    private CheckpointFiles() {
    }

    public static void log(final Logger logger, final Path directory) {
        if (Files.notExists(directory)) {
            logger.info("checkpoint directory does not exist | {}", directory.toAbsolutePath());
            return;
        }
        try (Stream<Path> files = Files.list(directory)) {
            final List<Path> sorted = files.sorted().toList();
            for (final Path file : sorted) {
                final String name = file.getFileName().toString();
                final long size = sizeOf(logger, file);
                logger.info("checkpoint file on disk | {} | {} bytes", name, size);
            }
        } catch (IOException e) {
            logger.warn("cannot list checkpoint directory {}", directory, e);
        }
    }

    private static long sizeOf(final Logger logger, final Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            logger.warn("cannot read size of {}", file, e);
            return -1L;
        }
    }
}
