package sandbox.observation;

import java.nio.file.Path;

/**
 * 取证用的固定坐标：checkpoint 落盘目录与线程标识。
 *
 * <p>阶段 1 与阶段 2 是**两个独立 JVM**，只靠这两个常量在磁盘上会合。
 */
public final class Checkpoints {

    public static final Path DIRECTORY = Path.of("target", "lg4j-checkpoints");

    public static final String THREAD_ID = "observation-thread-1";

    private Checkpoints() {
    }
}
