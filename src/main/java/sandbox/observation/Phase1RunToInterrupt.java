package sandbox.observation;

import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.NodeOutput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.state.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 阶段 1：跑到中断点为止，然后**让本进程结束**。
 *
 * <p>阶段 2 由另一个全新 JVM 接手，两阶段只通过磁盘上的 checkpoint 会合 ——
 * 这是观察项 ② 「挂起—续跑」的取证点：状态存在哪、进程是否必须活着。
 */
public final class Phase1RunToInterrupt {

    private static final Logger LOG = LoggerFactory.getLogger(Phase1RunToInterrupt.class);

    private Phase1RunToInterrupt() {
    }

    public static void main(final String[] args) throws GraphStateException, IOException {

        final long pid = ProcessHandle.current().pid();
        LOG.info("=== PHASE 1 | run to interrupt | JVM #1 | pid={} ===", pid);
        LOG.info("checkpoint directory = {}", Checkpoints.DIRECTORY.toAbsolutePath());
        LOG.info("threadId = {}", Checkpoints.THREAD_ID);

        final CompiledGraph<ObservationState> graph = HumanGateGraph.build(Checkpoints.DIRECTORY);

        final RunnableConfig config = RunnableConfig.builder()
                .threadId(Checkpoints.THREAD_ID)
                .build();

        LOG.info("--- stream until interruption ---");
        final AsyncGenerator.Cancellable<NodeOutput<ObservationState>> stream =
                graph.stream(Map.of(ObservationState.KEY_DRAFT, ""), config);
        for (final Object raw : stream) {
            LOG.info("stream event | {}", EventLog.describe(GraphResult.from(raw)));
        }
        // 中断信号是 AsyncGenerator 的**完成值**，不在 for-each 的元素流里 —— 必须显式取。
        LOG.info("stream result value (generator done) | {}", EventLog.describe(GraphResult.from(stream)));

        final StateSnapshot<ObservationState> snapshot = graph.getState(config);
        LOG.info("getState(config) | node={} | next={} | data={}",
                snapshot.node(), snapshot.next(), snapshot.state().data());

        final StateSnapshot<ObservationState> lastSnapshot = graph.lastStateOf(config).orElseThrow();
        LOG.info("lastStateOf(config) | node={} | next={} | data={}",
                lastSnapshot.node(), lastSnapshot.next(), lastSnapshot.state().data());

        logCheckpointFiles();

        LOG.info("=== PHASE 1 done | JVM #1 exits now | phase 2 must resume from disk ===");
    }

    private static void logCheckpointFiles() throws IOException {
        if (Files.notExists(Checkpoints.DIRECTORY)) {
            LOG.warn("checkpoint directory does not exist: {}", Checkpoints.DIRECTORY.toAbsolutePath());
            return;
        }
        try (Stream<Path> files = Files.list(Checkpoints.DIRECTORY)) {
            files.forEach(file -> {
                final String name = file.getFileName().toString();
                final long size = sizeOf(file);
                LOG.info("checkpoint file on disk | {} | {} bytes", name, size);
            });
        }
    }

    private static long sizeOf(final Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            LOG.warn("cannot read size of {}", file, e);
            return -1L;
        }
    }
}
