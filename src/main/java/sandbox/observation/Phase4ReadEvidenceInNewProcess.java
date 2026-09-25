package sandbox.observation;

import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.NodeOutput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.state.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 观察项 ③ 的第二阶段：在**全新的 JVM** 里读回阶段 3 留在盘上的东西。
 *
 * <p>两次读有意用两份不同的 {@code RunnableConfig}：一份完全不带元数据，一份带 ——
 * 差别只在这份 config 上，盘上的文件是同一个。这正是「元数据袋由调用方现传、不是恢复出来的」
 * 的直接证据。
 *
 * <p>读回之后接着续跑到底，用来看累积的证据轨迹能不能跨进程活下来。
 */
public final class Phase4ReadEvidenceInNewProcess {

    private static final Logger LOG = LoggerFactory.getLogger(Phase4ReadEvidenceInNewProcess.class);

    private static final String META_DECISION_SOURCE_ID = "decisionSourceId";

    private static final String META_DECISION_SOURCE_ID_VALUE = "stub-provider-1";

    private Phase4ReadEvidenceInNewProcess() {
    }

    public static void main(final String[] args) throws Exception {

        final long pid = ProcessHandle.current().pid();
        LOG.info("=== PHASE 4 | read decision evidence from disk | JVM #2 | pid={} ===", pid);
        LOG.info("checkpoint directory = {}", Checkpoints.DIRECTORY.toAbsolutePath());
        LOG.info("threadId = {}", Checkpoints.THREAD_ID);

        final CompiledGraph<EvidenceState> graph = EvidenceTraceGraph.build(Checkpoints.DIRECTORY);

        final RunnableConfig fresh = RunnableConfig.builder()
                .threadId(Checkpoints.THREAD_ID)
                .build();

        LOG.info("--- read #1: fresh config with no caller metadata ---");
        final StateSnapshot<EvidenceState> bare = graph.getState(fresh);
        final String bareMetadataKeys = String.join(",", bare.metadataKeys());
        final String bareState = String.valueOf(bare.state().data());
        LOG.info("state restored from disk | node={} | next={}", bare.node(), bare.next());
        LOG.info("state restored from disk | metadataKeys=[{}]", bareMetadataKeys);
        LOG.info("state restored from disk | stateData={}", bareState);

        LOG.info("--- read #2: same disk, caller metadata added to THIS config only ---");
        final RunnableConfig withMetadata = RunnableConfig.builder()
                .threadId(Checkpoints.THREAD_ID)
                .putMetadata(META_DECISION_SOURCE_ID, META_DECISION_SOURCE_ID_VALUE)
                .build();
        final StateSnapshot<EvidenceState> annotated = graph.getState(withMetadata);
        final String annotatedMetadataKeys = String.join(",", annotated.metadataKeys());
        LOG.info("same checkpoint | metadataKeys=[{}]", annotatedMetadataKeys);

        LOG.info("--- getStateHistory in a fresh process ---");
        for (final StateSnapshot<EvidenceState> entry : graph.getStateHistory(fresh)) {
            final String entryMetadataKeys = String.join(",", entry.metadataKeys());
            final String entryState = String.valueOf(entry.state().data());
            LOG.info("history entry | node={} | next={} | metadataKeys=[{}]", entry.node(), entry.next(), entryMetadataKeys);
            LOG.info("history entry | stateData={}", entryState);
        }

        LOG.info("--- resume to completion in this fresh process ---");
        final RunnableConfig updated = graph.updateState(fresh, Map.of(), null);
        final AsyncGenerator.Cancellable<NodeOutput<EvidenceState>> stream =
                graph.stream(GraphInput.resume(), updated);
        for (final Object raw : stream) {
            final GraphResult result = GraphResult.from(raw);
            LOG.info("stream event | {}", EventLog.describe(result));
        }
        final GraphResult done = GraphResult.from(stream);
        LOG.info("stream result value (generator done) | {}", EventLog.describe(done));

        final StateSnapshot<EvidenceState> finalSnapshot = graph.lastStateOf(updated).orElseThrow();
        final String finalState = String.valueOf(finalSnapshot.state().data());
        LOG.info("lastStateOf(updated) | node={} | next={}", finalSnapshot.node(), finalSnapshot.next());
        LOG.info("lastStateOf(updated) | stateData={}", finalState);

        LOG.info("--- what is on disk after the run finished ---");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);

        LOG.info("=== PHASE 4 done ===");
    }
}
