package sandbox.observation;

import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.HasMetadata;
import org.bsc.langgraph4j.NodeOutput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.checkpoint.Checkpoint;
import org.bsc.langgraph4j.state.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 观察项 ③ 的第一阶段：让「决策源」写出证据，跑到人工闸门停下，然后**让 JVM 结束**。
 *
 * <p>本阶段要回答的是：一次决策跑完之后，**框架自己**在盘上留了什么。
 * 调用方另外挂了一份 {@code RunnableConfig} 元数据（通用键值袋）作对照 ——
 * 它是本阶段唯一的「非状态值」留痕。
 */
public final class Phase3WriteEvidence {

    private static final Logger LOG = LoggerFactory.getLogger(Phase3WriteEvidence.class);

    private static final String META_DECISION_SOURCE_ID = "decisionSourceId";

    private static final String META_DECISION_SOURCE_ID_VALUE = "stub-provider-1";

    private static final String META_CALLER_TRACE_ID = "callerTraceId";

    private static final String META_CALLER_TRACE_ID_VALUE = "trace-abc-001";

    private Phase3WriteEvidence() {
    }

    public static void main(final String[] args) throws Exception {

        final long pid = ProcessHandle.current().pid();
        LOG.info("=== PHASE 3 | write decision evidence, stop at human gate | JVM #1 | pid={} ===", pid);
        LOG.info("checkpoint directory = {}", Checkpoints.DIRECTORY.toAbsolutePath());
        LOG.info("threadId = {}", Checkpoints.THREAD_ID);

        final CompiledGraph<EvidenceState> graph = EvidenceTraceGraph.build(Checkpoints.DIRECTORY);

        final RunnableConfig config = RunnableConfig.builder()
                .threadId(Checkpoints.THREAD_ID)
                .putMetadata(META_DECISION_SOURCE_ID, META_DECISION_SOURCE_ID_VALUE)
                .putMetadata(META_CALLER_TRACE_ID, META_CALLER_TRACE_ID_VALUE)
                .build();
        final String callerMetadataKeys = String.join(",", config.metadataKeys());
        LOG.info("caller metadata put on RunnableConfig | keys=[{}]", callerMetadataKeys);

        LOG.info("--- framework's own vocabulary for one execution ---");
        LOG.info("GraphResult.Type values = {}", Arrays.toString(GraphResult.Type.values()));
        LOG.info("CompiledGraph.StreamMode values = {}", Arrays.toString(CompiledGraph.StreamMode.values()));
        LOG.info("durable unit | {} declared fields = {}", Checkpoint.class.getSimpleName(),
                describeDeclaredFields(Checkpoint.class));

        LOG.info("--- stream until interruption (values mode) ---");
        final AsyncGenerator.Cancellable<NodeOutput<EvidenceState>> stream =
                graph.stream(Map.of(), config);
        for (final Object raw : stream) {
            final GraphResult result = GraphResult.from(raw);
            LOG.info("stream event | {} | elementClass={} | elementMetadataKeys={}",
                    EventLog.describe(result), raw.getClass().getName(), metadataKeysOf(raw));
        }
        final GraphResult done = GraphResult.from(stream);
        LOG.info("stream result value (generator done) | {}", EventLog.describe(done));

        final StateSnapshot<EvidenceState> snapshot = graph.getState(config);
        final String stateData = String.valueOf(snapshot.state().data());
        LOG.info("getState(config) | node={} | next={}", snapshot.node(), snapshot.next());
        LOG.info("getState(config) | stateData={}", stateData);
        final String snapshotMetadataKeys = String.join(",", snapshot.metadataKeys());
        LOG.info("getState(config) | metadataKeys=[{}] | (这是调用方本次传进去的，不是从盘上读回来的)",
                snapshotMetadataKeys);

        LOG.info("--- getStateHistory ---");
        for (final StateSnapshot<EvidenceState> entry : graph.getStateHistory(config)) {
            final String entryMetadataKeys = String.join(",", entry.metadataKeys());
            final String entryState = String.valueOf(entry.state().data());
            LOG.info("history entry | node={} | next={} | metadataKeys=[{}]", entry.node(), entry.next(), entryMetadataKeys);
            LOG.info("history entry | stateData={}", entryState);
        }

        LOG.info("--- what is on disk after the interruption ---");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);

        LOG.info("=== PHASE 3 done | JVM #1 exits now | phase 4 reads the same disk with a fresh config ===");
    }

    private static String metadataKeysOf(final Object element) {
        return (element instanceof HasMetadata hasMetadata)
                ? String.join(",", hasMetadata.metadataKeys())
                : "<not-a-metadata-carrier>";
    }

    private static String describeDeclaredFields(final Class<?> type) {
        final List<String> fields = Arrays.stream(type.getDeclaredFields())
                .sorted(Comparator.comparing(Field::getName))
                .map(field -> field.getType().getSimpleName() + " " + field.getName())
                .collect(Collectors.toList());
        return String.join(", ", fields);
    }
}
