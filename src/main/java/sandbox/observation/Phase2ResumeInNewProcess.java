package sandbox.observation;

import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.state.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 阶段 2：在**全新的 JVM** 里，凭同一个 threadId 从磁盘上的 checkpoint 续跑。
 *
 * <p>人工介入的形态在这里取到：外部调用方直接 {@code updateState} 写状态，
 * 且**不以任何图节点身份**写入（{@code asNode = null}）—— 这是观察项 ① 的一手证据。
 */
public final class Phase2ResumeInNewProcess {

    private static final Logger LOG = LoggerFactory.getLogger(Phase2ResumeInNewProcess.class);

    public static final String HUMAN_DECISION = "APPROVED";

    private Phase2ResumeInNewProcess() {
    }

    public static void main(final String[] args) throws GraphStateException, Exception {

        final long pid = ProcessHandle.current().pid();
        LOG.info("=== PHASE 2 | resume from disk | JVM #2 | pid={} ===", pid);
        LOG.info("checkpoint directory = {}", Checkpoints.DIRECTORY.toAbsolutePath());
        LOG.info("threadId = {}", Checkpoints.THREAD_ID);

        final CompiledGraph<ObservationState> graph = HumanGateGraph.build(Checkpoints.DIRECTORY);

        final RunnableConfig config = RunnableConfig.builder()
                .threadId(Checkpoints.THREAD_ID)
                .build();

        final StateSnapshot<ObservationState> restored = graph.getState(config);
        LOG.info("state restored from disk | node={} | next={} | data={}",
                restored.node(), restored.next(), restored.state().data());

        final RunnableConfig updated = graph.updateState(
                config, Map.of(ObservationState.KEY_HUMAN_DECISION, HUMAN_DECISION), null);
        final StateSnapshot<ObservationState> afterHuman = graph.getState(updated);
        LOG.info("human decision written | {}={} | next node={}",
                ObservationState.KEY_HUMAN_DECISION, HUMAN_DECISION, afterHuman.next());

        LOG.info("--- resume stream ---");
        for (final Object raw : graph.stream(GraphInput.resume(), updated)) {
            LOG.info("stream event | {}", EventLog.describe(GraphResult.from(raw)));
        }

        final StateSnapshot<ObservationState> finalSnapshot = graph.getState(updated);
        LOG.info("final state | node={} | next={} | data={}",
                finalSnapshot.node(), finalSnapshot.next(), finalSnapshot.state().data());

        LOG.info("--- getStateHistory ---");
        for (final StateSnapshot<ObservationState> snapshot : graph.getStateHistory(updated)) {
            LOG.info("history entry | node={} | next={} | data={}",
                    snapshot.node(), snapshot.next(), snapshot.state().data());
        }

        LOG.info("=== PHASE 2 done ===");
    }
}
