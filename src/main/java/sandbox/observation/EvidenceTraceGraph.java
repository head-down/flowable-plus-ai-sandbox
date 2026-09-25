package sandbox.observation;

import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.action.AsyncNodeActionWithConfig;
import org.bsc.langgraph4j.checkpoint.BaseCheckpointSaver;
import org.bsc.langgraph4j.checkpoint.FileSystemSaver;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.bsc.langgraph4j.StateGraph.END;
import static org.bsc.langgraph4j.StateGraph.START;
import static org.bsc.langgraph4j.action.AsyncNodeActionWithConfig.node_async;

/**
 * 观察项 ③ 用的图：{@code decide -> await_human -> finalize}。
 *
 * <p>结构与 {@link HumanGateGraph} 同形，但 {@code decide} 节点扮演「决策源」，把调研报告
 * §3.1 的五个留痕下限项当作**普通状态值**写进 state —— 框架对它们没有任何认识。
 * 中断点仍是节点边界，好让「盘上留下什么」可以被一个新进程读回来对比。
 */
public final class EvidenceTraceGraph {

    public static final String NODE_DECIDE = "decide";

    public static final String NODE_AWAIT_HUMAN = "await_human";

    public static final String NODE_FINALIZE = "finalize";

    public static final String STUB_MODEL_ID = "stub-decision-source/1.0";

    public static final String STUB_INPUT_SNAPSHOT =
            "input-snapshot: purchase request A-1001, amount=12000, requester=<redacted>";

    public static final String STUB_RAW_OUTPUT = "{\"action\":\"APPROVE\",\"confidence\":0.83}";

    public static final String STUB_RATIONALE = "amount within configured cap; source not on watchlist";

    public static final String STUB_CONFIDENCE = "0.83";

    private EvidenceTraceGraph() {
    }

    public static CompiledGraph<EvidenceState> build(final Path checkpointDirectory) throws GraphStateException {

        final AsyncNodeActionWithConfig<EvidenceState> decideAction = node_async((state, config) -> {
            final String metadataKeysSeen = String.join(",", config.metadataKeys());
            return Map.of(
                    EvidenceState.KEY_MODEL_ID, STUB_MODEL_ID,
                    EvidenceState.KEY_INPUT_SNAPSHOT, STUB_INPUT_SNAPSHOT,
                    EvidenceState.KEY_RAW_OUTPUT, STUB_RAW_OUTPUT,
                    EvidenceState.KEY_RATIONALE, STUB_RATIONALE,
                    EvidenceState.KEY_CONFIDENCE, STUB_CONFIDENCE,
                    EvidenceState.KEY_TRACE, List.of(
                            "decide | model=" + STUB_MODEL_ID
                                    + " | configMetadataKeys=" + metadataKeysSeen)
            );
        });

        final AsyncNodeActionWithConfig<EvidenceState> awaitHumanAction = node_async((state, config) -> Map.of(
                EvidenceState.KEY_TRACE, List.of("entered:" + NODE_AWAIT_HUMAN)
        ));

        final AsyncNodeActionWithConfig<EvidenceState> finalizeAction = node_async((state, config) -> Map.of(
                EvidenceState.KEY_TRACE, List.of("entered:" + NODE_FINALIZE)
        ));

        final StateGraph<EvidenceState> graphBuilder =
                new StateGraph<>(EvidenceState.SCHEMA, EvidenceState::new)
                        .addNode(NODE_DECIDE, decideAction)
                        .addNode(NODE_AWAIT_HUMAN, awaitHumanAction)
                        .addNode(NODE_FINALIZE, finalizeAction)
                        .addEdge(START, NODE_DECIDE)
                        .addEdge(NODE_DECIDE, NODE_AWAIT_HUMAN)
                        .addEdge(NODE_AWAIT_HUMAN, NODE_FINALIZE)
                        .addEdge(NODE_FINALIZE, END);

        final BaseCheckpointSaver saver =
                new FileSystemSaver(checkpointDirectory, graphBuilder.getStateSerializer());

        final CompileConfig compileConfig = CompileConfig.builder()
                .checkpointSaver(saver)
                .interruptBefore(NODE_AWAIT_HUMAN)
                .releaseThread(false)
                .build();

        return graphBuilder.compile(compileConfig);
    }
}
