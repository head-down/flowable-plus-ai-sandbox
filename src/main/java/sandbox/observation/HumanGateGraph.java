package sandbox.observation;

import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.action.AsyncNodeAction;
import org.bsc.langgraph4j.checkpoint.BaseCheckpointSaver;
import org.bsc.langgraph4j.checkpoint.FileSystemSaver;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.bsc.langgraph4j.StateGraph.END;
import static org.bsc.langgraph4j.StateGraph.START;
import static org.bsc.langgraph4j.action.AsyncNodeAction.node_async;

/**
 * 一张最小的人工闸门图：{@code draft -> human_gate -> finalize}。
 *
 * <p>中断点声明在**节点边界**（{@code interruptBefore}）—— 这是观察项 ①
 * 里「编排层」落点的一种形态：闸门不在调用内、不在输出后，而在节点之间。
 *
 * <p>checkpoint 落在文件系统（{@link FileSystemSaver}），因而可以跨进程读取 ——
 * 这是观察项 ② 的前提。
 */
public final class HumanGateGraph {

    public static final String NODE_DRAFT = "draft";

    public static final String NODE_HUMAN_GATE = "human_gate";

    public static final String NODE_FINALIZE = "finalize";

    public static final String DRAFT_TEXT = "draft produced by node 'draft'";

    private HumanGateGraph() {
    }

    public static CompiledGraph<ObservationState> build(final Path checkpointDirectory) throws GraphStateException {

        final AsyncNodeAction<ObservationState> draftAction = node_async(state -> Map.of(
                ObservationState.KEY_DRAFT, DRAFT_TEXT,
                ObservationState.KEY_TRAIL, List.of("entered:" + NODE_DRAFT)
        ));

        final AsyncNodeAction<ObservationState> humanGateAction = node_async(state -> Map.of(
                ObservationState.KEY_TRAIL,
                List.of("entered:" + NODE_HUMAN_GATE
                        + "; humanDecision=" + state.humanDecision().orElse("<absent>"))
        ));

        final AsyncNodeAction<ObservationState> finalizeAction = node_async(state -> Map.of(
                ObservationState.KEY_TRAIL, List.of("entered:" + NODE_FINALIZE)
        ));

        final StateGraph<ObservationState> graphBuilder =
                new StateGraph<>(ObservationState.SCHEMA, ObservationState::new)
                        .addNode(NODE_DRAFT, draftAction)
                        .addNode(NODE_HUMAN_GATE, humanGateAction)
                        .addNode(NODE_FINALIZE, finalizeAction)
                        .addEdge(START, NODE_DRAFT)
                        .addEdge(NODE_DRAFT, NODE_HUMAN_GATE)
                        .addEdge(NODE_HUMAN_GATE, NODE_FINALIZE)
                        .addEdge(NODE_FINALIZE, END);

        final BaseCheckpointSaver saver =
                new FileSystemSaver(checkpointDirectory, graphBuilder.getStateSerializer());

        final CompileConfig compileConfig = CompileConfig.builder()
                .checkpointSaver(saver)
                .interruptBefore(NODE_HUMAN_GATE)
                // 关掉线程释放。默认值(true)会在执行收尾时把 checkpoint 归档成 -vN 快照并删掉原文件，
                // 那会让「新进程直接续跑」观察不到；归档行为本身留作后续观察项。
                .releaseThread(false)
                .build();

        return graphBuilder.compile(compileConfig);
    }
}
