package sandbox.observation;

import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.action.InterruptionMetadata;
import org.bsc.langgraph4j.state.AgentState;

/**
 * 把 {@code graph.stream(...)} 产出的事件压成一行可读的形态描述。
 *
 * <p>{@link GraphResult} 是框架自带的统一结果类型 —— 它本身也是观察项 ③ 的一部分：
 * 框架对「一次执行产出了什么」有自己的分类。
 *
 * <p>有意压到 {@link AgentState} 而不是某个具体 state 类型：本沙盒每个观察项各有一套状态，
 * 事件描述的形态与状态类型无关。
 */
public final class EventLog {

    private EventLog() {
    }

    public static String describe(final GraphResult result) {
        if (result.isInterruptionMetadata()) {
            final InterruptionMetadata<AgentState> metadata = result.asInterruptionMetadata();
            return "type=%s | interrupted at node=%s | reason=%s | state=%s".formatted(
                    result.type(),
                    metadata.nodeId(),
                    metadata.reason().orElse("<none>"),
                    metadata.state().data());
        }
        if (result.isNodeOutput()) {
            return "type=%s | node=%s | state=%s".formatted(
                    result.type(),
                    result.asNodeOutput().node(),
                    result.asNodeOutput().state().data());
        }
        return "type=%s | payload=%s".formatted(result.type(), result.result());
    }
}
