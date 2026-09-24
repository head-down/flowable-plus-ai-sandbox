package sandbox.observation;

import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.action.InterruptionMetadata;

/**
 * 把 {@code graph.stream(...)} 产出的事件压成一行可读的形态描述。
 *
 * <p>{@link GraphResult} 是框架自带的统一结果类型 —— 它本身也是观察项 ③ 的一部分：
 * 框架对「一次执行产出了什么」有自己的分类。
 */
public final class EventLog {

    private EventLog() {
    }

    public static String describe(final GraphResult result) {
        if (result.isInterruptionMetadata()) {
            final InterruptionMetadata<ObservationState> metadata = result.asInterruptionMetadata();
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
