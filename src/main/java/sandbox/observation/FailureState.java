package sandbox.observation;

import org.bsc.langgraph4j.state.AgentState;
import org.bsc.langgraph4j.state.Channel;
import org.bsc.langgraph4j.state.Channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 观察项 ⑤（错误降级与 Kill Switch 的接缝）用的状态。
 *
 * <p>字段只有一个目的：让「控制权落到谁手里」这件事在 log 里可读 ——
 * {@code decision} 是决策值，{@code humanDecision} 是人写进来的那一份，
 * {@code trail} 是节点各自留下的轨迹。
 */
public class FailureState extends AgentState {

    public static final String KEY_DECISION = "decision";

    public static final String KEY_HUMAN_DECISION = "humanDecision";

    public static final String KEY_TRAIL = "trail";

    public static final Map<String, Channel<?>> SCHEMA = Map.of(
            KEY_DECISION, Channels.<String>base(() -> ""),
            KEY_HUMAN_DECISION, Channels.<String>base(() -> ""),
            KEY_TRAIL, Channels.<String>appender(ArrayList::new)
    );

    public FailureState(final Map<String, Object> initData) {
        super(initData);
    }

    public Optional<String> decision() {
        return value(KEY_DECISION);
    }

    public Optional<String> humanDecision() {
        return value(KEY_HUMAN_DECISION);
    }

    public List<String> trail() {
        return this.<List<String>>value(KEY_TRAIL).orElseGet(ArrayList::new);
    }
}
