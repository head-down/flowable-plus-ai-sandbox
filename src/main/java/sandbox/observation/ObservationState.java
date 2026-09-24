package sandbox.observation;

import org.bsc.langgraph4j.state.AgentState;
import org.bsc.langgraph4j.state.Channel;
import org.bsc.langgraph4j.state.Channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 观察用的最小状态：草稿 -> 人工闸门 -> 定稿。
 *
 * <p>字段刻意压到最少 —— 形态证据不该来自业务复杂度。
 */
public class ObservationState extends AgentState {

    public static final String KEY_DRAFT = "draft";

    public static final String KEY_HUMAN_DECISION = "humanDecision";

    public static final String KEY_TRAIL = "trail";

    public static final Map<String, Channel<?>> SCHEMA = Map.of(
            KEY_DRAFT, Channels.<String>base(() -> ""),
            KEY_HUMAN_DECISION, Channels.<String>base(() -> ""),
            KEY_TRAIL, Channels.<String>appender(ArrayList::new)
    );

    public ObservationState(final Map<String, Object> initData) {
        super(initData);
    }

    public Optional<String> draft() {
        return value(KEY_DRAFT);
    }

    public Optional<String> humanDecision() {
        return value(KEY_HUMAN_DECISION);
    }

    public List<String> trail() {
        return this.<List<String>>value(KEY_TRAIL).orElseGet(ArrayList::new);
    }
}
