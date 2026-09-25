package sandbox.observation;

import org.bsc.langgraph4j.state.AgentState;
import org.bsc.langgraph4j.state.Channel;
import org.bsc.langgraph4j.state.Channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 观察项 ③（决策证据 / trace 的组织）用的状态。
 *
 * <p>字段刻意照搬调研报告 §3.1 的五个「留痕下限项」——模型标识 / 输入快照 / 原始输出 /
 * 判定依据 / 置信度——好让观察变成一句可判的对比：**这五项在框架里有没有自己的位置，
 * 还是只能由应用塞进普通状态值**。
 */
public class EvidenceState extends AgentState {

    public static final String KEY_MODEL_ID = "modelId";

    public static final String KEY_INPUT_SNAPSHOT = "inputSnapshot";

    public static final String KEY_RAW_OUTPUT = "rawOutput";

    public static final String KEY_RATIONALE = "rationale";

    public static final String KEY_CONFIDENCE = "confidence";

    public static final String KEY_TRACE = "evidenceTrace";

    public static final Map<String, Channel<?>> SCHEMA = Map.of(
            KEY_MODEL_ID, Channels.<String>base(() -> ""),
            KEY_INPUT_SNAPSHOT, Channels.<String>base(() -> ""),
            KEY_RAW_OUTPUT, Channels.<String>base(() -> ""),
            KEY_RATIONALE, Channels.<String>base(() -> ""),
            KEY_CONFIDENCE, Channels.<String>base(() -> ""),
            KEY_TRACE, Channels.<String>appender(ArrayList::new)
    );

    public EvidenceState(final Map<String, Object> initData) {
        super(initData);
    }

    public Optional<String> modelId() {
        return value(KEY_MODEL_ID);
    }

    public Optional<String> inputSnapshot() {
        return value(KEY_INPUT_SNAPSHOT);
    }

    public Optional<String> rawOutput() {
        return value(KEY_RAW_OUTPUT);
    }

    public Optional<String> rationale() {
        return value(KEY_RATIONALE);
    }

    public Optional<String> confidence() {
        return value(KEY_CONFIDENCE);
    }

    public List<String> trace() {
        return this.<List<String>>value(KEY_TRACE).orElseGet(ArrayList::new);
    }
}
