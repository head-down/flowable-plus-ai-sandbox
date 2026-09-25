package sandbox.observation;

import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.CompileConfig;
import org.bsc.langgraph4j.CompiledGraph;
import org.bsc.langgraph4j.GraphInput;
import org.bsc.langgraph4j.GraphInterruptException;
import org.bsc.langgraph4j.GraphResult;
import org.bsc.langgraph4j.GraphRunnerException;
import org.bsc.langgraph4j.NodeOutput;
import org.bsc.langgraph4j.RunnableConfig;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.checkpoint.FileSystemSaver;
import org.bsc.langgraph4j.hook.NodeHook;
import org.bsc.langgraph4j.hook.RetryPolicy;
import org.bsc.langgraph4j.state.StateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.bsc.langgraph4j.StateGraph.END;
import static org.bsc.langgraph4j.StateGraph.START;
import static org.bsc.langgraph4j.action.AsyncNodeActionWithConfig.node_async;

/**
 * 观察项 ⑤：出错时控制权落在谁手里。
 *
 * <p>四个探针，各造一种「决策源没能给出可用的决策」：
 *
 * <ul>
 *   <li>A 瞬时失败 → 框架自带的重试策略救回来；</li>
 *   <li>B 重试耗尽 → 失败去哪了、状态被推进到哪；</li>
 *   <li>C 节点抛 {@code GraphInterruptException} → 错误变成中断，控制权交还给人；</li>
 *   <li>D 取消（kill switch）→ 消费方看到的是什么。</li>
 * </ul>
 *
 * <p>四个探针共用一份 checkpoint 目录、各自的 threadId —— 盘上的东西也是观察对象。
 */
public final class Phase5FailureSeams {

    private static final Logger LOG = LoggerFactory.getLogger(Phase5FailureSeams.class);

    private static final String NODE_CALL_SOURCE = "call_decision_source";

    private static final String NODE_HUMAN_GATE = "human_gate";

    private static final String NODE_FINALIZE = "finalize";

    private static final String NODE_SLOW = "slow_decision_source";

    private static final String THREAD_RETRY_OK = "probe-a-retry-then-success";

    private static final String THREAD_RETRY_EXHAUSTED = "probe-b-retry-exhausted";

    private static final String THREAD_ERROR_AS_INTERRUPT = "probe-c-error-as-interrupt";

    private static final String THREAD_CANCEL = "probe-d-cancel";

    private static final String DECISION_APPROVE = "APPROVE";

    private static final String HUMAN_DECISION_OVERRIDE = "OVERRIDDEN_BY_HUMAN";

    private static final String REFUSAL_REASON = "MODEL_DECLINED: mock refusal";

    private static final int RETRY_MAX_ATTEMPTS = 3;

    private static final int ATTEMPTS_BEFORE_SUCCESS = 2;

    private static final long RETRY_DELAY_MILLIS = 20L;

    private static final long SLOW_NODE_MILLIS = 3000L;

    private static final long POST_CANCEL_OBSERVE_MILLIS = 4000L;

    private static final int MAX_CAUSE_DEPTH = 10;

    private Phase5FailureSeams() {
    }

    public static void main(final String[] args) {
        final long pid = ProcessHandle.current().pid();
        LOG.info("=== PHASE 5 | failure degrade & kill-switch seams | JVM | pid={} ===", pid);
        LOG.info("checkpoint directory = {}", Checkpoints.DIRECTORY.toAbsolutePath());

        runProbe("A", Phase5FailureSeams::probeRetryThenSuccess);
        runProbe("B", Phase5FailureSeams::probeRetryExhausted);
        runProbe("C", Phase5FailureSeams::probeErrorAsInterrupt);
        runProbe("D", Phase5FailureSeams::probeCancel);

        LOG.info("=== PHASE 5 done ===");
    }

    /** 探针体允许抛受检异常；驱动层统一收口。 */
    private interface Probe {
        void run() throws Exception;
    }

    private static void runProbe(final String name, final Probe probe) {
        try {
            probe.run();
        } catch (Throwable escaped) {
            // 取证台的边界必须接住一切：「框架到底会抛出什么」正是本阶段的观察对象本身。
            logThrowable("PROBE " + name + " | escaped the probe", escaped);
        }
    }

    // ---------------------------------------------------------------- PROBE A

    private static void probeRetryThenSuccess() throws Exception {
        LOG.info("--- PROBE A | retry policy: transient failure then success | threadId={} ---", THREAD_RETRY_OK);

        final AtomicInteger attempts = new AtomicInteger();
        final StateGraph<FailureState> builder = new StateGraph<>(FailureState.SCHEMA, FailureState::new)
                .addNode(NODE_CALL_SOURCE, node_async((state, config) -> {
                    final int attempt = attempts.incrementAndGet();
                    LOG.info("call_decision_source | attempt={}", attempt);
                    if (attempt <= ATTEMPTS_BEFORE_SUCCESS) {
                        throw new IOException("transient outbound failure, attempt=" + attempt);
                    }
                    return Map.of(
                            FailureState.KEY_DECISION, DECISION_APPROVE,
                            FailureState.KEY_TRAIL, List.of("decided@attempt=" + attempt)
                    );
                }))
                .addEdge(START, NODE_CALL_SOURCE)
                .addEdge(NODE_CALL_SOURCE, END);
        builder.addWrapCallNodeHook(NODE_CALL_SOURCE, retryPolicy(RETRY_MAX_ATTEMPTS));

        final CompiledGraph<FailureState> graph = builder.compile(compileConfig(builder));
        final RunnableConfig config = RunnableConfig.builder().threadId(THREAD_RETRY_OK).build();

        consume(graph.stream(Map.of(), config), "A");

        final StateSnapshot<FailureState> snapshot = graph.getState(config);
        LOG.info("[A] app-side attempts = {}", attempts.get());
        LOG.info("[A] getState | node={} | next={}", snapshot.node(), snapshot.next());
        logState("[A] getState", snapshot);
        logHistory(graph, config, "A");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);
    }

    // ---------------------------------------------------------------- PROBE B

    private static void probeRetryExhausted() throws Exception {
        LOG.info("--- PROBE B | retry exhausted: where does the failure go | threadId={} ---",
                THREAD_RETRY_EXHAUSTED);

        final AtomicInteger attempts = new AtomicInteger();
        final StateGraph<FailureState> builder = new StateGraph<>(FailureState.SCHEMA, FailureState::new)
                .addNode(NODE_CALL_SOURCE, node_async((state, config) -> {
                    final int attempt = attempts.incrementAndGet();
                    LOG.info("call_decision_source | attempt={} | about to fail", attempt);
                    throw new IOException("permanent outbound failure, attempt=" + attempt);
                }))
                .addEdge(START, NODE_CALL_SOURCE)
                .addEdge(NODE_CALL_SOURCE, END);
        builder.addWrapCallNodeHook(NODE_CALL_SOURCE, retryPolicy(RETRY_MAX_ATTEMPTS));

        final CompiledGraph<FailureState> graph = builder.compile(compileConfig(builder));
        final RunnableConfig config = RunnableConfig.builder().threadId(THREAD_RETRY_EXHAUSTED).build();

        final AsyncGenerator.Cancellable<NodeOutput<FailureState>> stream = graph.stream(Map.of(), config);
        try {
            consume(stream, "B");
            LOG.info("[B] consumer finished WITHOUT any exception —— 失败被静默掉了");
        } catch (CompletionException observed) {
            logThrowable("[B] consumer saw", observed);
            final Optional<GraphRunnerException> runner = GraphRunnerException.of(observed);
            final String nodeId = runner.flatMap(GraphRunnerException::nodeId).orElse("<none>");
            LOG.info("[B] GraphRunnerException | present={} | nodeId={}", runner.isPresent(), nodeId);
        }

        final GraphResult done = GraphResult.from(stream);
        LOG.info("[B] app-side attempts = {}", attempts.get());
        LOG.info("[B] GraphResult.from(stream).type() = {}", done.type());

        final StateSnapshot<FailureState> snapshot = graph.getState(config);
        LOG.info("[B] getState | node={} | next={}", snapshot.node(), snapshot.next());
        logState("[B] getState", snapshot);
        logHistory(graph, config, "B");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);
    }

    // ---------------------------------------------------------------- PROBE C

    private static void probeErrorAsInterrupt() throws Exception {
        LOG.info("--- PROBE C | node throws GraphInterruptException -> interruption | threadId={} ---",
                THREAD_ERROR_AS_INTERRUPT);

        final StateGraph<FailureState> builder = new StateGraph<>(FailureState.SCHEMA, FailureState::new)
                .addNode(NODE_CALL_SOURCE, node_async((state, config) -> {
                    if (state.humanDecision().isPresent()) {
                        final String human = state.humanDecision().get();
                        LOG.info("call_decision_source | human decision present, 节点不再拒绝 | humanDecision={}", human);
                        return Map.of(
                                FailureState.KEY_DECISION, DECISION_APPROVE,
                                FailureState.KEY_TRAIL, List.of("resumed-after-human=" + human)
                        );
                    }
                    LOG.info("call_decision_source | 决策源拒答 -> throw GraphInterruptException");
                    throw new GraphInterruptException(config, REFUSAL_REASON);
                }))
                .addNode(NODE_HUMAN_GATE, node_async((state, config) -> Map.of(
                        FailureState.KEY_TRAIL, List.of("entered:" + NODE_HUMAN_GATE)
                )))
                .addNode(NODE_FINALIZE, node_async((state, config) -> Map.of(
                        FailureState.KEY_TRAIL, List.of("entered:" + NODE_FINALIZE)
                )))
                .addEdge(START, NODE_CALL_SOURCE)
                .addEdge(NODE_CALL_SOURCE, NODE_HUMAN_GATE)
                .addEdge(NODE_HUMAN_GATE, NODE_FINALIZE)
                .addEdge(NODE_FINALIZE, END);

        final CompiledGraph<FailureState> graph = builder.compile(compileConfig(builder));
        final RunnableConfig config = RunnableConfig.builder().threadId(THREAD_ERROR_AS_INTERRUPT).build();

        LOG.info("[C1] 首次运行：决策源抛异常");
        consume(graph.stream(Map.of(), config), "C1");
        final StateSnapshot<FailureState> afterRefusal = graph.getState(config);
        LOG.info("[C1] getState | node={} | next={}", afterRefusal.node(), afterRefusal.next());
        logHistory(graph, config, "C1");

        LOG.info("[C2] 不做任何人工动作，直接续跑 —— 看框架记不记得「停在哪一步」");
        consume(graph.stream(GraphInput.resume(), config), "C2");

        LOG.info("[C3] 人工写一份覆盖决定，再续跑");
        final RunnableConfig updated = graph.updateState(
                config, Map.of(FailureState.KEY_HUMAN_DECISION, HUMAN_DECISION_OVERRIDE), null);
        consume(graph.stream(GraphInput.resume(), updated), "C3");

        final StateSnapshot<FailureState> finalSnapshot = graph.lastStateOf(updated).orElseThrow();
        LOG.info("[C3] lastStateOf | node={} | next={}", finalSnapshot.node(), finalSnapshot.next());
        logState("[C3] lastStateOf", finalSnapshot);
        logHistory(graph, updated, "C3");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);
    }

    // ---------------------------------------------------------------- PROBE D

    private static void probeCancel() throws Exception {
        LOG.info("--- PROBE D | cancel (kill switch) while a node is running | threadId={} ---", THREAD_CANCEL);

        final AtomicBoolean nodeFinished = new AtomicBoolean(false);
        final AtomicBoolean nodeInterrupted = new AtomicBoolean(false);

        final StateGraph<FailureState> builder = new StateGraph<>(FailureState.SCHEMA, FailureState::new)
                .addNode(NODE_SLOW, node_async((state, config) -> {
                    LOG.info("[D] slow_decision_source | started, will sleep {} ms", SLOW_NODE_MILLIS);
                    try {
                        Thread.sleep(SLOW_NODE_MILLIS);
                    } catch (InterruptedException e) {
                        nodeInterrupted.set(true);
                        final String message = e.toString();
                        LOG.info("[D] slow_decision_source | interrupted while sleeping | {}", message);
                        throw e;
                    }
                    nodeFinished.set(true);
                    LOG.info("[D] slow_decision_source | finished sleeping, about to commit state");
                    return Map.of(
                            FailureState.KEY_DECISION, DECISION_APPROVE,
                            FailureState.KEY_TRAIL, List.of("slow:done")
                    );
                }))
                .addEdge(START, NODE_SLOW)
                .addEdge(NODE_SLOW, END);

        final CompiledGraph<FailureState> graph = builder.compile(compileConfig(builder));
        final RunnableConfig config = RunnableConfig.builder().threadId(THREAD_CANCEL).build();

        final AsyncGenerator.Cancellable<NodeOutput<FailureState>> stream = graph.stream(Map.of(), config);
        final Iterator<NodeOutput<FailureState>> iterator = stream.iterator();
        while (iterator.hasNext()) {
            final NodeOutput<FailureState> output = iterator.next();
            final Object state = output.state().data();
            LOG.info("[D] stream event | node={} | state={}", output.node(), state);
            if (output.isSTART()) {
                final boolean cancelled = stream.cancel(true);
                LOG.info("[D] stream.cancel(true) returned {}", cancelled);
            }
        }
        LOG.info("[D] iteration ended | nodeFinished={} | nodeInterrupted={}", nodeFinished.get(), nodeInterrupted.get());

        Thread.sleep(POST_CANCEL_OBSERVE_MILLIS);
        LOG.info("[D] {} ms after cancel | nodeFinished={} | nodeInterrupted={}",
                POST_CANCEL_OBSERVE_MILLIS, nodeFinished.get(), nodeInterrupted.get());

        final GraphResult done = GraphResult.from(stream);
        LOG.info("[D] GraphResult.from(stream).type() = {}", done.type());

        final StateSnapshot<FailureState> snapshot = graph.getState(config);
        LOG.info("[D] getState | node={} | next={}", snapshot.node(), snapshot.next());
        logState("[D] getState", snapshot);
        logHistory(graph, config, "D");
        CheckpointFiles.log(LOG, Checkpoints.DIRECTORY);
    }

    // ---------------------------------------------------------------- helpers

    private static CompileConfig compileConfig(final StateGraph<FailureState> builder) {
        return CompileConfig.builder()
                .checkpointSaver(new FileSystemSaver(Checkpoints.DIRECTORY, builder.getStateSerializer()))
                .releaseThread(false)
                .build();
    }

    private static NodeHook.WrapCall<FailureState> retryPolicy(final int maxAttempts) {
        return RetryPolicy.builder()
                .maxAttempts(maxAttempts)
                .retryDelay(Duration.ofMillis(RETRY_DELAY_MILLIS))
                .retryOn(IOException.class)
                .build()
                .asHook();
    }

    private static void consume(final AsyncGenerator.Cancellable<NodeOutput<FailureState>> stream, final String probe) {
        for (final Object raw : stream) {
            final GraphResult result = GraphResult.from(raw);
            LOG.info("[{}] stream event | {}", probe, EventLog.describe(result));
        }
        final GraphResult done = GraphResult.from(stream);
        LOG.info("[{}] stream result value (generator done) | {}", probe, EventLog.describe(done));
    }

    private static void logState(final String label, final StateSnapshot<FailureState> snapshot) {
        final String state = String.valueOf(snapshot.state().data());
        LOG.info("{} | stateData={}", label, state);
    }

    private static void logHistory(final CompiledGraph<FailureState> graph, final RunnableConfig config,
                                   final String probe) {
        final Collection<StateSnapshot<FailureState>> history = graph.getStateHistory(config);
        for (final StateSnapshot<FailureState> entry : history) {
            final String state = String.valueOf(entry.state().data());
            LOG.info("[{}] history entry | node={} | next={} | stateData={}", probe, entry.node(), entry.next(), state);
        }
        LOG.info("[{}] history size = {}", probe, history.size());
    }

    private static void logThrowable(final String label, final Throwable throwable) {
        Throwable current = throwable;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_DEPTH) {
            final String type = current.getClass().getName();
            final String message = String.valueOf(current.getMessage());
            LOG.info("{} | depth={} | type={} | message={}", label, depth, type, message);
            current = current.getCause();
            depth++;
        }
    }
}
