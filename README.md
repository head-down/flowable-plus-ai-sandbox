# flowable-plus-ai-sandbox

> ## ⚠️ 探索沙盒 —— 非交付物
>
> - **不承诺可运行**：本仓的代码是**取证耗材**，随时可能坏死、随时可能重写。
> - **随时弃用 / 不维护**：不接需求、不做版本管理、不保证构建通过。
> - **不构成任何 API、行为或选型承诺**：结论只以**观察记录**为准，代码本身不是结论。
>
> 请勿把本仓用作依赖、示例或选型依据。

## 用途

把第三方 Agent 框架的**真实行为**跑出来，留下**原始 log**，回答一组**形态问题**：

| 编号 | 观察项 | 要回答什么 |
|---|---|---|
| ① | HITL 接缝的位置与粒度 | 人工介入的闸门开在**哪一层**、颗粒度多粗 |
| ② | 「挂起—续跑」的形态与代价 | 状态**存在哪**、恢复**由谁发起**、跨时长等待怎么表达 |
| ③ | 决策证据 / trace 的组织 | 框架把什么留成痕、留成什么形态 |
| ⑤ | 错误降级与 Kill Switch 的接缝 | 出错时控制权如何交还给规则引擎或人 |

**观察记录不在这里。** 记录的第一住所是 `head-down/flowable-plus-labs` 的 `docs/research/`，
本仓只负责**产生 log**。

## 与 flowable-plus 的关系

- `head-down/flowable-plus` 本体是 **Java 8 + Spring Boot 2.7.18 + Flowable 6.8.0**。
- 本沙盒是**并列的 Java 17 观察台**，**不是**升级路径，也**不**回填主仓。
- 它服务的是 `head-down/flowable-plus-labs` 里「AI 决策接入」的设计论证：
  论证需要形态证据，形态证据需要真跑。

## 怎么跑

**执行面 = GitHub Actions**（`.github/workflows/observe.yml`）。
本地**不需要** JDK 17，也不需要任何模型 API 凭据 —— 观察用桩，不用真模型。

```bash
# 本地若已有 JDK 17：
mvn -B compile
mvn -B exec:java -Dexec.mainClass=sandbox.observation.Phase1RunToInterrupt      # ① / ②
mvn -B exec:java -Dexec.mainClass=sandbox.observation.Phase2ResumeInNewProcess # ① / ②
mvn -B exec:java -Dexec.mainClass=sandbox.observation.Phase3WriteEvidence      # ③
mvn -B exec:java -Dexec.mainClass=sandbox.observation.Phase4ReadEvidenceInNewProcess # ③
mvn -B exec:java -Dexec.mainClass=sandbox.observation.Phase5FailureSeams       # ⑤
```

阶段 1 / 2 与阶段 3 / 4 各自是**两个独立 JVM** —— 这正是「跨进程恢复」的取证点：前一阶段跑到中断后**进程结束**，
后一阶段在**新进程**里从磁盘上的 checkpoint 恢复。阶段 5 的四个探针在**同一进程**里跑（重试 / 失败外抛 /
异常转中断 / 取消），只靠不同的 `threadId` 分开各自的盘上痕迹。

## 当前观察对象

`LangGraph4j` `org.bsc.langgraph4j:langgraph4j-core:1.9.1`（Java 17+）。
选它是因为它是 Java 侧少数把 **interrupt + checkpointer 都做成一等**的候选
（另见 `head-down/flowable-plus-labs` 的对应票据）。

## 内容纪律

本仓可公开，故一律按可公开标准书写：**不出现**真实下游项目名 / 公司名 / 人名。
