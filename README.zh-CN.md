# Java Pipeline 框架

轻量级、可嵌入、零依赖的通用任务流水线（Pipeline）框架，运行环境 JDK 17+。

它把一个业务过程（订单处理、数据 ETL、审批流、消息处理）拆分为**有序、独立、可复用、可插拔**的处理阶段，
由框架统一负责编排、数据流转、横切关注点、容错与流程控制。

参考模型：Netty Pipeline（责任链 + 上下文）、Servlet Filter（拦截器链）、简单工作流引擎。

- [English](README.md)

基于 [Apache License 2.0](LICENSE) 开源。

## 目录

- [特性](#特性)
- [设计目标](#设计目标)
- [总体架构](#总体架构)
- [核心抽象与设计原则](#核心抽象与设计原则)
- [包结构](#包结构)
- [构建](#构建)
- [快速开始](#快速开始)
- [编写阶段](#编写阶段)
- [数据传递模型](#数据传递模型)
- [流程控制：停止 / 跳过 / 取消 / 超时](#流程控制停止--跳过--取消--超时)
- [错误处理](#错误处理)
- [拦截器](#拦截器)
- [循环编排](#循环编排)
- [finallyStage 收尾](#finallystage-收尾)
- [异步执行与运行时查询](#异步执行与运行时查询)
- [状态模型](#状态模型)
- [轨迹与导出器](#轨迹与导出器)
- [线程模型](#线程模型)
- [示例一：订单流水线](#示例一订单流水线)
- [示例二：录像循环流水线](#示例二录像循环流水线)

## 特性

| 能力 | 状态 |
|------|------|
| 阶段顺序执行 + 共享上下文（payload + attrs） | 已实现 |
| 阶段级条件路由（`accept`） | 已实现 |
| 流程控制：`abort()` / `skipRest()` / `exitLoop()` | 已实现 |
| 拦截器（`beforeStage` / `afterStage` / `onStageError`） | 已实现 |
| 错误策略：全局 `ErrorHandler` + 循环级覆盖 | 已实现 |
| `LoopStage`：`while_` / `maxIterations` / 按圈轨迹 | 已实现 |
| `finallyStage` 任何退出路径都执行 | 已实现 |
| `executeAsync` + `ExecutionHandle`（状态 / 当前阶段 / 圈数 / 取消 / 等待） | 已实现 |
| `CancelToken` 协作式取消、单次执行超时 | 已实现 |
| 轨迹采集 + Log / JSON / Mermaid 导出器 | 已实现 |
| 事件监听（`PipelineListener` / `PipelineEvent`） | 已实现 |
| 重试策略、YAML DSL、子流水线、并行阶段（DAG） | 规划中 |

## 设计目标

- **编排**：阶段的注册、排序、顺序执行
- **数据流转**：跨阶段共享的上下文（Context）
- **横切关注点**：日志、耗时、审计 → 拦截器统一处理，业务阶段保持纯粹
- **容错**：某个阶段失败后继续还是中断 → 策略可配
- **流程控制**：支持提前中断（如风控拒绝）、跳过剩余阶段

## 总体架构（三层）

```text
┌─────────────────── API 层（使用者面对）───────────────────┐
│  PipelineBuilder（链式构建）  Stage 实现（业务阶段）  execute() 启动  │
├─────────────────── 核心引擎层 ────────────────────────────┤
│  Pipeline（编排调度）  StageContext（上下文+流程控制）           │
│  Interceptor（AOP 拦截）  ErrorHandler（异常策略）              │
├─────────────────── 扩展模块层（按需引入）────────────────────┤
│  Trace/Exporter ｜ Retry/Timeout ｜ Listener ｜ YAML DSL ｜ Metrics │
└──────────────────────────────────────────────────────────┘
```

## 核心抽象与设计原则

| 抽象 | 职责 | 关键成员 |
|------|------|----------|
| `Stage<C>` | 最小处理单元，一个阶段做一件事 | `name()` / `accept(ctx)` 条件路由 / `handle(ctx)` |
| `StageContext<C>` | 数据载体 + 流程控制器：payload、attrs、abort/skip/exit 标志 | `payload()` / `setAttr` / `getAttr` / `abort()` / `skipRest()` / `exitLoop()` |
| `Pipeline<C>` | 持有阶段列表，负责执行、拦截、异常分流 | `execute(C)` / `executeAsync(C)` / `executeAsync(C, Executor)` |
| `ExecutionHandle<C>` | 异步执行的运行时句柄 | `state()` / `currentStage()` / `iteration()` / `cancel()` / `await()` / `future()` |
| `Interceptor<C>` | 对每个阶段的环绕切面 | `beforeStage` / `afterStage` / `onStageError` |
| `ErrorHandler<C>` | 阶段抛异常后的决策：`CONTINUE` / `ABORT` | `onError(stage, ctx, e)` |
| `PipelineBuilder<C>` | 链式装配，`build()` 产出不可变 `Pipeline` | `addStage` / `loop` / `finallyStage` / `errorHandler` / `build` |

**设计原则**：

1. **接口最小化** —— 核心只有 5 个抽象，`Stage` 是唯一必实现接口（函数式接口，lambda 即可用）
2. **不可变流水线** —— `build()` 之后阶段列表只读，流水线可并发复用、无状态
3. **控制流显式化** —— 中断/跳过通过 Context 标志位而非异常控制流，异常只用于真正的错误
4. **泛型 `<C>` 贯穿** —— 同一套引擎可跑订单上下文、文件上下文、消息上下文

## 包结构

```text
src/com/devin/pipeline/
├── core/                    引擎与核心抽象
│   ├── Stage.java           业务代码唯一必须实现的接口
│   ├── AsyncStage.java      返回 CompletableFuture 的阶段（由引擎 join）
│   ├── StageContext.java     payload + attrs + 流程控制，每次执行一个
│   ├── Pipeline.java        引擎：execute / executeAsync / LoopStage / StageRunner
│   ├── ExecutionHandle.java 异步运行时句柄（状态、当前阶段、取消、等待）
│   ├── control/             流程控制与策略
│   │   ├── CancelToken.java     外部取消标志位（volatile）
│   │   ├── ErrorAction.java     CONTINUE / ABORT
│   │   ├── ErrorHandler.java    阶段失败后的决策
│   │   └── Interceptor.java     每个阶段的 AOP 切面
│   ├── execution/           状态与结果
│   │   ├── ExecutionState.java  PENDING / RUNNING / COMPLETED / ABORTED / FAILED / CANCELLED / TIMEOUT
│   │   ├── StageStatus.java     SKIPPED / SUCCESS / FAILED / CANCELLED
│   │   ├── FlowReason.java      统一退出原因（type + code + message）
│   │   └── ExecutionResult.java 终态 + 轨迹 + payload
│   ├── trace/               轨迹数据模型与导出 SPI
│   │   ├── ExecutionTrace.java  一次执行的完整轨迹
│   │   ├── StageRecord.java     单个阶段的执行记录
│   │   └── TraceExporter.java   导出器接口
│   └── event/               事件与监听
│       ├── PipelineEvent.java    结构化阶段/状态事件
│       └── PipelineListener.java 监听回调
├── builder/
│   └── PipelineBuilder.java   链式装配（addStage / loop / finallyStage / ...）
└── exporter/                 内置导出器
    ├── LogTraceExporter.java   结构化日志（默认）
    ├── JsonTraceExporter.java  trace-<id>.json
    └── MermaidExporter.java    trace-<id>.md 流程图
```

## 构建

```bash
# 编译（JDK 17+）
javac -encoding UTF-8 -d out/classes $(find src -name "*.java")

# Maven
mvn -q compile
```

框架本身零运行时依赖；导出器的输出目录由调用方指定（示例中使用 `out/traces/`）。

## 快速开始

```java
Pipeline<Order> pipeline = PipelineBuilder.<Order>of("order-pipeline")
        .addStage(Stage.of("validate input", ctx -> { /* ... */ }))
        .addStage(Stage.of("risk check", ctx -> {
            if (riskIsHigh(ctx)) {
                ctx.abort("risk rejected");
            }
        }))
        .addStage(Stage.of("deduct inventory", ctx -> ctx.payload().amount--))
        .loop(loop -> loop.name("settle")
                .stages(fetchStream, encode)
                .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
                .maxIterations(100_000)
                .onIterationError(ErrorAction.CONTINUE))
        .finallyStage(Stage.of("close resources", ctx -> closeAll(ctx)))
        .timeout(5_000)
        .errorHandler((stage, ctx, e) -> ErrorAction.ABORT)
        .addInterceptor(timingInterceptor)
        .addListener(auditListener)
        .addExporter(new LogTraceExporter())
        .addExporter(new JsonTraceExporter(Path.of("out", "traces")))
        .addExporter(new MermaidExporter(Path.of("out", "traces")))
        .build();

ExecutionResult<Order> result = pipeline.execute(order);
```

涉及到的导入：

```java
import com.devin.pipeline.builder.PipelineBuilder;
import com.devin.pipeline.core.ExecutionHandle;
import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.StageContext;
import com.devin.pipeline.core.control.CancelToken;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.control.Interceptor;
import com.devin.pipeline.core.event.PipelineEvent;
import com.devin.pipeline.core.event.PipelineListener;
import com.devin.pipeline.core.execution.ExecutionResult;
import com.devin.pipeline.core.execution.ExecutionState;
import com.devin.pipeline.core.execution.FlowReason;
import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.exporter.JsonTraceExporter;
import com.devin.pipeline.exporter.LogTraceExporter;
import com.devin.pipeline.exporter.MermaidExporter;
```

## 编写阶段

```java
// lambda 写法
Stage<Order> validate = Stage.of("validate input", ctx -> {
    Order o = ctx.payload();
    if (o.id == null || o.id.isBlank()) {
        throw new IllegalArgumentException("missing order id");
    }
    ctx.setAttr("riskScore", score(o));       // 传给后续阶段
});

// 类写法 + 条件路由
public final class PayStage implements Stage<Order> {
    @Override public String name() { return "payment"; }

    @Override public boolean accept(StageContext<Order> ctx) {
        return "NEW".equals(ctx.payload().status);   // false -> SKIPPED，不是错误
    }

    @Override public void handle(StageContext<Order> ctx) {
        ctx.payload().status = "PAID";
    }
}

// 异步写法：引擎在阶段返回前统一 join
public final class UploadStage implements AsyncStage<Order> {
    @Override public String name() { return "upload"; }

    @Override public CompletableFuture<Void> handleAsync(StageContext<Order> ctx) {
        return CompletableFuture.runAsync(() -> upload(ctx.payload()));
    }
}
```

引擎内单个阶段的生命周期：

```text
for each stage:
  1. accept(ctx)?            false -> 记录 SKIPPED，继续
  2. interceptor.beforeStage
  3. stage.handle(ctx)       可调用 abort() / skipRest() / exitLoop()
  4. interceptor.afterStage
  5. 异常时: interceptor.onStageError -> errorHandler.onError
             CONTINUE -> 继续下一阶段 | ABORT -> 终止流水线
  6. 在阶段边界检查停止标志（abort / skip / cancel / timeout）
```

## 数据传递模型

**核心机制：单一上下文对象 + 引用传递。** `execute(payload)` 创建**一个** `StageContext<C>`，
同一引用贯穿所有阶段，零拷贝：

| 通道 | 放什么 | 例子 |
|------|--------|------|
| **payload（主通道）** | 被流水线加工的业务主对象 | `Order`：校验阶段补字段、风控阶段打标记、库存阶段改状态 |
| **attrs（辅助通道）** | 跨阶段的中间产物 / 衍生数据 | `riskScore=87`、`payUrl`、生成的流水号 |

阶段就地修改 payload 的字段，后续阶段直接可见——"消息传递"在本框架里等价于
"共享同一个对象的顺序加工"。

**阶段间契约**：契约是弱契约（靠约定），框架不校验类型流。规范做法：

- 用 `TypedKey<T>` 类型安全属性键替代 magic string，键集中定义为常量类（如 `OrderAttrs`），形成阶段间可见的"数据契约表"
- Stage 文档注明：需要哪些前置 attr、产出哪些 attr
- 防御：`accept()` 里可检查前置依赖缺失 → 返回 false 跳过，或快速失败

**为什么选可变引用传递（而非不可变消息）**：Pipeline 的典型负载是"单个大业务对象顺序加工"，
可变引用零拷贝、性能好、写法直观；其"执行后状态难回溯"的短板由轨迹采集（attrs 快照 +
payload 序列化钩子）补齐。单线程串行 + 引用传递 → 阶段间数据可见性由程序顺序保证，无同步成本。

**红线**：阶段内不得把 payload 引用交给其他线程后不在 `handle()` 返回前 join，
否则后续阶段可能读到未完成数据。

**跨流水线传递**：`pipelineA.execute(order)` 结束后，把同一个 `order`（或其 attrs）交给
`pipelineB`——payload 本身就是载体；嵌套子流水线直接共享当前 ctx。

## 流程控制：停止 / 跳过 / 取消 / 超时

**原则：协作式停止（cooperative cancellation）** —— 引擎只在阶段边界检查停止条件，
正在执行的 `handle()` 一定会执行完，不会被框架强杀。

| 触发源 | API | 终态 |
|--------|-----|------|
| 阶段内业务决策 | `ctx.abort()` / `ctx.abort("risk rejected")` / `ctx.skipRest()` | `ABORTED` / `COMPLETED` |
| 错误策略 | `ErrorHandler` 返回 `ABORT` | `FAILED` |
| 外部取消 | `handle.cancel()` 或另一线程 `CancelToken.cancel()` | `CANCELLED` |
| 超时 | 构建器 `.timeout(ms)` | `TIMEOUT` |
| 仅跳出循环 | `ctx.exitLoop()` | 循环结束，流水线继续 |

```java
CancelToken token = new CancelToken();          // 交给消息监听器持有
messageListener.onStop(msg -> token.cancel("remote stop"));
```

「接收某个消息后停止」的典型接线：外部消息监听器 → 持有该次执行的 `CancelToken`
（或 `ExecutionHandle`）→ 收到停止消息时调 `cancel()` → 下一个阶段边界生效。

**检查点时机**：每个阶段**开始前**统一检查（执行循环内唯一判定点）：

```text
继续条件 = !aborted && !skipped && !cancelled && 线程未被 interrupt
```

阶段执行中途不检查（业务需要快速响应取消的，可自检 `token.isCancelled()` 提前返回）。

**停止后的收尾**（三种触发源统一走这条路）：

1. 写入 `ExecutionTrace` 最终状态（COMPLETED / ABORTED / FAILED / CANCELLED）
2. 已执行阶段的 `StageRecord` 全部保留；停止点之后的阶段补记为 SKIPPED / CANCELLED
3. **导出器照常触发** —— 审计必须回答"停在哪、为什么停"
4. `CompletableFuture` 以正常完成收尾（携带最终状态对象），**取消不抛异常**——取消是正常路径的一种结果，不是错误

**interrupt 的能力边界**：`future.cancel(true)` 的中断信号只能打断阻塞操作
（sleep / IO / 阻塞队列 take）；CPU 密集型阶段收不到，需要业务代码自检 `token`。
框架不提供也无法提供"强杀线程"——这是 Java 线程模型的约束。

## 错误处理

```java
.errorHandler((stage, ctx, e) -> {
    log.warn("stage {} failed", stage.name(), e);
    return e instanceof TimeoutException ? ErrorAction.CONTINUE : ErrorAction.ABORT;
})
```

- 默认策略：任何异常直接 `ABORT`。
- 循环内以循环级策略优先：`.onIterationError(ErrorAction.CONTINUE)`（如丢一帧继续录）。
- `onStageError` 先于 `ErrorHandler` 触发，拦截器能看到每一次失败。

## 拦截器

```java
Interceptor<Order> timing = new Interceptor<>() {
    @Override public void beforeStage(Stage<Order> stage, StageContext<Order> ctx) {
        ctx.setAttr("t0", System.nanoTime());
    }

    @Override public void afterStage(Stage<Order> stage, StageContext<Order> ctx) {
        long ms = (System.nanoTime() - ctx.<Long>getAttr("t0")) / 1_000_000;
        log.info("{} took {}ms", stage.name(), ms);
    }
};
```

典型用途：日志埋点、耗时统计、审计、事务开关、灰度检查。
注册顺序即执行顺序：`beforeStage` 正序、`afterStage` 逆序，保证嵌套语义。

## 循环编排

```java
.loop(loop -> loop
        .name("record")
        .stages(capture, encode)                 // 循环体，每圈按序执行
        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
        .maxIterations(100_000)                  // 死循环兜底，触达则 trace 标记 truncated
        .onIterationError(ErrorAction.CONTINUE)) // 循环级错误策略
```

**两种做法的取舍**（为什么选框架级 `LoopStage`）：

| 做法 | 说明 | 问题 |
|------|------|------|
| 单 Stage 内 while 循环 | 循环体塞进一个 handle() | 循环体无法独立 trace / 拦截 / 按阶段限错；一条 trace 记录跑几小时；不可复用 |
| **框架级 LoopStage（采用）** | 循环体是子阶段序列，复用现有执行循环 | 需要循环边界与迭代索引支持（成本低，引擎已具备全部积木） |

语义要点：

- 循环条件在**每圈边界**检查，与停止模型同构（协作式）：外部收到停止消息改 `stopRequested`
  或 cancel token 后，当前圈执行完即退出
- `ctx.exitLoop()` 跳出当前圈但不终止流水线；`abort()` / `skipRest()` 语义照常穿透循环
- `ctx.currentLoop().iteration` 为当前圈数（从 1 开始）；每圈每个阶段各产生一条 `StageRecord`
  （`loop=record, iteration=N`），长任务保留细粒度可观测性
- 达到 `maxIterations` 时轨迹标记 `truncated`，终态为 `COMPLETED`

## finallyStage 收尾

```java
.finallyStage(Stage.of("close camera", ctx -> close(ctx.payload())))
```

资源打开类流水线必须保证清理：**无论正常完成、`abort`、`skipRest`、取消、超时还是异常都会执行**
（语义同 `try-finally`）。多个 finallyStage 按注册顺序执行；其内部异常只记入轨迹，
不覆盖原始失败原因。

与循环的关系：finallyStage 在整条流水线层面；循环内资源的清理建议放进循环体最后一阶段，
或用 abort 穿透 + finally 兜底。

## 异步执行与运行时查询

```java
ExecutionHandle<RecordSession> handle = pipeline.executeAsync(session, executor);

handle.state();          // RUNNING / COMPLETED / ...（volatile 快照）
handle.currentStage();   // 当前正在执行的阶段名
handle.iteration();      // 当前循环圈数，不在循环内为 0
handle.cancel("user requested");
ExecutionResult<RecordSession> result = handle.await();   // 或 handle.future()
```

单个 worker 线程串行跑完所有阶段，语义与 `execute(payload)` 完全一致；同一个 `Pipeline`
实例可被多线程并发执行。

**四种查询手段（按场景选）**：

| 场景 | 手段 | 说明 |
|------|------|------|
| 阶段内自查（业务代码） | `ctx` 上的状态/标志位 | 执行中业务逻辑感知自身进度 |
| 异步提交方轮询 | `handle.state()` / `currentStage()` / `iteration()` | volatile 快照读，适合监控面板定时刷 |
| 等待结果 | `handle.future()`（CompletableFuture）或 `await()` | 完成后返回 `ExecutionResult`（终态 + 耗时 + 轨迹引用） |
| 事件推送 | `PipelineListener.onStateChanged(old, new, reason)` | 状态每次跃迁主动回调，适合接入监控/告警，不用轮询 |
| 事后审计 | 导出的 Json / Mermaid 轨迹文件 | 终态 + 每阶段明细，权威记录 |

同步 `execute()` 没有中间查询问题——调用线程被占用，返回即终态（`ExecutionResult`）。

## 状态模型

```text
PENDING --提交--> RUNNING --+--> COMPLETED   （跑完，或 skipRest 提前结束）
                            +--> ABORTED     （ctx.abort）
                            +--> FAILED      （异常且策略为 ABORT）
                            +--> CANCELLED   （外部取消）
                            +--> TIMEOUT     （超时）
```

- 终态互斥、一次性转换、不可逆。
- 状态机只有执行线程一个写者，状态字段用 volatile 发布，外部线程快照读，无需加锁。
  读到 RUNNING + 某阶段名，说明"读到这一刻它至少进行到了这里"，不承诺纳秒级精确。
- `FlowReason` 把状态与原因分离：`type`（`NORMAL`、`ABORTED`、`FAILED`、`CANCELLED`、`TIMEOUT`、
  `LOOP_TRUNCATED`、`REJECTED`）+ `code` + `message`，便于排障、审计、监控和上报。

**终态判定规则**：

| 触发 | 终态 |
|------|------|
| 所有阶段执行完 / `skipRest()` | COMPLETED |
| 阶段内 `ctx.abort()` | ABORTED |
| 异常 + ErrorHandler 返回 ABORT | FAILED |
| `token.cancel()` / `handle.cancel()` | CANCELLED |
| `.timeout(ms)` 超时 | TIMEOUT |
| `maxIterations` 循环兜底触发 | COMPLETED（但 trace 标记 truncated，提示未自然结束） |

## 轨迹与导出器

导出分为两类，覆盖「给人看」和「给机器用」两种场景：

| 类型 | 内容 | 形式 | 场景 |
|------|------|------|------|
| **动态执行导出** | 执行轨迹：每阶段状态、起止时间、耗时、异常信息 | 结构化日志 / JSON / Mermaid | 排障、审计、监控、复盘 |

每次执行产出一份 `ExecutionTrace`（traceId、流水线名、起止时间、耗时、truncated 标记、
失败原因、`FlowReason`），其中包含每个阶段执行的 `StageRecord`（序号、阶段名、循环名、
圈数、状态、耗时、异常）。

| 导出器 | 产物 |
|--------|------|
| `LogTraceExporter` | 结构化日志：一行汇总 + 每阶段一行（默认开启） |
| `JsonTraceExporter(Path)` | `trace-<id>.json`，供机器 / 日志平台消费 |
| `MermaidExporter(Path)` | `trace-<id>.md`，按状态着色的 flowchart（绿=成功、灰=跳过、红=失败、橙=取消） |

推荐组合：结构化日志（实时排障）+ Mermaid 报告（事后复盘），JSON 留给机器消费。

自定义导出器：

```java
public final class MetricsExporter implements TraceExporter {
    @Override public void export(ExecutionTrace t) {
        for (StageRecord r : t.records()) {
            registry.timer("pipeline.stage", "name", r.stageName()).record(r.durationMs());
        }
    }
}
```

也可以按事件推送：

```java
.addListener(new PipelineListener<>() {
    @Override public void onEvent(PipelineEvent e) {
        auditSink.write(e);   // STAGE_START / STAGE_FINISH / STATE_CHANGED
    }
})
```

**设计要点**：

- 采集在引擎层自动完成，业务 Stage 与 Interceptor **零侵入**
- `ExecutionTrace` / `StageRecord` 是核心层数据模型，`TraceExporter` 及实现可插拔
- traceId 贯穿一次执行，多导出器输出可通过 traceId 关联

## 线程模型

**核心原则：一次执行（execute）= 单线程串行；框架级并发只发生在「多次执行之间」。**

| 形态 | 线程归属 | 说明 |
|------|----------|------|
| `execute(payload)` | 调用者线程全程执行 | 所有阶段、拦截器、错误处理、轨迹采集、导出器都在当前线程跑完才返回，无锁、无竞争 |
| `executeAsync(payload, Executor)` | 单个 worker 线程串行执行全部阶段 | 调用线程只负责提交，阶段间仍是同一线程顺序执行，语义与同步版完全一致 |
| 多次执行并发 | 多个 worker / 调用线程 | 同一个 Pipeline 实例可被并发执行 |

**为什么不需要锁**：

1. `Pipeline` 不可变 —— `build()` 后阶段列表、拦截器、配置全部只读 → 任意线程共享安全
2. `StageContext` 每次执行新建 —— 执行之间零共享状态 → 天然隔离
3. 一次执行内部单线程 —— 阶段间存在天然 happens-before（程序顺序），Context 内部用普通
   `HashMap` 即可，**不需要** ConcurrentHashMap

**需要使用者注意的边界**：

- 阶段内部自起线程：可以并行（如批量调外部接口），但必须在 `handle()` 返回前 join 完
  （或实现 `AsyncStage`），否则 `abort()` 语义会漏掉在途工作、轨迹耗时失真
- 共享外部资源：Stage 引用的有状态对象（连接池、带状态 service）由使用方自行保证线程安全，
  框架只保证「框架自身」无共享
- 导出器线程：默认与执行同线程同步触发（保证 trace 完整后再导出）

**为什么并行阶段（DAG）要谨慎**：一旦引入「一次执行内部多线程并行跑阶段」，上述三个前提
全部失效——Context 属性读写需要同步或按阶段分片隔离、abort 语义要变成协调取消、trace 时序
需要并发收集。因此并行/DAG 排在后续单独评估，不塞进核心引擎。

## 示例一：订单流水线

校验 → 风控 → 扣库存 → 支付 → 通知，最后接一个审计收尾阶段。

```java
import com.devin.pipeline.builder.PipelineBuilder;
import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.execution.ExecutionResult;
import com.devin.pipeline.exporter.JsonTraceExporter;
import com.devin.pipeline.exporter.LogTraceExporter;
import com.devin.pipeline.exporter.MermaidExporter;

import java.nio.file.Path;

public class OrderDemo {

    public static final class Order {
        public String id;
        public int amount;
        public String status = "NEW";
        public int riskScore;

        public Order(String id, int amount) {
            this.id = id;
            this.amount = amount;
        }

        @Override public String toString() {
            return "Order{id='" + id + "', amount=" + amount + ", status='" + status
                    + "', riskScore=" + riskScore + "}";
        }
    }

    public static void main(String[] args) {
        Path traces = Path.of("out", "traces");

        Pipeline<Order> pipeline = PipelineBuilder.<Order>of("order-pipeline")
                .addStage(Stage.of("validate input", ctx -> {
                    Order o = ctx.payload();
                    if (o.id == null || o.id.isBlank()) {
                        throw new IllegalArgumentException("missing order id");
                    }
                    if (o.amount <= 0) {
                        throw new IllegalArgumentException("invalid amount: " + o.amount);
                    }
                }))
                .addStage(Stage.of("risk check", ctx -> {
                    Order o = ctx.payload();
                    o.riskScore = o.amount > 10_000 ? 40 : 85;
                    ctx.setAttr("riskScore", o.riskScore);
                    if (o.riskScore < 60) {
                        ctx.abort("risk rejected");
                    }
                }))
                .addStage(Stage.of("deduct inventory", ctx -> ctx.payload().amount--))
                .addStage(Stage.of("payment", ctx -> ctx.payload().status = "PAID"))
                .addStage(Stage.of("send notification", ctx ->
                        System.out.println("  [notification] order " + ctx.payload().id + " is paid")))
                .finallyStage(Stage.of("audit ledger", ctx ->
                        System.out.println("  [audit] final order state: " + ctx.payload())))
                .errorHandler((stage, ctx, e) -> {
                    System.out.println("  [ErrorHandler] " + stage.name() + " failed: "
                            + e.getMessage() + " -> ABORT");
                    return ErrorAction.ABORT;
                })
                .addExporter(new LogTraceExporter())
                .addExporter(new JsonTraceExporter(traces))
                .addExporter(new MermaidExporter(traces))
                .build();

        ExecutionResult<Order> ok = pipeline.execute(new Order("A001", 3));
        System.out.println("normal: " + ok);

        ExecutionResult<Order> bad = pipeline.execute(new Order("B002", -1));
        System.out.println("invalid amount: " + bad);

        ExecutionResult<Order> risky = pipeline.execute(new Order("C003", 99_999));
        System.out.println("high risk: " + risky);
    }
}
```

覆盖场景：正常完成、参数校验失败（`FAILED`）、风控中断（`ABORTED`）、finallyStage 始终执行。

## 示例二：录像循环流水线

打开摄像头 → [取流 → 编码] × N 圈 → 关闭摄像头，含停止信号与异步取消。

```java
import com.devin.pipeline.builder.PipelineBuilder;
import com.devin.pipeline.core.ExecutionHandle;
import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.execution.ExecutionResult;
import com.devin.pipeline.exporter.JsonTraceExporter;
import com.devin.pipeline.exporter.LogTraceExporter;
import com.devin.pipeline.exporter.MermaidExporter;

import java.nio.file.Path;

public class RecordDemo {

    public static final class RecordSession {
        public boolean cameraOn;
        public int frames;
        public boolean fileClosed;
    }

    public static void main(String[] args) throws Exception {
        Path traces = Path.of("out", "traces");

        Pipeline<RecordSession> pipeline = PipelineBuilder.<RecordSession>of("dashcam-record")
                .addStage(Stage.of("open camera", ctx -> {
                    ctx.payload().cameraOn = true;
                    System.out.println("  camera opened");
                }))
                .loop(loop -> loop
                        .name("record")
                        .stages(
                                Stage.of("capture", ctx -> {
                                    int iter = ctx.currentLoop().iteration;
                                    if (iter == 2) {
                                        throw new RuntimeException("capture timeout (simulated jitter)");
                                    }
                                    ctx.payload().frames++;
                                    System.out.println("  [record#" + iter + "] capture ok, frames="
                                            + ctx.payload().frames);
                                }),
                                Stage.of("encode", ctx -> {
                                    int iter = ctx.currentLoop().iteration;
                                    System.out.println("  [record#" + iter + "] encode ok");
                                    if (iter >= 3) {
                                        ctx.setAttr("stopRequested", true);
                                        System.out.println("  [record#" + iter
                                                + "] stop signal received; exit loop at current boundary");
                                    }
                                }))
                        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
                        .maxIterations(100_000)
                        .onIterationError(ErrorAction.CONTINUE))
                .finallyStage(Stage.of("close camera", ctx -> {
                    ctx.payload().fileClosed = true;
                    System.out.println("  camera closed; total frames=" + ctx.payload().frames);
                }))
                .addExporter(new LogTraceExporter())
                .addExporter(new JsonTraceExporter(traces))
                .addExporter(new MermaidExporter(traces))
                .build();

        ExecutionResult<RecordSession> result = pipeline.execute(new RecordSession());
        System.out.println("result: " + result);

        // 异步执行 + 外部取消
        Pipeline<RecordSession> silent = PipelineBuilder.<RecordSession>of("dashcam-record-async")
                .addStage(Stage.of("open camera", ctx -> ctx.payload().cameraOn = true))
                .loop(loop -> loop
                        .name("record")
                        .stages(
                                Stage.of("capture", ctx -> sleep(10)),
                                Stage.of("encode", ctx -> sleep(10)))
                        .maxIterations(1_000_000))
                .finallyStage(Stage.of("close camera", ctx -> ctx.payload().fileClosed = true))
                .traceDisabled()
                .build();

        ExecutionHandle<RecordSession> handle = silent.executeAsync(new RecordSession());
        Thread.sleep(60);
        handle.cancel("user requested");
        RecordSession s = handle.await().payload();
        System.out.println("after cancel: frames=" + s.frames + ", fileClosed=" + s.fileClosed);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

覆盖场景：每圈每阶段的细粒度轨迹、循环级错误覆盖（抖动帧 `CONTINUE`）、圈边界检查停止信号、
finallyStage 资源清理、异步取消后资源仍然关闭。
