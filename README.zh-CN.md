# Java Pipeline 框架

一个轻量、可嵌入、零依赖的 JVM 工作流流水线框架（JDK 17+）。

它把业务流程（订单处理、ETL、审批流、消息处理）拆成有序、独立、可复用的阶段，由框架负责编排、
数据流转、横切关注点、容错与流程控制。

- [English](README.md)
- 基于 [Apache License 2.0](LICENSE) 发布

## 目录

- [安装与引入](#安装与引入)
- [快速开始](#快速开始)
- [使用说明](#使用说明)
  - [编写阶段](#编写阶段)
  - [阶段间传数据](#阶段间传数据)
  - [编排：顺序 / 循环 / 子流水线 / 并行](#编排顺序--循环--子流水线--并行)
  - [流程控制：中止 / 跳过 / 取消 / 超时](#流程控制中止--跳过--取消--超时)
  - [错误处理](#错误处理)
  - [重试策略](#重试策略)
  - [拦截器](#拦截器)
  - [finallyStage 收尾](#finallystage-收尾)
  - [异步执行与运行时查询](#异步执行与运行时查询)
  - [轨迹与导出器](#轨迹与导出器)
  - [事件](#事件)
  - [YAML DSL](#yaml-dsl)
- [API 速查](#api-速查)
- [示例一：订单流水线](#示例一订单流水线)
- [示例二：录像循环流水线](#示例二录像循环流水线)
- [注意事项与边界](#注意事项与边界)

## 安装与引入

**环境要求**：JDK 17+，框架本身零运行时依赖。

**作为依赖引入（JitPack，仓库打 tag 后可用）：**

```groovy
repositories { maven { url 'https://jitpack.io' } }
dependencies { implementation 'com.github.Devin12355:Pipleline:v1.0.0' }
```

```xml
<dependency>
    <groupId>com.github.Devin12355</groupId>
    <artifactId>Pipleline</artifactId>
    <version>v1.0.0</version>
</dependency>
```

**源码引入（克隆工程，或把 `src/` 拷进你的项目）：**

```bash
mvn -q compile                                                  # Maven 编译
javac -encoding UTF-8 -d out/classes $(find src -name "*.java")  # 纯 javac
mvn -q install        # 装到本地仓库后使用 com.devin:pipeline-core:1.0.0-SNAPSHOT
```

**自己导出依赖包（不需要仓库 tag）：**

```bash
./scripts/export-lib.sh                    # Linux / macOS
powershell -File .\scripts\export-lib.ps1  # Windows
```

产物在 `out/dist/`：

| 文件 | 用途 |
|------|------|
| `pipeline-core-1.0.0.jar` | JVM 项目：`implementation files('libs/pipeline-core-1.0.0.jar')` |
| `pipeline-core-1.0.0-sources.jar` | IDE 里关联源码 |
| `pipeline-core-1.0.0-src.zip` | 直接把 `com/devin/pipeline` 拷进自己的源码目录 |
| `pipeline-core-android/`（`--module` / `-AsModule`） | Android Studio 模块，`include ':pipeline-core'` 即用 |

## 快速开始

```java
Pipeline<Order> pipeline = PipelineBuilder.<Order>of("order-pipeline")
        .addStage(Stage.of("参数校验", ctx -> { /* ... */ }))
        .addStage(Stage.of("风控检查", ctx -> {
            if (riskIsHigh(ctx)) {
                ctx.abort("风控拒绝");
            }
        }))
        .addStage(Stage.of("扣减库存", ctx -> ctx.payload().amount--))
        .addStage(Stage.of("发送通知", ctx -> notify(ctx.payload())))
        .finallyStage(Stage.of("写入审计", ctx -> audit(ctx.payload())))
        .timeout(5_000)
        .errorHandler((stage, ctx, e) -> ErrorAction.ABORT)
        .addExporter(new LogTraceExporter())
        .build();

ExecutionResult<Order> result = pipeline.execute(order);

System.out.println(result.state());     // COMPLETED / ABORTED / FAILED / ...
System.out.println(result.reason());    // 结束原因
System.out.println(result.trace());     // 逐阶段轨迹（关闭采集时为 null）
```

## 使用说明

### 编写阶段

阶段是最小执行单元：**一个阶段做一件事**。`Stage<C>` 是业务代码唯一需要接触的接口，支持 lambda。

```java
// Lambda 形式（总是执行）
Stage<Order> validate = Stage.of("参数校验", ctx -> {
    Order o = ctx.payload();
    if (o.id == null || o.id.isBlank()) {
        throw new IllegalArgumentException("缺少订单号");      // 真正的失败 -> 交给错误策略
    }
    ctx.setAttr("riskScore", score(o));                        // 把数据传给后续阶段
});

// 带条件的 lambda：条件为 false 记 SKIPPED（正常结果，不是错误）
Stage<Order> pay = Stage.of("支付",
        ctx -> "NEW".equals(ctx.payload().status),
        ctx -> ctx.payload().status = "PAID");

// 类形式
public final class PayStage implements Stage<Order> {
    @Override public String name() { return "支付"; }

    @Override public boolean accept(StageContext<Order> ctx) {
        return "NEW".equals(ctx.payload().status);
    }

    @Override public void handle(StageContext<Order> ctx) {
        ctx.payload().status = "PAID";
    }
}

// 异步形式：引擎会 join 该 future 后再继续
public final class UploadStage implements AsyncStage<Order> {
    @Override public String name() { return "上传"; }

    @Override public CompletableFuture<Void> handleAsync(StageContext<Order> ctx) {
        return CompletableFuture.runAsync(() -> upload(ctx.payload()));
    }
}
```

引擎对每个阶段做的事：

```text
1. accept(ctx)?                     为 false -> 记 SKIPPED，继续
2. interceptor.beforeStage
3. stage.handle(ctx)                可调用 abort() / skipRest() / exitLoop()
4. interceptor.afterStage
5. 抛异常时：重试（如配置） -> interceptor.onStageError -> errorHandler.onError
                  CONTINUE -> 继续下一阶段 | ABORT -> 停止
6. 阶段边界检查：abort / skipRest / cancel / interrupt / timeout
```

### 阶段间传数据

`execute(payload)` 会创建**唯一一个** `StageContext<C>`，同一个引用贯穿所有阶段（零拷贝）。两条通道：

| 通道 | 用途 | 示例 |
|------|------|------|
| `payload()` | 正在处理的业务对象 | `Order` 被各阶段原地修改 |
| `attrs` | 辅助 / 派生数据 | `riskScore=87`、`payUrl`、生成的流水号 |

```java
ctx.payload().status = "PAID";          // 主通道
ctx.setAttr("riskScore", 87);           // 辅助通道
int score = ctx.<Integer>getAttr("riskScore");
Map<String, Object> all = ctx.attrs();  // 快照副本
```

建议把属性名（或类型安全 key）集中放到常量类，让阶段之间的隐式数据契约可见：

```java
public final class OrderAttrs {
    private OrderAttrs() {}

    public static final StageContext.TypedKey<Integer> RISK_SCORE =
            StageContext.TypedKey.of("riskScore");
}

ctx.setAttr(OrderAttrs.RISK_SCORE, 87);
int score = ctx.getAttr(OrderAttrs.RISK_SCORE);          // 无需强转
```

框架不校验阶段之间的数据类型 —— 请注明每个阶段需要哪些 attrs、产出哪些 attrs，或让 `accept()`
校验前置条件后跳过 / 快速失败。

### 编排：顺序 / 循环 / 子流水线 / 并行

**顺序** —— 按注册顺序执行：

```java
.addStage(a).addStage(b).addStages(c, d)
```

**循环** —— 循环体重复执行，每圈每阶段都有独立轨迹记录：

```java
.loop(loop -> loop
        .name("record")
        .stages(capture, encode)                                  // 循环体，按序执行
        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
        .maxIterations(100_000)                                   // 安全上限
        .onIterationError(ErrorAction.CONTINUE))                  // 循环级错误策略
```

- 条件在**每圈开始前**检查，当前圈一定会跑完
- `ctx.exitLoop()` 在当前圈结束后离开循环，流水线继续
- `ctx.currentLoop().iteration` 是从 1 开始的计数；每圈每个阶段都有自己的 `StageRecord`
  （`loop=record, iteration=N`）
- 触达 `maxIterations` 时以 `COMPLETED` 结束，轨迹标记 `truncated`

**子流水线** —— 一条流水线作为另一条的阶段：

```java
Pipeline<Order> settle = PipelineBuilder.<Order>of("settle")...build();

PipelineBuilder.<Order>of("order-pipeline")
        .subPipeline("settle", settle)
```

子流水线与父级**共享同一个** `StageContext`、轨迹、取消令牌和 deadline：attrs 互通，子阶段记录
落在父级轨迹里，`abort()` / `skipRest()` / 取消都会向父级传播；子流水线自己的 timeout、
ErrorHandler、拦截器作用于它的阶段。

**并行组** —— 分支并发执行，全部 join 后流水线才继续：

```java
.parallel("enrich", p -> p
        .stages(callA, callB, callC)
        .executor(myPool))                 // 可选，默认 ForkJoinPool.commonPool()
```

分支共享同一个 `StageContext`，因此**不要在不同分支里写同一个 attr / payload 字段**；失败分支
按错误策略处理。

### 流程控制：中止 / 跳过 / 取消 / 超时

停止是**协作式**的：引擎只在阶段边界检查停止条件，正在执行的 `handle()` 一定会跑完，框架不会强杀。

| 需求 | API | 终态 |
|------|-----|------|
| 业务否决 | `ctx.abort("风控拒绝")` | `ABORTED` |
| 跳过剩余阶段 | `ctx.skipRest()` | `COMPLETED` |
| 只跳出当前循环 | `ctx.exitLoop()` | 流水线继续 |
| 外部停止 | `handle.cancel("用户取消")` 或 `token.cancel("远程停止")` | `CANCELLED` |
| 限制执行时长 | `.timeout(5_000)` | `TIMEOUT` |

```java
CancelToken token = new CancelToken();                 // 交给消息监听器持有
messageListener.onStop(msg -> token.cancel("远程停止"));

ExecutionHandle<Order> handle = pipeline.executeAsync(order);
handle.cancel("用户取消");                              // 在下一个阶段边界生效
ExecutionResult<Order> result = handle.await();        // 取消不会抛异常
```

无论哪种触发，走的是同一条收尾路径：轨迹定稿、停止点之后的阶段记为 `SKIPPED` / `CANCELLED`、
**导出器照常执行**、future 正常完成并返回终态结果。

终态一览：

| 触发 | 终态 |
|------|------|
| 全部阶段执行完，或 `skipRest()` | `COMPLETED` |
| `ctx.abort()` | `ABORTED` |
| 异常 + `ABORT` 策略 | `FAILED` |
| `cancel()` | `CANCELLED` |
| 超过 `.timeout(ms)` | `TIMEOUT` |
| 触达 `maxIterations` | `COMPLETED`（轨迹 `truncated`） |

`FlowReason`（`type` + `code` + `message`）独立于状态，说明“为什么结束”。

### 错误处理

```java
.errorHandler((stage, ctx, e) -> {
    log.warn("阶段 {} 失败", stage.name(), e);
    return e instanceof TimeoutException ? ErrorAction.CONTINUE : ErrorAction.ABORT;
})
```

- `CONTINUE` —— 该阶段记 `FAILED` 并继续；`ABORT` —— 停止流水线
- 默认是 `ABORT`（`ErrorHandler.abortAll()`；`ErrorHandler.continueAll()` 则相反）
- 循环内以 `.onIterationError(ErrorAction.CONTINUE)` 覆盖
- `Interceptor.onStageError` 先于 `ErrorHandler` 执行，拦截器能看到每一次失败

### 重试策略

```java
.addStage(Stage.withRetry(Stage.of("调用接口", ctx -> callRemote()),
        RetryPolicy.of(3).initialDelayMs(100).multiplier(2.0).maxDelayMs(2_000)))

.retryPolicy(RetryPolicy.of(3))           // 流水线级默认
```

- 重试发生在阶段内部；每次失败的尝试仍会触发 `onStageError`
- 只有最后一次尝试仍失败时才进入 `ErrorHandler`
- `StageRecord.attempts()` 记录实际执行次数；退避按小片睡眠，随时可被取消 / 中断
- 阶段级策略优先于流水线默认；`retryOn(...)` 可过滤可重试的异常

```java
RetryPolicy.of(4)
        .initialDelayMs(200)
        .multiplier(2.0)                                  // 指数退避：200、400、800...
        .maxDelayMs(5_000)
        .retryOn(e -> e instanceof java.net.SocketTimeoutException);
```

### 拦截器

```java
Interceptor<Order> timing = new Interceptor<>() {
    @Override public void beforeStage(Stage<Order> stage, StageContext<Order> ctx) {
        ctx.setAttr("t0", System.nanoTime());
    }

    @Override public void afterStage(Stage<Order> stage, StageContext<Order> ctx) {
        long ms = (System.nanoTime() - ctx.<Long>getAttr("t0")) / 1_000_000;
        log.info("{} 耗时 {}ms", stage.name(), ms);
    }
};
```

典型用途：日志、耗时统计、审计、事务控制、灰度校验。注册顺序即执行顺序 —— `beforeStage` 正序、
`afterStage` 逆序（嵌套语义）。拦截器抛出的异常会被吞掉：观测不会改变执行结果。

### finallyStage 收尾

```java
.finallyStage(Stage.of("关闭摄像头", ctx -> close(ctx.payload())))
```

**所有**退出路径都会执行 —— 正常完成、`abort`、`skipRest`、取消、超时、异常（与 `try-finally`
语义一致）。多个 finally 阶段按注册顺序执行；它们的异常记入轨迹，但不会覆盖原始失败原因。

### 异步执行与运行时查询

```java
ExecutionHandle<RecordSession> handle = pipeline.executeAsync(session);           // commonPool
ExecutionHandle<RecordSession> h2 = pipeline.executeAsync(session, myExecutor);    // 自定义线程池

handle.state();          // PENDING / RUNNING / COMPLETED / ...（volatile 快照）
handle.currentStage();   // 当前正在执行的阶段
handle.iteration();      // 当前循环圈数，不在循环中为 0
handle.cancel("用户取消");
ExecutionResult<RecordSession> result = handle.await();     // 或 handle.future()
```

单个工作线程串行执行所有阶段，语义与 `execute(payload)` 完全一致；`Pipeline` 是不可变对象，
可被多线程并发执行。

| 诉求 | 用法 |
|------|------|
| 阶段内自查 | `ctx.stopRequested()`、`ctx.currentLoop()` |
| 看板 / 轮询 | `handle.state()` / `currentStage()` / `iteration()` |
| 等待结果 | `handle.await()` / `handle.future()` |
| 事件推送 | `PipelineListener.onEvent`（无需轮询） |
| 事后审计 | 导出的 JSON / Mermaid 轨迹 |

### 轨迹与导出器

每次执行产出一份 `ExecutionTrace`（traceId、起止时间、耗时、`truncated`、结束原因），其中每个
阶段执行对应一条 `StageRecord`：序号、阶段名、循环归属、圈数、状态、耗时、错误、尝试次数。

| 导出器 | 产物 |
|--------|------|
| `LogTraceExporter` | 结构化日志：一行汇总 + 每阶段一行 |
| `JsonTraceExporter(Path)` | `trace-<id>.json`，供机器 / 日志平台消费 |
| `MermaidExporter(Path)` | `trace-<id>.md` 流程图，按状态着色 |

```java
.addExporter(new LogTraceExporter())
.addExporter(new JsonTraceExporter(Path.of("out", "traces")))
.addExporter(new MermaidExporter(Path.of("out", "traces")))
.traceDisabled()                          // 完全关闭采集
```

读取轨迹：

```java
ExecutionTrace trace = result.trace();
for (StageRecord r : trace.records()) {
    System.out.println(r);                // #3 风控检查 (settle#2) SUCCESS 12ms x3
}
trace.hasFailure();
trace.truncated();
```

自定义导出器（SPI 只有一个方法）：

```java
public final class MetricsExporter implements TraceExporter {
    @Override public void export(ExecutionTrace t) {
        for (StageRecord r : t.records()) {
            registry.timer("pipeline.stage", "name", r.stageName()).record(r.durationMs());
        }
    }
}
```

### 事件

```java
.addListener(new PipelineListener<>() {
    @Override public void onEvent(PipelineEvent e) {
        auditSink.write(e);   // PIPELINE_START / STAGE_START / STAGE_FINISH / STATE_CHANGED / PIPELINE_FINISH
    }
})
```

事件携带 traceId、流水线名、阶段名、循环归属、圈数、状态、耗时和终态原因 —— 用于监控告警时
无需轮询。

### YAML DSL

YAML 只声明结构与数值，行为留在 Java 里按 id 绑定。

```java
StageRegistry<Order> registry = new StageRegistry<Order>()
        .handler("validate", ctx -> { /* ... */ })
        .condition("is-new", ctx -> "NEW".equals(ctx.payload().status))
        .pipeline("settle", settle)
        .executor("io-pool", myPool);

Pipeline<Order> fromFile = YamlPipelineLoader.load(Path.of("pipeline.yaml"), registry);
Pipeline<Order> fromText = YamlPipelineLoader.load(yamlString, registry);
```

```yaml
name: order-pipeline
timeoutMs: 5000
trace: true
errorAction: ABORT            # ABORT / CONTINUE / 已注册的 errorHandler id
retry:                        # 流水线级默认
  maxAttempts: 3
  initialDelayMs: 100
  multiplier: 2.0
stages:
  - name: validate
    handler: validate
    when: is-new               # 已注册的条件 id
    retry:
      maxAttempts: 5
  - name: settle
    subPipeline: settle
  - name: fanout
    parallel:
      executor: io-pool
      stages:
        - name: enrich-a
          handler: enrich-a
        - name: enrich-b
          handler: enrich-b
  - name: record
    loop:
      while: should-record
      maxIterations: 100
      onIterationError: CONTINUE
      stages:
        - name: capture
          handler: capture
  - name: audit
    handler: audit
    finally: true
```

阶段支持的键：`name`、`handler` + `when` + `retry`、`loop`、`subPipeline`、`parallel`、
`finally: true`。加载器使用内置的块级 YAML 子集解析器，因此框架保持零依赖 —— 不支持锚点/引用、
多行标量、流式集合（`{a: 1}`）与标签。

## API 速查

| 类型 | 常用方法 |
|------|----------|
| `Stage<C>` | `name()`、`accept(ctx)`、`handle(ctx)`、`retry()`；工厂 `Stage.of(name, handler)`、`Stage.of(name, condition, handler)`、`Stage.withRetry(stage, policy)` |
| `AsyncStage<C>` | `handleAsync(ctx)` 返回 `CompletableFuture<Void>`（引擎负责 join） |
| `StageContext<C>` | `payload()` / `setPayload()`、`setAttr` / `getAttr` / `hasAttr` / `removeAttr` / `attrs()`、`abort(reason)`、`skipRest()`、`exitLoop()`、`stopRequested()`、`currentLoop()`、`currentStage()`、`cancelToken()`、`traceId()` |
| `Pipeline<C>` | `execute(payload)`、`executeAsync(payload)`、`executeAsync(payload, executor)`、`name()`、`stages()` |
| `ExecutionHandle<C>` | `state()`、`currentStage()`、`iteration()`、`cancel(reason)`、`await()`、`await(timeout, unit)`、`future()`、`payload()`、`cancelToken()` |
| `ExecutionResult<C>` | `state()`、`reason()`、`payload()`、`trace()`、`durationMs()`、`isSuccess()` |
| `ExecutionTrace` | `records()`、`durationMs()`、`truncated()`、`reason()`、`hasFailure()` |
| `StageRecord` | `index()`、`stageName()`、`loopName()`、`iteration()`、`status()`、`durationMs()`、`error()`、`attempts()` |
| `PipelineBuilder<C>` | `addStage(s)` / `addStages(...)`、`loop(cfg)`、`parallel(name, cfg)`、`subPipeline(name, p)`、`finallyStage(s)`、`errorHandler(h)`、`retryPolicy(p)`、`timeout(ms)`、`traceEnabled(bool)` / `traceDisabled()`、`addInterceptor`、`addListener`、`addExporter`、`build()` |
| 循环配置 | `name`、`stages(...)`、`addStage`、`while_(cond)`、`maxIterations(n)`、`onIterationError(action)` |
| 并行配置 | `stages(...)`、`addStage`、`executor(e)` |

## 示例一：订单流水线

参数校验 → 风控检查 → 扣减库存 → 支付 → 发送通知，最后有审计收尾阶段。

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
                .addStage(Stage.of("参数校验", ctx -> {
                    Order o = ctx.payload();
                    if (o.id == null || o.id.isBlank()) {
                        throw new IllegalArgumentException("缺少订单号");
                    }
                    if (o.amount <= 0) {
                        throw new IllegalArgumentException("金额非法: " + o.amount);
                    }
                }))
                .addStage(Stage.of("风控检查", ctx -> {
                    Order o = ctx.payload();
                    o.riskScore = o.amount > 10_000 ? 40 : 85;
                    ctx.setAttr("riskScore", o.riskScore);
                    if (o.riskScore < 60) {
                        ctx.abort("风控拒绝");
                    }
                }))
                .addStage(Stage.of("扣减库存", ctx -> ctx.payload().amount--))
                .addStage(Stage.of("支付", ctx -> ctx.payload().status = "PAID"))
                .addStage(Stage.of("发送通知", ctx ->
                        System.out.println("  [通知] 订单 " + ctx.payload().id + " 已支付")))
                .finallyStage(Stage.of("写入审计", ctx ->
                        System.out.println("  [审计] 订单终态: " + ctx.payload())))
                .errorHandler((stage, ctx, e) -> {
                    System.out.println("  [ErrorHandler] " + stage.name() + " 失败: "
                            + e.getMessage() + " -> ABORT");
                    return ErrorAction.ABORT;
                })
                .addExporter(new LogTraceExporter())
                .addExporter(new JsonTraceExporter(traces))
                .addExporter(new MermaidExporter(traces))
                .build();

        ExecutionResult<Order> ok = pipeline.execute(new Order("A001", 3));
        System.out.println("正常: " + ok);

        ExecutionResult<Order> bad = pipeline.execute(new Order("B002", -1));
        System.out.println("金额非法: " + bad);

        ExecutionResult<Order> risky = pipeline.execute(new Order("C003", 99_999));
        System.out.println("高风险: " + risky);
    }
}
```

覆盖场景：正常完成、校验失败（`FAILED`）、业务中止（`ABORTED`）、finally 阶段始终执行。

## 示例二：录像循环流水线

开摄像头 → [采集 → 编码] × N → 关摄像头，含停止信号与异步取消。

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
                .addStage(Stage.of("打开摄像头", ctx -> {
                    ctx.payload().cameraOn = true;
                    System.out.println("  摄像头已打开");
                }))
                .loop(loop -> loop
                        .name("record")
                        .stages(
                                Stage.of("采集", ctx -> {
                                    int iter = ctx.currentLoop().iteration;
                                    if (iter == 2) {
                                        throw new RuntimeException("采集超时（模拟抖动）");
                                    }
                                    ctx.payload().frames++;
                                    System.out.println("  [record#" + iter + "] 采集成功, frames="
                                            + ctx.payload().frames);
                                }),
                                Stage.of("编码", ctx -> {
                                    int iter = ctx.currentLoop().iteration;
                                    System.out.println("  [record#" + iter + "] 编码成功");
                                    if (iter >= 3) {
                                        ctx.setAttr("stopRequested", true);
                                        System.out.println("  [record#" + iter
                                                + "] 收到停止信号，在当前边界退出循环");
                                    }
                                }))
                        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
                        .maxIterations(100_000)
                        .onIterationError(ErrorAction.CONTINUE))
                .finallyStage(Stage.of("关闭摄像头", ctx -> {
                    ctx.payload().fileClosed = true;
                    System.out.println("  摄像头已关闭; 总帧数=" + ctx.payload().frames);
                }))
                .addExporter(new LogTraceExporter())
                .addExporter(new JsonTraceExporter(traces))
                .addExporter(new MermaidExporter(traces))
                .build();

        ExecutionResult<RecordSession> result = pipeline.execute(new RecordSession());
        System.out.println("结果: " + result);

        // 异步执行 + 外部取消
        Pipeline<RecordSession> silent = PipelineBuilder.<RecordSession>of("dashcam-record-async")
                .addStage(Stage.of("打开摄像头", ctx -> ctx.payload().cameraOn = true))
                .loop(loop -> loop
                        .name("record")
                        .stages(
                                Stage.of("采集", ctx -> sleep(10)),
                                Stage.of("编码", ctx -> sleep(10)))
                        .maxIterations(1_000_000))
                .finallyStage(Stage.of("关闭摄像头", ctx -> ctx.payload().fileClosed = true))
                .traceDisabled()
                .build();

        ExecutionHandle<RecordSession> handle = silent.executeAsync(new RecordSession());
        Thread.sleep(60);
        handle.cancel("用户取消");
        RecordSession s = handle.await().payload();
        System.out.println("取消后: frames=" + s.frames + ", fileClosed=" + s.fileClosed);
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

覆盖场景：循环体每圈独立轨迹、循环级错误覆盖（抖动帧 `CONTINUE`）、圈边界检查停止信号、
finally 资源清理、异步取消后资源仍然关闭。

## 注意事项与边界

- **一次执行 = 一个线程、串行**（按需开启的 `parallel(...)` 并行组是文档化的例外）。`Pipeline`
  不可变、可跨线程复用；每次执行拥有独立的 `StageContext`。
- **阶段内部起的线程必须 join** 后再从 `handle()` 返回（或实现 `AsyncStage`），否则 `abort()`
  会漏掉在途工作、轨迹耗时也不准。
- **取消是协作式的**：`future.cancel(true)` 只能中断阻塞操作；CPU 密集型阶段应自行轮询
  `ctx.cancelToken().isCancelled()`。框架无法杀死线程。
- **共享的外部资源**（连接池、有状态服务）线程安全性由使用方保证，框架只保证自身不共享状态。
- **拦截器、监听器、导出器不会改变执行结果** —— 它们抛出的异常一律被吞掉。
- **Android 场景**：用 `scripts/export-lib.ps1 -AsModule` 导出并按模块引入；需要
  `coreLibraryDesugaring 'com.android.tools:desugar_jdk_libs:2.1.4'` 才能使用 `java.time` 与
  `List.of` / `Map.of`，`minSdk` 建议 24（`java.util.concurrent.CompletableFuture` 的限制）。

基于 [Apache License 2.0](LICENSE) 发布。
