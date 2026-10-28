# Java Pipeline Framework

A lightweight, embeddable, zero-dependency workflow pipeline framework for the JVM (JDK 17+).

It splits a business process (order processing, ETL, approval flow, message handling) into ordered,
independent, reusable stages and takes care of orchestration, data flow, cross-cutting concerns,
fault tolerance and flow control for you.

- [中文文档](README.zh-CN.md)
- Licensed under the [Apache License 2.0](LICENSE)

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Usage guide](#usage-guide)
  - [Stages](#stages)
  - [Passing data between stages](#passing-data-between-stages)
  - [Orchestration: sequence, loop, sub-pipeline, parallel](#orchestration-sequence-loop-sub-pipeline-parallel)
  - [Flow control: abort, skip, cancel, timeout](#flow-control-abort-skip-cancel-timeout)
  - [Error handling](#error-handling)
  - [Retry policy](#retry-policy)
  - [Interceptors](#interceptors)
  - [finallyStage cleanup](#finallystage-cleanup)
  - [Async execution and runtime query](#async-execution-and-runtime-query)
  - [Trace and exporters](#trace-and-exporters)
  - [Events](#events)
  - [YAML DSL](#yaml-dsl)
- [API cheat sheet](#api-cheat-sheet)
- [Example 1: order pipeline](#example-1-order-pipeline)
- [Example 2: recording pipeline with loop](#example-2-recording-pipeline-with-loop)
- [Notes and limits](#notes-and-limits)

## Install

**Requirements**: JDK 17+, no runtime dependencies (the framework itself is dependency-free).

**As a dependency (JitPack, once the repository has a tag):**

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

**From source (clone or copy `src/` into your project):**

```bash
mvn -q compile                                            # Maven
javac -encoding UTF-8 -d out/classes $(find src -name "*.java")   # plain javac
mvn -q install       # then: com.devin:pipeline-core:1.0.0-SNAPSHOT
```

**Export a library yourself (no repository tag needed):**

```bash
./scripts/export-lib.sh                    # Linux / macOS
powershell -File .\scripts\export-lib.ps1  # Windows
```

Produces in `out/dist/`:

| File | Use it for |
|------|-----------|
| `pipeline-core-1.0.0.jar` | JVM projects: `implementation files('libs/pipeline-core-1.0.0.jar')` |
| `pipeline-core-1.0.0-sources.jar` | attach sources in the IDE |
| `pipeline-core-1.0.0-src.zip` | copy `com/devin/pipeline` straight into your sources |
| `pipeline-core-android/` (`--module` / `-AsModule`) | Android Studio module, ready to `include ':pipeline-core'` |

## Quick start

```java
Pipeline<Order> pipeline = PipelineBuilder.<Order>of("order-pipeline")
        .addStage(Stage.of("validate input", ctx -> { /* ... */ }))
        .addStage(Stage.of("risk check", ctx -> {
            if (riskIsHigh(ctx)) {
                ctx.abort("risk rejected");
            }
        }))
        .addStage(Stage.of("deduct inventory", ctx -> ctx.payload().amount--))
        .addStage(Stage.of("send notification", ctx -> notify(ctx.payload())))
        .finallyStage(Stage.of("audit ledger", ctx -> audit(ctx.payload())))
        .timeout(5_000)
        .errorHandler((stage, ctx, e) -> ErrorAction.ABORT)
        .addExporter(new LogTraceExporter())
        .build();

ExecutionResult<Order> result = pipeline.execute(order);

System.out.println(result.state());     // COMPLETED / ABORTED / FAILED / ...
System.out.println(result.reason());    // why it ended
System.out.println(result.trace());     // per-stage trace (null when tracing disabled)
```

## Usage guide

### Stages

A stage is the smallest unit: **one stage does one thing**. `Stage<C>` is the only interface your
business code needs — lambda friendly.

```java
// Lambda form (always runs)
Stage<Order> validate = Stage.of("validate input", ctx -> {
    Order o = ctx.payload();
    if (o.id == null || o.id.isBlank()) {
        throw new IllegalArgumentException("missing order id");   // real failure -> error policy
    }
    ctx.setAttr("riskScore", score(o));                            // pass data downstream
});

// Lambda with a condition: false -> SKIPPED (a normal outcome, never an error)
Stage<Order> pay = Stage.of("payment",
        ctx -> "NEW".equals(ctx.payload().status),
        ctx -> ctx.payload().status = "PAID");

// Class form
public final class PayStage implements Stage<Order> {
    @Override public String name() { return "payment"; }

    @Override public boolean accept(StageContext<Order> ctx) {
        return "NEW".equals(ctx.payload().status);
    }

    @Override public void handle(StageContext<Order> ctx) {
        ctx.payload().status = "PAID";
    }
}

// Async form: the engine joins the future before moving on
public final class UploadStage implements AsyncStage<Order> {
    @Override public String name() { return "upload"; }

    @Override public CompletableFuture<Void> handleAsync(StageContext<Order> ctx) {
        return CompletableFuture.runAsync(() -> upload(ctx.payload()));
    }
}
```

What the engine does per stage:

```text
1. accept(ctx)?                     false -> record SKIPPED, continue
2. interceptor.beforeStage
3. stage.handle(ctx)                may call abort() / skipRest() / exitLoop()
4. interceptor.afterStage
5. on exception: retry (if configured) -> interceptor.onStageError -> errorHandler.onError
                 CONTINUE -> next stage | ABORT -> stop
6. at the stage boundary: check abort / skipRest / cancel / interrupt / timeout
```

### Passing data between stages

`execute(payload)` creates **one** `StageContext<C>`; the same reference travels through all stages
(zero copy). Two channels:

| Channel | Purpose | Example |
|---------|---------|---------|
| `payload()` | the business object being processed | `Order` mutated in place by each stage |
| `attrs` | auxiliary / derived data | `riskScore=87`, `payUrl`, generated serial number |

```java
ctx.payload().status = "PAID";          // main channel
ctx.setAttr("riskScore", 87);           // auxiliary channel
int score = ctx.<Integer>getAttr("riskScore");
Map<String, Object> all = ctx.attrs();  // snapshot copy
```

Recommended practice: collect attribute names (or type-safe keys) in one constants class so the
implicit data contract between stages stays visible.

```java
public final class OrderAttrs {
    private OrderAttrs() {}

    public static final StageContext.TypedKey<Integer> RISK_SCORE =
            StageContext.TypedKey.of("riskScore");
}

ctx.setAttr(OrderAttrs.RISK_SCORE, 87);
int score = ctx.getAttr(OrderAttrs.RISK_SCORE);          // no cast
```

The framework does not validate data types between stages — document which attrs each stage
requires and produces, or let `accept()` verify prerequisites and skip (or fail fast).

### Orchestration: sequence, loop, sub-pipeline, parallel

**Sequence** — stages run in registration order:

```java
.addStage(a).addStage(b).addStages(c, d)
```

**Loop** — a body executed repeatedly with per-iteration trace records:

```java
.loop(loop -> loop
        .name("record")
        .stages(capture, encode)                                  // body, in order per iteration
        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
        .maxIterations(100_000)                                   // safety bound
        .onIterationError(ErrorAction.CONTINUE))                  // per-iteration error policy
```

- the condition is checked **before each iteration**; the current iteration always finishes
- `ctx.exitLoop()` leaves the loop after the current iteration, the pipeline continues
- `ctx.currentLoop().iteration` is the 1-based counter; each stage of each iteration gets its own
  `StageRecord` (`loop=record, iteration=N`)
- hitting `maxIterations` ends as `COMPLETED` with the trace marked `truncated`

**Sub-pipeline** — one pipeline used as a stage of another:

```java
Pipeline<Order> settle = PipelineBuilder.<Order>of("settle")...build();

PipelineBuilder.<Order>of("order-pipeline")
        .subPipeline("settle", settle)
```

The child runs with the **same** `StageContext`, trace, cancel token and deadline: attrs are shared,
child stage records land in the parent trace, and `abort()` / `skipRest()` / cancel propagate.
The child's own timeout, error handler and interceptors apply to its stages.

**Parallel group** — branches run concurrently, joined before the pipeline continues:

```java
.parallel("enrich", p -> p
        .stages(callA, callB, callC)
        .executor(myPool)                  // optional; default ForkJoinPool.commonPool()
        .failFast())                       // optional; cancel siblings once one branch stops
```

Branches share the same `StageContext`, so **do not write the same attrs / payload fields from
different branches**. A failing branch follows the error policy.

The join is **deadline aware**: with `.timeout(ms)`, branches that miss the deadline are cancelled
and the execution ends as `TIMEOUT` instead of hanging. If several branches fail, the first reason
wins and the rest are appended to its message rather than being dropped.

### Flow control: abort, skip, cancel, timeout

Stopping is **cooperative**: the engine only checks stop conditions at stage boundaries, so a
running `handle()` always completes — the framework never kills a stage.

| You want | API | Terminal state |
|----------|-----|----------------|
| Reject the business case | `ctx.abort("risk rejected")` | `ABORTED` |
| Skip the remaining stages | `ctx.skipRest()` | `COMPLETED` |
| Leave the enclosing loop only | `ctx.exitLoop()` | pipeline continues |
| Stop from outside | `handle.cancel("user requested")` or `token.cancel("remote stop")` | `CANCELLED` |
| Bound the execution | `.timeout(5_000)` | `TIMEOUT` |

The deadline is checked at stage boundaries, before every loop iteration, while a `ParallelStage`
group is joined, and while an `AsyncStage` future is awaited — the two join points cancel the
underlying futures instead of blocking past the deadline.

```java
CancelToken token = new CancelToken();                 // hand it to a message listener
messageListener.onStop(msg -> token.cancel("remote stop"));

ExecutionHandle<Order> handle = pipeline.executeAsync(order);
handle.cancel("user requested");                       // effective at the next stage boundary
ExecutionResult<Order> result = handle.await();        // cancellation is not an exception
```

Whatever the trigger, the same exit path runs: the trace is finalized, stages after the stop point
are recorded as `SKIPPED` / `CANCELLED`, **exporters still run**, and the future completes normally
with the terminal result.

Terminal states:

| Trigger | State |
|---------|-------|
| all stages finished, or `skipRest()` | `COMPLETED` |
| `ctx.abort()` | `ABORTED` |
| exception + `ABORT` policy | `FAILED` |
| `cancel()` | `CANCELLED` |
| `.timeout(ms)` exceeded | `TIMEOUT` |
| `maxIterations` reached | `COMPLETED` (`reason.type() == LOOP_TRUNCATED`, trace `truncated`) |

`FlowReason` (`type` + `code` + `message`) carries *why* it ended, separately from the state.

### Error handling

```java
.errorHandler((stage, ctx, e) -> {
    log.warn("stage {} failed", stage.name(), e);
    return e instanceof TimeoutException ? ErrorAction.CONTINUE : ErrorAction.ABORT;
})
```

- `CONTINUE` — record `FAILED` for that stage and keep going; `ABORT` — stop the pipeline
- default is `ABORT` (`ErrorHandler.abortAll()`; `ErrorHandler.continueAll()` for the opposite)
- loop-level override wins inside a loop: `.onIterationError(ErrorAction.CONTINUE)`
- `Interceptor.onStageError` runs before the `ErrorHandler`, so interceptors see every failure

### Retry policy

```java
.addStage(Stage.withRetry(Stage.of("call api", ctx -> callRemote()),
        RetryPolicy.of(3).initialDelayMs(100).multiplier(2.0).maxDelayMs(2_000)))

.retryPolicy(RetryPolicy.of(3))           // pipeline-wide default
```

- retries happen inside the stage; every attempt that fails still runs `onStageError`
- the `ErrorHandler` is consulted only after the last attempt failed
- `StageRecord.attempts()` reports how many times the body ran; backoff sleeps in small slices and
  stays cancel-aware
- per-stage policy wins over the pipeline default; `retryOn(...)` filters retriable failures

```java
RetryPolicy.of(4)
        .initialDelayMs(200)
        .multiplier(2.0)                                  // exponential: 200, 400, 800...
        .maxDelayMs(5_000)
        .retryOn(e -> e instanceof java.net.SocketTimeoutException);
```

### Interceptors

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

Typical uses: logging, timing, auditing, transaction control, gray-release checks. Registration
order is execution order — `beforeStage` in order, `afterStage` in reverse (nesting semantics).
Interceptor exceptions are swallowed: observation never changes an outcome.

### finallyStage cleanup

```java
.finallyStage(Stage.of("close camera", ctx -> close(ctx.payload())))
```

Runs on **every** exit path — normal completion, `abort`, `skipRest`, cancel, timeout and
exceptions (same semantics as `try-finally`). Multiple finally stages run in registration order;
their exceptions are recorded in the trace but do not replace the original failure reason.

### Async execution and runtime query

```java
ExecutionHandle<RecordSession> handle = pipeline.executeAsync(session);          // common pool
ExecutionHandle<RecordSession> h2 = pipeline.executeAsync(session, myExecutor);   // your executor

handle.state();          // PENDING / RUNNING / COMPLETED / ... (volatile snapshot)
handle.currentStage();   // stage currently executing
handle.iteration();      // current loop iteration, 0 when not in a loop
handle.cancel("user requested");
ExecutionResult<RecordSession> result = handle.await();     // or handle.future()
```

One worker thread runs all stages serially, so semantics are identical to `execute(payload)`;
the same `Pipeline` instance can be executed concurrently by many threads (it is immutable).

| Need | Use |
|------|-----|
| self-check inside a stage | flags on `ctx` (`stopRequested()`, `currentLoop()`) |
| dashboard / polling | `handle.state()` / `currentStage()` / `iteration()` |
| wait for the result | `handle.await()` / `handle.future()` |
| push notifications | `PipelineListener.onEvent` (no polling) |
| post-hoc audit | exported JSON / Mermaid trace |

### Trace and exporters

Every execution produces an `ExecutionTrace` (traceId, timing, duration, `truncated`, `reason`)
holding one `StageRecord` per stage execution — index, stage name, loop scope, iteration, status,
duration, error, attempts.

| Exporter | Output |
|----------|--------|
| `LogTraceExporter` | structured log: one summary line + one line per stage |
| `JsonTraceExporter(Path)` | `trace-<id>.json` for machines / log platforms (directory is mandatory) |
| `MermaidExporter(Path)` | `trace-<id>.md` flowchart colored by status |

```java
.addExporter(new LogTraceExporter())
.addExporter(new JsonTraceExporter(Path.of("out", "traces")))
.addExporter(new MermaidExporter(Path.of("out", "traces")))
.addExporter(new JsonTraceExporter(traces), ioPool)   // export off the execution thread
.traceDisabled()                          // turn capture off entirely
```

Exporters normally run inline on the thread that finished the execution; hand over an executor
(`addExporter(exporter, executor)`) when the export does I/O on a latency sensitive thread such as
an Android main thread.

Reading a trace:

```java
ExecutionTrace trace = result.trace();
for (StageRecord r : trace.records()) {
    System.out.println(r);                // #3 risk check (settle#2) SUCCESS 12ms x3
}
trace.hasFailure();
trace.truncated();
```

Custom exporter (SPI is one method):

```java
public final class MetricsExporter implements TraceExporter {
    @Override public void export(ExecutionTrace t) {
        for (StageRecord r : t.records()) {
            registry.timer("pipeline.stage", "name", r.stageName()).record(r.durationMs());
        }
    }
}
```

### Events

```java
.addListener(new PipelineListener<>() {
    @Override public void onEvent(PipelineEvent e) {
        auditSink.write(e);   // PIPELINE_START / STAGE_START / STAGE_FINISH / STATE_CHANGED / PIPELINE_FINISH
    }
})
```

Events carry traceId, pipeline name, stage name, loop scope, iteration, status, duration and the
terminal reason — everything needed for monitoring without polling.

### YAML DSL

YAML declares structure and numbers; behaviour stays in Java and is bound by id.

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
errorAction: ABORT            # ABORT / CONTINUE / registered error-handler id
retry:                        # pipeline-wide default
  maxAttempts: 3
  initialDelayMs: 100
  multiplier: 2.0
stages:
  - name: validate
    handler: validate
    when: is-new               # registered condition id
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

Supported stage keys: `name`, `handler` + `when` + `retry`, `loop`, `subPipeline`, `parallel`,
`finally: true`. The loader uses a built-in block-style YAML subset parser, so the framework keeps
its zero-dependency promise — anchors/aliases, multi-line scalars, flow collections (`{a: 1}`) and
tags are not supported.

## API cheat sheet

| Type | What you call |
|------|---------------|
| `Stage<C>` | `name()`, `accept(ctx)`, `handle(ctx)`, `retry()`; factories `Stage.of(name, handler)`, `Stage.of(name, condition, handler)`, `Stage.withRetry(stage, policy)` |
| `AsyncStage<C>` | `handleAsync(ctx)` returning `CompletableFuture<Void>` (joined by the engine) |
| `StageContext<C>` | `payload()` / `setPayload()`, `setAttr` / `getAttr` / `hasAttr` / `removeAttr` / `attrs()`, `abort(reason)`, `skipRest()`, `exitLoop()`, `stopRequested()`, `currentLoop()`, `currentStage()`, `cancelToken()`, `traceId()` |
| `Pipeline<C>` | `execute(payload)`, `executeAsync(payload)`, `executeAsync(payload, executor)`, `name()`, `stages()` |
| `ExecutionHandle<C>` | `state()`, `currentStage()`, `iteration()`, `cancel(reason)`, `await()`, `await(timeout, unit)`, `future()`, `payload()`, `cancelToken()` |
| `ExecutionResult<C>` | `state()`, `reason()`, `payload()`, `trace()`, `durationMs()`, `isSuccess()` |
| `ExecutionTrace` | `records()`, `durationMs()`, `truncated()`, `reason()`, `hasFailure()` |
| `StageRecord` | `index()`, `stageName()`, `loopName()`, `iteration()`, `status()`, `durationMs()`, `error()`, `attempts()` |
| `PipelineBuilder<C>` | `addStage(s)` / `addStages(...)`, `loop(cfg)`, `parallel(name, cfg)`, `subPipeline(name, p)`, `finallyStage(s)`, `errorHandler(h)`, `retryPolicy(p)`, `timeout(ms)`, `traceEnabled(bool)` / `traceDisabled()`, `addInterceptor`, `addListener`, `addExporter`, `build()` |
| Loop config | `name`, `stages(...)`, `addStage`, `while_(cond)`, `maxIterations(n)`, `onIterationError(action)` |
| Parallel config | `stages(...)`, `addStage`, `executor(e)`, `failFast()` / `failFast(bool)` |

## Example 1: order pipeline

Validate → risk check → inventory → payment → notification, with an audit finally stage.

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

Scenarios covered: normal completion, validation failure (`FAILED`), business abort (`ABORTED`),
and a finally stage that always runs.

## Example 2: recording pipeline with loop

Open camera → [capture → encode] × N → close camera, with a stop signal and async cancellation.

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

        // Async execution + external cancellation
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

Scenarios covered: loop body trace per iteration, iteration-level error override (`CONTINUE` on a
jitter frame), cooperative stop signal checked at the iteration boundary, finally cleanup, and async
cancellation where the finally stage still closes the camera.

## Notes and limits

- **One execution = one thread, serial** (the opt-in `parallel(...)` group is the documented
  exception). `Pipeline` is immutable and reusable across threads; each execution gets its own
  `StageContext`.
- **Threads started inside a stage must be joined** before `handle()` returns (or implement
  `AsyncStage`), otherwise `abort()` misses in-flight work and trace timing becomes unreliable.
- **Cancellation is cooperative**: `future.cancel(true)` can only interrupt blocking operations;
  CPU-bound stages should poll `ctx.cancelToken().isCancelled()`. The framework cannot kill threads.
- **Shared external resources** (connection pools, stateful services) must be thread-safe on your
  side; the framework only guarantees that it shares nothing.
- **Interceptors, listeners and exporters never change an outcome** — their exceptions are swallowed.
- **Async always completes**: if the engine dies with an unexpected `Error`, or the executor rejects
  the task, `executeAsync` completes the handle exceptionally (`state() == FAILED`) instead of
  leaving `await()` blocked forever. Catch it through `handle.future()`.
- **Android**: compile and export with `scripts/export-lib.ps1 -AsModule` — add
  `coreLibraryDesugaring 'com.android.tools:desugar_jdk_libs:2.1.4'` for `java.time` and the
  `List.of` / `Map.of` factory methods, and set `minSdk 24` (`java.util.concurrent.CompletableFuture`).
  Three more Android specifics:

  ```kotlin
  // 1. bring your own executor: ForkJoinPool.commonPool() competes with the app's own work
  val ioPool = Executors.newFixedThreadPool(2)
  val handle = pipeline.executeAsync(payload, ioPool)
  pipeline.parallel("enrich") { p -> p.stages(a, b).executor(ioPool).failFast() }

  // 2. traces must live inside the app's storage - there is no usable working directory
  pipeline.addExporter(JsonTraceExporter(context.filesDir.toPath()) { m -> Log.w("Pipeline", m) }, ioPool)

  // 3. System.out is discarded on a device
  pipeline.addExporter(LogTraceExporter { line -> Log.d("Pipeline", line) })
  ```

  Never call `handle.await()` on the main thread.

Licensed under the [Apache License 2.0](LICENSE).
