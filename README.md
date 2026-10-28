# Java Pipeline Framework

A lightweight, embeddable, zero-dependency workflow pipeline framework for the JVM (JDK 17+).

It splits a business process (order processing, ETL, approval flow, message handling) into ordered,
independent, reusable and pluggable stages, and takes care of orchestration, data flow, cross-cutting
concerns, fault tolerance and flow control for you.

Reference models: Netty Pipeline (handler chain + context), Servlet Filter (interceptor chain),
simple workflow engines.

- [中文文档](README.zh-CN.md)

Released under the [Apache License 2.0](LICENSE).

## Contents

- [Features](#features)
- [Design goals](#design-goals)
- [Architecture](#architecture)
- [Core abstractions and design principles](#core-abstractions-and-design-principles)
- [Package structure](#package-structure)
- [Build](#build)
- [Quick start](#quick-start)
- [Writing a stage](#writing-a-stage)
- [Data flow model](#data-flow-model)
- [Flow control: stop, skip, cancel, timeout](#flow-control-stop-skip-cancel-timeout)
- [Error handling](#error-handling)
- [Interceptors](#interceptors)
- [Loop orchestration](#loop-orchestration)
- [finallyStage cleanup](#finallystage-cleanup)
- [Async execution and runtime query](#async-execution-and-runtime-query)
- [State model](#state-model)
- [Trace and exporters](#trace-and-exporters)
- [Threading model](#threading-model)
- [Example 1: order pipeline](#example-1-order-pipeline)
- [Example 2: recording pipeline with loop](#example-2-recording-pipeline-with-loop)

## Features

| Capability | Status |
|------------|--------|
| Ordered stage execution with a shared context (`payload` + `attrs`) | Done |
| Conditional routing per stage (`accept`) | Done |
| Flow control: `abort()` / `skipRest()` / `exitLoop()` | Done |
| Interceptors (`beforeStage` / `afterStage` / `onStageError`) | Done |
| Error policy: global `ErrorHandler`, loop-level override | Done |
| `LoopStage`: `while_` / `maxIterations` / iteration-aware trace | Done |
| `finallyStage` cleanup on every exit path | Done |
| `executeAsync` + `ExecutionHandle` (state / currentStage / iteration / cancel / await) | Done |
| Cooperative cancellation via `CancelToken`, per-execution timeout | Done |
| Trace capture + Log / JSON / Mermaid exporters | Done |
| Listener events (`PipelineListener` / `PipelineEvent`) | Done |
| Retry policy, YAML DSL, sub-pipeline, parallel stages (DAG) | Planned |

## Design goals

- **Orchestration**: stage registration, ordering, sequential execution
- **Data flow**: a shared context (Context) across stages
- **Cross-cutting concerns**: logging, timing, auditing → handled by interceptors, business stages stay pure
- **Fault tolerance**: continue or abort after a stage fails → configurable policy
- **Flow control**: early abort (e.g. risk rejection), skipping remaining stages

## Architecture (three layers)

```text
┌───────────────── API layer (what users face) ─────────────────┐
│  PipelineBuilder (fluent)   Stage impls (business)   execute() │
├──────────────────── Core engine layer ────────────────────────┤
│  Pipeline (orchestration)   StageContext (ctx + flow control)  │
│  Interceptor (AOP)          ErrorHandler (failure policy)      │
├──────────────── Extension modules (opt-in) ───────────────────┤
│  Trace/Exporter │ Retry/Timeout │ Listener │ YAML DSL │ Metrics│
└───────────────────────────────────────────────────────────────┘
```

## Core abstractions and design principles

| Abstraction | Responsibility | Key members |
|-------------|----------------|-------------|
| `Stage<C>` | Smallest processing unit; one stage does one thing | `name()` / `accept(ctx)` conditional routing / `handle(ctx)` |
| `StageContext<C>` | Data carrier + flow controller: payload, attrs, abort/skip/exit flags | `payload()` / `setAttr` / `getAttr` / `abort()` / `skipRest()` / `exitLoop()` |
| `Pipeline<C>` | Holds the stage list; drives execution, interception and error routing | `execute(C)` / `executeAsync(C)` / `executeAsync(C, Executor)` |
| `ExecutionHandle<C>` | Runtime handle for async executions | `state()` / `currentStage()` / `iteration()` / `cancel()` / `await()` / `future()` |
| `Interceptor<C>` | Around advice for every stage | `beforeStage` / `afterStage` / `onStageError` |
| `ErrorHandler<C>` | Decision after a stage throws: `CONTINUE` or `ABORT` | `onError(stage, ctx, e)` |
| `PipelineBuilder<C>` | Fluent assembly; `build()` yields an immutable `Pipeline` | `addStage` / `loop` / `finallyStage` / `errorHandler` / `build` |

**Design principles**:

1. **Minimal interfaces** — only 5 core abstractions; `Stage` is the only mandatory one (functional interface, lambda friendly)
2. **Immutable pipeline** — the stage list is read-only after `build()`; pipelines are stateless and reusable across threads
3. **Explicit control flow** — abort/skip via context flags instead of exception-based control flow; exceptions are for real errors only
4. **Generic `<C>` end to end** — the same engine runs order contexts, file contexts, message contexts

## Package structure

```text
src/com/devin/pipeline/
├── core/                    engine and core abstractions
│   ├── Stage.java           the only interface business code must implement
│   ├── AsyncStage.java      stage returning a CompletableFuture (joined by the engine)
│   ├── StageContext.java    payload + attrs + flow control, one per execution
│   ├── Pipeline.java        engine: execute / executeAsync / LoopStage / StageRunner
│   ├── ExecutionHandle.java async runtime handle (state, current stage, cancel, await)
│   ├── control/             flow control and policies
│   │   ├── CancelToken.java     external cancellation flag (volatile)
│   │   ├── ErrorAction.java     CONTINUE / ABORT
│   │   ├── ErrorHandler.java    per-stage failure decision
│   │   └── Interceptor.java     AOP around each stage
│   ├── execution/           states and results
│   │   ├── ExecutionState.java  PENDING / RUNNING / COMPLETED / ABORTED / FAILED / CANCELLED / TIMEOUT
│   │   ├── StageStatus.java     SKIPPED / SUCCESS / FAILED / CANCELLED
│   │   ├── FlowReason.java      unified exit reason (type + code + message)
│   │   └── ExecutionResult.java terminal state + trace + payload
│   ├── trace/               trace data model and export SPI
│   │   ├── ExecutionTrace.java  one execution trace
│   │   ├── StageRecord.java     one stage record
│   │   └── TraceExporter.java   exporter SPI
│   └── event/               events and listeners
│       ├── PipelineEvent.java     structured stage/state event
│       └── PipelineListener.java  listener callbacks
├── builder/
│   └── PipelineBuilder.java   fluent assembly (addStage / loop / finallyStage / ...)
└── exporter/                 built-in exporters
    ├── LogTraceExporter.java    structured log (default)
    ├── JsonTraceExporter.java   trace-<id>.json
    └── MermaidExporter.java     trace-<id>.md flowchart
```

## Build

```bash
# Compile (JDK 17+)
javac -encoding UTF-8 -d out/classes $(find src -name "*.java")

# Maven
mvn -q compile
```

The framework itself has zero runtime dependencies. Exporters write to a directory you choose
(default in the examples: `out/traces/`).

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

Imports used above:

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

## Writing a stage

```java
// Lambda form
Stage<Order> validate = Stage.of("validate input", ctx -> {
    Order o = ctx.payload();
    if (o.id == null || o.id.isBlank()) {
        throw new IllegalArgumentException("missing order id");
    }
    ctx.setAttr("riskScore", score(o));       // pass data to later stages
});

// Class form with conditional routing
public final class PayStage implements Stage<Order> {
    @Override public String name() { return "payment"; }

    @Override public boolean accept(StageContext<Order> ctx) {
        return "NEW".equals(ctx.payload().status);   // false -> SKIPPED, not an error
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

Stage lifecycle inside the engine:

```text
for each stage:
  1. accept(ctx)?            false -> record SKIPPED, continue
  2. interceptor.beforeStage
  3. stage.handle(ctx)       may call abort() / skipRest() / exitLoop()
  4. interceptor.afterStage
  5. on exception: interceptor.onStageError -> errorHandler.onError
                   CONTINUE -> next stage | ABORT -> stop pipeline
  6. check stop flags (abort / skip / cancel / timeout) at stage boundary
```

## Data flow model

**Core mechanism: a single context object passed by reference.** `execute(payload)` creates
**one** `StageContext<C>`; the same reference flows through all stages, zero copy:

| Channel | What goes in | Example |
|---------|--------------|---------|
| **payload (main)** | the business object being processed | `Order`: validation fills fields, risk check tags, inventory mutates |
| **attrs (auxiliary)** | intermediate / derived data across stages | `riskScore=87`, `payUrl`, generated serial number |

Stages mutate the payload in place and later stages see it directly — "message passing" in this
framework is equivalent to "sequential processing of one shared object".

**Inter-stage contract**: a weak contract (by convention); the framework does not validate data
types. Recommended practices:

- Use type-safe `TypedKey<T>` attribute keys instead of magic strings, collected in constant
  classes (e.g. `OrderAttrs`) as a visible "data contract table" between stages
- Document per stage: which attrs it requires and which it produces
- Defense: `accept()` may check missing prerequisites → return false to skip, or fail fast

**Why mutable reference passing (instead of immutable messages)**: the typical payload is "one
large business object processed sequentially", where mutable references win on zero-copy
performance and readability; the weakness (hard to replay history) is compensated by trace
capture (attrs snapshots + payload serialization hooks). Single-threaded serial execution +
reference passing → visibility between stages is guaranteed by program order, no sync cost.

**Red line**: a stage must not hand the payload reference to other threads without joining them
before `handle()` returns, or later stages may read incomplete data.

**Cross-pipeline flow**: after `pipelineA.execute(order)`, hand the same `order` (or its attrs)
to `pipelineB` — the payload itself is the carrier; nested sub-pipelines share the current ctx.

## Flow control: stop, skip, cancel, timeout

Stopping is **cooperative**: the engine only checks stop conditions at stage boundaries, so a
running `handle()` always completes and is never killed by the framework.

| Trigger | API | Terminal state |
|---------|-----|----------------|
| Business decision inside a stage | `ctx.abort()` / `ctx.abort("risk rejected")` / `ctx.skipRest()` | `ABORTED` / `COMPLETED` |
| Error policy | `ErrorHandler` returns `ABORT` | `FAILED` |
| External cancellation | `handle.cancel()` or `CancelToken.cancel()` from another thread | `CANCELLED` |
| Timeout | `.timeout(ms)` on the builder | `TIMEOUT` |
| Leave a loop only | `ctx.exitLoop()` | loop ends, pipeline continues |

```java
CancelToken token = new CancelToken();          // shared with a message listener
messageListener.onStop(msg -> token.cancel("remote stop"));
```

Typical wiring for "stop after receiving a message": external listener → holds the execution's
`CancelToken` (or `ExecutionHandle`) → calls `cancel()` on stop message → effective at the next
stage boundary.

**Checkpoint timing**: checked uniformly **before each stage starts** (the single decision point
in the execution loop):

```text
continue condition = !aborted && !skipped && !cancelled && thread not interrupted
```

No checks mid-stage (stages needing fast cancel response can poll `token.isCancelled()` themselves).

**After stopping** (all triggers follow the same path):

1. Write the final `ExecutionTrace` state (COMPLETED / ABORTED / FAILED / CANCELLED)
2. Finished `StageRecord`s are kept; stages after the stop point are recorded as SKIPPED / CANCELLED
3. **Exporters still run** — auditing must answer "where did it stop and why"
4. The `CompletableFuture` completes normally with the final result — **cancellation does not throw**;
   it is one of the normal outcomes, not an error

**Limits of interrupt**: `future.cancel(true)` can only interrupt blocking operations
(sleep / IO / blocking queue take); CPU-bound stages never see it and must poll `token`
themselves. The framework cannot provide thread killing — that is a JVM constraint.

## Error handling

```java
.errorHandler((stage, ctx, e) -> {
    log.warn("stage {} failed", stage.name(), e);
    return e instanceof TimeoutException ? ErrorAction.CONTINUE : ErrorAction.ABORT;
})
```

- Default policy: `ABORT` on any exception.
- Loop-level override wins inside a loop: `.onIterationError(ErrorAction.CONTINUE)` (e.g. drop one frame and keep recording).
- `onStageError` is invoked before `ErrorHandler`, so interceptors see every failure.

## Interceptors

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

Typical uses: logging, timing, auditing, transaction control, gray-release checks.
Registration order is execution order: `beforeStage` in order, `afterStage` in reverse, so nesting
semantics hold.

## Loop orchestration

```java
.loop(loop -> loop
        .name("record")
        .stages(capture, encode)                 // body, executed in order per iteration
        .while_(ctx -> !Boolean.TRUE.equals(ctx.<Boolean>getAttr("stopRequested")))
        .maxIterations(100_000)                  // safety bound; trace marked truncated if hit
        .onIterationError(ErrorAction.CONTINUE)) // per-iteration error policy
```

**Why a framework-level `LoopStage`**:

| Approach | Description | Problem |
|----------|-------------|---------|
| while loop inside one Stage | body stuffed into one handle() | body cannot be traced / intercepted / error-limited per stage; one trace record runs for hours; not reusable |
| **Framework LoopStage (adopted)** | body is a sub-stage sequence reusing the execution loop | needs loop boundaries and iteration index (cheap; the engine already has all the pieces) |

Semantics:

- The loop condition is checked **before each iteration** (same cooperative model as stopping):
  after an external stop message flips `stopRequested` or cancels the token, the current
  iteration finishes and the loop exits
- `ctx.exitLoop()` leaves the loop after the current iteration without terminating the pipeline;
  `abort()` / `skipRest()` semantics pass through loops unchanged
- `ctx.currentLoop().iteration` gives the current iteration (1-based); each stage of each iteration
  produces its own `StageRecord` (`loop=record, iteration=N`), keeping fine-grained observability
  for long tasks
- Reaching `maxIterations` marks the trace `truncated` and finishes as `COMPLETED`

## finallyStage cleanup

```java
.finallyStage(Stage.of("close camera", ctx -> close(ctx.payload())))
```

Resource-opening pipelines must guarantee cleanup: **runs on every exit path — normal completion,
`abort`, `skipRest`, cancel, timeout and exceptions** (same semantics as `try-finally`).
Multiple finally stages run in registration order; their exceptions are recorded in the trace but
do not replace the original failure reason.

Relation to loops: finallyStage lives at the pipeline level; cleanup of resources opened inside a
loop belongs in the last stage of the loop body, or use abort pass-through + finally as a backstop.

## Async execution and runtime query

```java
ExecutionHandle<RecordSession> handle = pipeline.executeAsync(session, executor);

handle.state();          // RUNNING / COMPLETED / ... (volatile snapshot)
handle.currentStage();   // stage currently executing
handle.iteration();      // current loop iteration, 0 when not in a loop
handle.cancel("user requested");
ExecutionResult<RecordSession> result = handle.await();   // or handle.future()
```

A single worker thread runs all stages serially; semantics are identical to `execute(payload)`.
The same `Pipeline` instance can be executed concurrently by many threads.

**Query options (pick by scenario)**:

| Scenario | Means | Notes |
|----------|-------|-------|
| Self-check inside a stage | state flags on `ctx` | business logic senses its own progress |
| Polling by the async submitter | `handle.state()` / `currentStage()` / `iteration()` | volatile snapshot reads, good for dashboards |
| Waiting for the result | `handle.future()` (CompletableFuture) or `await()` | returns `ExecutionResult` (terminal state + duration + trace) |
| Event push | `PipelineListener.onStateChanged(old, new, reason)` | callback on every state transition, no polling needed |
| Post-hoc audit | exported Json / Mermaid trace files | terminal state + per-stage details, the authoritative record |

Synchronous `execute()` has no intermediate query problem — the caller thread is occupied and the
return value is the terminal state (`ExecutionResult`).

## State model

```text
PENDING --submit--> RUNNING --+--> COMPLETED   (finished, or skipRest)
                              +--> ABORTED     (ctx.abort)
                              +--> FAILED      (exception + ABORT policy)
                              +--> CANCELLED   (external cancel)
                              +--> TIMEOUT     (deadline exceeded)
```

- Terminal states are mutually exclusive, one-shot and irreversible.
- Only the executing thread writes state; state fields are published via volatile, external readers
  use snapshot reads without locks. Reading RUNNING + a stage name means "at this moment it has at
  least reached here" — no nanosecond precision promised.
- `FlowReason` separates state from reason: `type` (`NORMAL`, `ABORTED`, `FAILED`, `CANCELLED`,
  `TIMEOUT`, `LOOP_TRUNCATED`, `REJECTED`) + `code` + `message`, for troubleshooting, auditing,
  monitoring and reporting.

**Terminal state rules**:

| Trigger | Terminal state |
|---------|----------------|
| All stages finished / `skipRest()` | COMPLETED |
| `ctx.abort()` inside a stage | ABORTED |
| Exception + ErrorHandler ABORT | FAILED |
| `token.cancel()` / `handle.cancel()` | CANCELLED |
| `.timeout(ms)` exceeded | TIMEOUT |
| `maxIterations` bound reached | COMPLETED (trace marked truncated, i.e. not a natural end) |

## Trace and exporters

| Type | Content | Form | Scenario |
|------|---------|------|----------|
| **Dynamic execution export** | per-stage status, timing, duration, errors | structured log / JSON / Mermaid | troubleshooting, auditing, monitoring, review |

Every execution produces an `ExecutionTrace` (traceId, pipeline name, start/end, duration,
truncated flag, failure reason, `FlowReason`) holding one `StageRecord` per stage execution
(index, stage name, loop name, iteration, status, duration, error).

| Exporter | Output |
|----------|--------|
| `LogTraceExporter` | Structured log: one summary line + one line per stage (default) |
| `JsonTraceExporter(Path)` | `trace-<id>.json` for machines / log platforms |
| `MermaidExporter(Path)` | `trace-<id>.md` flowchart colored by status (green=success, gray=skipped, red=failed, orange=cancelled) |

Recommended combination: structured log (live troubleshooting) + Mermaid report (post-hoc review),
with JSON left for machine consumption.

Custom exporter:

```java
public final class MetricsExporter implements TraceExporter {
    @Override public void export(ExecutionTrace t) {
        for (StageRecord r : t.records()) {
            registry.timer("pipeline.stage", "name", r.stageName()).record(r.durationMs());
        }
    }
}
```

Events are also pushed per execution:

```java
.addListener(new PipelineListener<>() {
    @Override public void onEvent(PipelineEvent e) {
        auditSink.write(e);   // STAGE_START / STAGE_FINISH / STATE_CHANGED
    }
})
```

**Design points**:

- Capture happens automatically in the engine; business stages and interceptors are **zero-intrusion**
- `ExecutionTrace` / `StageRecord` are core-layer data models; `TraceExporter` and its
  implementations are pluggable
- traceId spans one execution; outputs of multiple exporters can be correlated by traceId

## Threading model

**Core principle: one execution = one thread, serial. Concurrency only happens between executions.**

| Form | Threads | Notes |
|------|---------|-------|
| `execute(payload)` | caller thread all the way | stages, interceptors, error handling, trace capture and exporters all run on the current thread; no locks, no contention |
| `executeAsync(payload, Executor)` | one worker thread, serial | the caller only submits; stages still run sequentially on one thread, identical semantics to the sync version |
| Concurrent executions | many workers / caller threads | the same Pipeline instance can be executed concurrently |

**Why no locks are needed**:

1. `Pipeline` is immutable — stage list, interceptors and config are read-only after `build()` → safe to share
2. `StageContext` is created per execution — zero shared state between executions → naturally isolated
3. One execution is single-threaded — stages have a natural happens-before (program order), so the
   context can use a plain `HashMap`; no ConcurrentHashMap needed

**Boundaries users must respect**:

- Threads started inside a stage: parallelism is allowed (e.g. batch external calls) but must be
  joined before `handle()` returns (or implement `AsyncStage`); otherwise `abort()` semantics miss
  in-flight work and trace timing becomes unreliable
- Shared external resources: stateful objects referenced by stages (connection pools, stateful
  services) must be made thread-safe by the user; the framework only guarantees that the framework
  itself shares nothing
- Exporter threads: by default triggered synchronously on the executing thread (trace is complete
  before export)

**Why parallel stages (DAG) need caution**: once stages run in parallel inside one execution, all
three premises above break — context attribute access needs synchronization or per-stage
partitioning, abort becomes coordinated cancellation, and trace ordering needs concurrent
collection. That is why DAG is evaluated separately instead of being pushed into the core engine.

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
