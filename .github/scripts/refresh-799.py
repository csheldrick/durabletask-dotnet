from pathlib import Path

TRACE_HELPER = Path("src/Shared/Grpc/Tracing/TraceHelper.cs")
PROCESSOR = Path("src/Worker/Grpc/GrpcDurableTaskWorker.Processor.cs")
INDEX = Path("src/Worker/Grpc/TracingHistoryEventIndex.cs")
TRACE_TESTS = Path("test/Worker/Grpc.Tests/TraceHelperTests.cs")
INDEX_TESTS = Path("test/Worker/Grpc.Tests/TracingHistoryEventIndexTests.cs")

trace = TRACE_HELPER.read_text()
needle = '    static readonly ActivitySource ActivityTraceSource = new ActivitySource(Source);\\n'
addition = '''    static readonly ActivitySource ActivityTraceSource = new ActivitySource(Source);

    /// <summary>
    /// Gets whether any listener is subscribed to Durable Task tracing activities.
    /// </summary>
    /// <returns><see langword="true"/> when the activity source has at least one listener; otherwise, <see langword="false"/>.</returns>
    public static bool HasListeners() => ActivityTraceSource.HasListeners();
'''
assert needle in trace, "TraceHelper insertion point moved"
assert "public static bool HasListeners()" not in trace, "HasListeners already exists upstream"
trace = trace.replace(needle, addition, 1)
TRACE_HELPER.write_text(trace)

processor = PROCESSOR.read_text()
start = '            IReadOnlyList<P.HistoryEvent> pastEvents = materializedPastEvents ?? request.PastEvents;\\n'
end = '            OrchestratorExecutionResult? result = null;\\n'
start_i = processor.index(start)
end_i = processor.index(end, start_i)

replacement = '''            IReadOnlyList<P.HistoryEvent> pastEvents = materializedPastEvents ?? request.PastEvents;
            bool hasTraceListeners = TraceHelper.HasListeners();

            P.ExecutionStartedEvent? executionStartedEvent = null;
            if (hasTraceListeners || isInitialRewind)
            {
                executionStartedEvent =
                    request
                        .NewEvents
                        .Concat(pastEvents)
                        .Where(e => e.EventTypeCase == P.HistoryEvent.EventTypeOneofCase.ExecutionStarted)
                        .Select(e => e.ExecutionStarted)
                        .FirstOrDefault();
            }

            if (isInitialRewind)
            {
                if (executionStartedEvent is null)
                {
                    throw new InvalidOperationException("Rewinding orchestration has no ExecutionStartedEvent in its history");
                }

                if (rewindEvent!.ParentTraceContext is not null)
                {
                    executionStartedEvent = executionStartedEvent.Clone();
                    executionStartedEvent.ParentTraceContext = rewindEvent.ParentTraceContext;
                }
            }

            // A rewind starts a new orchestration span instead of continuing the failed execution's stored span.
            P.OrchestrationTraceContext? orchestrationTraceContext =
                isInitialRewind ? null : request.OrchestrationTraceContext;
            Activity? traceActivity = hasTraceListeners
                ? TraceHelper.StartTraceActivityForOrchestrationExecution(
                    executionStartedEvent,
                    orchestrationTraceContext)
                : null;

            if (isInitialRewind)
            {
                await this.CompleteOrchestratorTaskWithChunkingAsync(
                    RewindOrchestrationHandler.CreateResponse(
                        request,
                        pastEvents,
                        completionToken,
                        traceActivity),
                    this.worker.grpcOptions.CompleteOrchestrationWorkItemChunkSizeInBytes,
                    cancellationToken);
                return;
            }

            if (hasTraceListeners && executionStartedEvent is not null)
            {
                TracingHistoryEventIndex historyEventIndex = new(pastEvents);

                foreach (var newEvent in request.NewEvents)
                {
                    switch (newEvent.EventTypeCase)
                    {
                        case P.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceCompleted:
                            {
                                P.HistoryEvent? subOrchestrationInstanceCreatedEvent =
                                    historyEventIndex.GetSubOrchestrationInstanceCreatedEvent(
                                        newEvent.SubOrchestrationInstanceCompleted.TaskScheduledId);

                                TraceHelper.EmitTraceActivityForSubOrchestrationCompleted(
                                    request.InstanceId,
                                    subOrchestrationInstanceCreatedEvent,
                                    subOrchestrationInstanceCreatedEvent?.SubOrchestrationInstanceCreated);
                                break;
                            }

                        case P.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceFailed:
                            {
                                P.HistoryEvent? subOrchestrationInstanceCreatedEvent =
                                    historyEventIndex.GetSubOrchestrationInstanceCreatedEvent(
                                        newEvent.SubOrchestrationInstanceFailed.TaskScheduledId);

                                TraceHelper.EmitTraceActivityForSubOrchestrationFailed(
                                    request.InstanceId,
                                    subOrchestrationInstanceCreatedEvent,
                                    subOrchestrationInstanceCreatedEvent?.SubOrchestrationInstanceCreated,
                                    newEvent.SubOrchestrationInstanceFailed);
                                break;
                            }

                        case P.HistoryEvent.EventTypeOneofCase.TaskCompleted:
                            {
                                P.HistoryEvent? taskScheduledEvent =
                                    historyEventIndex.GetTaskScheduledEvent(newEvent.TaskCompleted.TaskScheduledId);

                                TraceHelper.EmitTraceActivityForTaskCompleted(
                                    request.InstanceId,
                                    taskScheduledEvent,
                                    taskScheduledEvent?.TaskScheduled);
                                break;
                            }

                        case P.HistoryEvent.EventTypeOneofCase.TaskFailed:
                            {
                                P.HistoryEvent? taskScheduledEvent =
                                    historyEventIndex.GetTaskScheduledEvent(newEvent.TaskFailed.TaskScheduledId);

                                TraceHelper.EmitTraceActivityForTaskFailed(
                                    request.InstanceId,
                                    taskScheduledEvent,
                                    taskScheduledEvent?.TaskScheduled,
                                    newEvent.TaskFailed);
                                break;
                            }

                        case P.HistoryEvent.EventTypeOneofCase.TimerFired:
                            TraceHelper.EmitTraceActivityForTimer(
                                request.InstanceId,
                                executionStartedEvent.Name,
                                newEvent.Timestamp.ToDateTime(),
                                newEvent.TimerFired);
                            break;
                    }
                }
            }

'''
processor = processor[:start_i] + replacement + processor[end_i:]
PROCESSOR.write_text(processor)

INDEX.write_text('''// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc;

/// <summary>
/// Indexes the orchestration history events used to reconstruct tracing spans.
/// </summary>
sealed class TracingHistoryEventIndex
{
    readonly Dictionary<int, P.HistoryEvent> subOrchestrationCreatedEvents = new();
    readonly Dictionary<int, P.HistoryEvent> taskScheduledEvents = new();

    public TracingHistoryEventIndex(IEnumerable<P.HistoryEvent> pastEvents)
    {
        foreach (P.HistoryEvent historyEvent in pastEvents)
        {
            switch (historyEvent.EventTypeCase)
            {
                case P.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceCreated:
                    // Preserve the previous FirstOrDefault semantics for duplicate IDs.
                    this.subOrchestrationCreatedEvents.TryAdd(historyEvent.EventId, historyEvent);
                    break;

                case P.HistoryEvent.EventTypeOneofCase.TaskScheduled:
                    // Preserve the previous LastOrDefault semantics for duplicate IDs.
                    this.taskScheduledEvents[historyEvent.EventId] = historyEvent;
                    break;
            }
        }
    }

    public P.HistoryEvent? GetSubOrchestrationInstanceCreatedEvent(int eventId)
        => this.subOrchestrationCreatedEvents.TryGetValue(eventId, out P.HistoryEvent? historyEvent)
            ? historyEvent
            : null;

    public P.HistoryEvent? GetTaskScheduledEvent(int eventId)
        => this.taskScheduledEvents.TryGetValue(eventId, out P.HistoryEvent? historyEvent)
            ? historyEvent
            : null;
}
''')

TRACE_TESTS.write_text('''// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.DurableTask.Tracing;

namespace Microsoft.DurableTask.Worker.Grpc.Tests;

public class TraceHelperTests
{
    [Fact]
    public void HasListeners_TracksMatchingActivityListener()
    {
        bool initialHasListeners = TraceHelper.HasListeners();
        ActivityListener listener = new()
        {
            ShouldListenTo = source => source.Name == "Microsoft.DurableTask",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        };

        try
        {
            ActivitySource.AddActivityListener(listener);

            TraceHelper.HasListeners().Should().BeTrue();
        }
        finally
        {
            listener.Dispose();
        }

        TraceHelper.HasListeners().Should().Be(initialHasListeners);
    }
}
''')

INDEX_TESTS.write_text('''// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using P = Microsoft.DurableTask.Protobuf;

namespace Microsoft.DurableTask.Worker.Grpc.Tests;

public class TracingHistoryEventIndexTests
{
    [Fact]
    public void GetSubOrchestrationInstanceCreatedEvent_DuplicateIds_ReturnsFirstEvent()
    {
        P.HistoryEvent first = new()
        {
            EventId = 7,
            SubOrchestrationInstanceCreated = new P.SubOrchestrationInstanceCreatedEvent { Name = "first" },
        };
        P.HistoryEvent second = new()
        {
            EventId = 7,
            SubOrchestrationInstanceCreated = new P.SubOrchestrationInstanceCreatedEvent { Name = "second" },
        };

        TracingHistoryEventIndex index = new([first, second]);

        index.GetSubOrchestrationInstanceCreatedEvent(7).Should().BeSameAs(first);
    }

    [Fact]
    public void GetTaskScheduledEvent_DuplicateIds_ReturnsLastEvent()
    {
        P.HistoryEvent first = new()
        {
            EventId = 11,
            TaskScheduled = new P.TaskScheduledEvent { Name = "first" },
        };
        P.HistoryEvent second = new()
        {
            EventId = 11,
            TaskScheduled = new P.TaskScheduledEvent { Name = "second" },
        };

        TracingHistoryEventIndex index = new([first, second]);

        index.GetTaskScheduledEvent(11).Should().BeSameAs(second);
    }

    [Fact]
    public void Lookups_MissingIds_ReturnNull()
    {
        P.HistoryEvent unrelated = new()
        {
            EventId = 3,
            TimerCreated = new P.TimerCreatedEvent(),
        };

        TracingHistoryEventIndex index = new([unrelated]);

        index.GetSubOrchestrationInstanceCreatedEvent(3).Should().BeNull();
        index.GetTaskScheduledEvent(3).Should().BeNull();
    }
}
''')

print("Reapplied #799 tracing optimization to current upstream main")
