// using System.Threading.Channels;
// using System.Collections.Concurrent;

// var builder = WebApplication.CreateBuilder(args);
// var app = builder.Build();
// app.UseWebSockets();

// var runs = new ConcurrentDictionary<string, RunState>();


// app.MapPost("/runs", (CreateRunRequest req) =>
// {
//     if (string.IsNullOrWhiteSpace(req.Instruction))
//         return Results.BadRequest(new {error = "instructions required"});

//         var id = Guid.NewGuid().ToString("N")[..12];
//         runs[id] = new RunState(id, req.Instruction);
//         return Results.Ok(new CreateRunResponse(id, $"/upload/{id}", "PUT"));
// });


// app.MapGet("/", () => "ok");
// app.Run();


// record CreateRunRequest(string? Instruction);
// record CreateRunResponse(string RunId, string UploadUrl, string UploadMethod);
// record RunEvent(long Seq, string Type, string Message, DateTimeOffset At);



// class RunState
// {
//     private readonly object _lock = new();
//     private readonly List<RunEvent> _events = new();
//     private readonly List<Channel<RunEvent>> _subscribers = new();
//     private long _seq = 0;
//     private string _status = "queued";
//     private bool _cancelled = false;
//     private bool _started = false;
//     private string? _errorClass;
//     private string? _errorMessage;
//     private readonly string _scenario;

//     public RunState(string id, string instructions)
//     {
//         Id = id;
//         _scenario = instructions.Contains("[code_error]") == true ? "code_error" :
//                     instructions.Contains("[timeout]") == true ? "timeout" :
//                     instructions.Contains("[cancel]") == true ? "cancel" :
//                     instructions.Contains("[unresolved]") == true ? "unresolved" :
//                     instructions.Contains("[sandbox_unavailable]") == true ? "sandbox_unavailable" :
//                     "completed";


//     }

//     public void Start()
//     {
//         lock (_lock)
//         {
//             if (_started) return;
//             _started = true;
//         }

//         Task.Run(async () =>
//         {
//             Emit("running", "Run started");
//             await Task.Delay(1000);

//             if (_scenario == "code_error")
//             {
//                 Emit("failed", "Run failed due to code error", "code_error");
//             }
//             else if (_scenario == "timeout")
//             {
//                 Emit("failed", "Run failed due to timeout", "timeout");
//             }
//             else if (_scenario == "cancel")
//             {
//                 Emit("cancelled", "Run was cancelled by user");
//             }
//             else if (_scenario == "unresolved")
//             {
//                 Emit("failed", "Run failed due to unresolved dependencies", "unresolved");
//             }
//             else if (_scenario == "sandbox_unavailable")
//             {
//                 Emit("failed", "Run failed because sandbox is unavailable", "sandbox_unavailable");
//             }
//             else
//             {
//                 Emit("completed", "Run completed successfully");
//             }
//         });


//         Cancel()
//         {
//          lock (_lock)
//             {
//                 if (lock (_lock) _cancelled == true) return;
//                 _cancelled = true;
//                 Emit("cancelled", "Run was cancelled by user");
//             }   
//         }

//     }

//     public string Id { get; }

//     public static bool IsTerminal(string type) => type is "completed" or "cancelled" or "failed";

//     private void Emit(string type, string message, string? errorClass = null)
//     {
//         lock (_lock)
//         {
//             if (errorClass is not null) _errorClass = errorClass;

//             _seq++;
//             var e = new RunEvent(_seq, type, message, DateTimeOffset.UtcNow);
//             _events.Add(e);

//             if (IsTerminal(type))
//             {
//                 _status = type;
//             }
//             else if (_status == "queued")
//             {
//                 _status = "running";
//             }

//             foreach (var ch in _subscribers)
//             {
//                 ch.Writer.TryWrite(e);
//                 if (IsTerminal(type))
//                 {
//                     ch.Writer.TryComplete();
//                 }
//             }
//         }
//     }
    
// }



using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.UseWebSockets();

var runs = new ConcurrentDictionary<string, RunState>();

app.MapPost("/runs", (CreateRunRequest req) =>
{
    if (string.IsNullOrWhiteSpace(req.Instruction))
        return Results.BadRequest(new { error = "instructions required" });

    var id = Guid.NewGuid().ToString("N")[..12];
    runs[id] = new RunState(id, req.Instruction);
    return Results.Ok(new CreateRunResponse(id, $"/upload/{id}", "PUT"));
});

app.MapPut("/upload/{id}", async (string id, HttpRequest request) =>
{
    if (!runs.ContainsKey(id)) return Results.NotFound();
    await request.Body.CopyToAsync(Stream.Null);
    return Results.Ok(new { bytes = request.ContentLength ?? 0 });
});

app.MapPost("/runs/{id}/start", (string id) =>
{
    if (!runs.TryGetValue(id, out var run)) return Results.NotFound();
    run.Start();
    return Results.Ok();
});

app.MapPost("/runs/{id}/cancel", (string id) =>
{
    if (!runs.TryGetValue(id, out var run)) return Results.NotFound();
    run.Cancel();
    return Results.Ok();
});

app.MapGet("/runs/{id}", (string id, long? since) =>
{
    if (!runs.TryGetValue(id, out var run)) return Results.NotFound();
    return Results.Ok(run.RunSnapshot(since ?? 0));
});

app.MapGet("/runs", () =>
    runs.Values
        .Select(r => new
        {
            runId = r.Id,
            status = r.GetStatus(),
            createdAt = r.CreatedAt,
            instruction = r.Instruction
        })
        .OrderByDescending(r => r.createdAt)
        .ToList());

app.MapGet("/runs/{id}/outputs/cleaned.csv", (string id) =>
    runs.TryGetValue(id, out var r) && r.Csv != null
        ? Results.File(r.Csv, "text/csv", "cleaned.csv")
        : Results.NotFound());

app.MapGet("/runs/{id}/outputs/report.json", (string id) =>
    runs.TryGetValue(id, out var r) && r.Report != null
        ? Results.File(r.Report, "application/json", "report.json")
        : Results.NotFound());

app.Map("/runs/{id}/ws", async (string id, HttpContext ctx) =>
{
    if (!runs.TryGetValue(id, out var run)) { ctx.Response.StatusCode = 404; return; }
    if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }

    long since = long.TryParse(ctx.Request.Query["since"], out var s) ? s : 0;

    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    await using var sub = run.Subscribe(since);

    await foreach (var evt in sub.Reader.ReadAllAsync(ctx.RequestAborted))
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(evt);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, ctx.RequestAborted);
        if (RunState.IsTerminal(evt.Type)) break;
    }
});

app.MapGet("/", () => "ok");
app.Run();

record CreateRunRequest(string? Instruction);
record CreateRunResponse(string RunId, string UploadUrl, string UploadMethod);
record RunEvent(long Seq, string Type, string Message, DateTimeOffset At);

class RunState
{
    private readonly object _lock = new();
    private readonly List<RunEvent> _events = new();
    private readonly List<Channel<RunEvent>> _subscribers = new();
    private long _seq = 0;
    private string _status = "queued";
    private bool _cancelled = false;
    private bool _started = false;
    private string? _errorClass;
    private string? _errorMessage;
    private readonly string _scenario;

    public string Instruction { get; }
    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
    public byte[]? Csv { get; private set; }
    public byte[]? Report { get; private set; }

    public string GetStatus()
    {
        lock (_lock) return _status;
    }

    public RunState(string id, string instructions)
    {
        Id = id;
        Instruction = instructions;
        _scenario =
            instructions.Contains("[code_error]") ? "code_error" :
            instructions.Contains("[timeout]") ? "timeout" :
            instructions.Contains("[cancel]") ? "cancel" :
            instructions.Contains("[unresolved]") ? "unresolved" :
            instructions.Contains("[sandbox_unavailable]") ? "sandbox_unavailable" :
            "completed";
    }

    public string Id { get; }

    public static bool IsTerminal(string type) =>
        type is "completed" or "failed" or "cancelled";

    public void Start()
    {
        lock (_lock)
        {
            if (_started) return;
            _started = true;
        }

        Task.Run(async () =>
        {
            try
            {
                Emit("running", "Run started");
                await Task.Delay(1000);

                if (_scenario == "code_error")
                {
                    Fail("code_error", "Run failed due to code error");
                }
                else if (_scenario == "timeout")
                {
                    Fail("timeout", "Run failed due to timeout");
                }
                else if (_scenario == "cancel")
                {
                    Emit("attempt_started", "Attempt 1: executing (waiting for cancel)");
                    while (true)
                    {
                        await Task.Delay(200);
                        lock (_lock) { if (_cancelled) break; }
                    }
                    Emit("cancelled", "Cancelled by user");
                }
                else if (_scenario == "unresolved")
                {
                    Fail("unresolved", "Run failed due to unresolved dependencies");
                }
                else if (_scenario == "sandbox_unavailable")
                {
                    Fail("sandbox_unavailable", "Run failed because sandbox is unavailable");
                }
                else
                {
                    Emit("completed", "Run completed successfully");
                    Csv = System.Text.Encoding.UTF8.GetBytes("id,name,stock\n1,Widget,10\n2,Gadget,5\n");
                    Report = System.Text.Encoding.UTF8.GetBytes(
                        "{\"resolved\":2,\"unresolved\":0,\"warnings\":[]}");
                }
            }
            catch (Exception ex)
            {
                Fail("internal_error", ex.Message);
            }
        });
    }

    public void Cancel()
    {
        lock (_lock) _cancelled = true;
    }

    public object RunSnapshot(long since)
    {
        lock (_lock)
        {
            return new
            {
                runId = Id,
                status = _status,
                attempt = 1,
                errorClass = _errorClass,
                errorMessage = _errorMessage,
                events = _events.Where(e => e.Seq > since).ToList()
            };
        }
    }

    public Subscription Subscribe(long since)
    {
        var ch = Channel.CreateUnbounded<RunEvent>();
        lock (_lock)
        {
            _subscribers.Add(ch);
            foreach (var e in _events.Where(e => e.Seq > since))
                ch.Writer.TryWrite(e);
            if (IsTerminal(_status))
                ch.Writer.TryComplete();
        }
        return new Subscription(this, ch);
    }

    public void Unsubscribe(Channel<RunEvent> ch)
    {
        lock (_lock) _subscribers.Remove(ch);
    }

    private void Fail(string errorClass, string message)
    {
        lock (_lock) _errorMessage = message;
        Emit("failed", $"{errorClass}: {message}", errorClass);
    }

    private void Emit(string type, string message, string? errorClass = null)
    {
        lock (_lock)
        {
            if (errorClass is not null) _errorClass = errorClass;

            _seq++;
            var e = new RunEvent(_seq, type, message, DateTimeOffset.UtcNow);
            _events.Add(e);

            if (IsTerminal(type))
                _status = type;
            else if (_status == "queued")
                _status = "running";

            foreach (var ch in _subscribers)
            {
                ch.Writer.TryWrite(e);
                if (IsTerminal(type))
                    ch.Writer.TryComplete();
            }
        }
    }

    public sealed class Subscription : IAsyncDisposable
    {
        private readonly RunState _state;
        public Subscription(RunState state, Channel<RunEvent> channel)
        {
            _state = state;
            Channel = channel;
        }
        public Channel<RunEvent> Channel { get; }
        public ChannelReader<RunEvent> Reader => Channel.Reader;
        public ValueTask DisposeAsync()
        {
            _state.Unsubscribe(Channel);
            return ValueTask.CompletedTask;
        }
    }
}
