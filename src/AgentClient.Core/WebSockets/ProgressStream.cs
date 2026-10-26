using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using AgentClient.Core.Http;
using AgentClient.Core.Models;
using System.Net.Http.Json;
using System.Diagnostics.Tracing;

namespace AgentClient.Core.WebSockets;

public sealed class ProgressStream
{
    private readonly HttpClient _http;
    private readonly AgentClientOptions _opts;

    public ProgressStream(HttpClient http, AgentClientOptions opts)
    {
        _http = http;
        _opts = opts;
    }

    public async IAsyncEnumerable<RunEvent> StreamAsync(
        string runId,
        long since,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var lastSeq = since;
        var attempt = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // 1) Replay backlog via GET
            var snapshot = await GetSnapshotAsync(runId, lastSeq, ct);
            foreach (var e in snapshot.Events)
            {
                if (e.Seq <= lastSeq) continue;
                lastSeq = e.Seq;
                yield return e;
            }

            if (IsTerminal(snapshot.Status))
                yield break;

            // 2) Subscribe live via WebSocket
            var wsUrl = _opts.BaseUrl.Replace("https://", "wss://").Replace("http://", "ws://")
                        + $"/runs/{runId}/ws?since={lastSeq}";

            using var ws = new ClientWebSocket();
            ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(_opts.KeepAliveSeconds);

            try
            {
                await ws.ConnectAsync(new Uri(wsUrl), ct);
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                if (++attempt >= _opts.MaxReconnectAttempts)
                    throw;

                await BackoffAsync(attempt, ct);
                continue;
            }

            attempt = 0;

            var buffer = new byte[8192];
            var msgBuffer = new MemoryStream();

            while (true)
            {
                RunEvent? evt = null;
                try
                {
                    var result = await ws.ReceiveAsync(buffer, ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                        break;

                    msgBuffer.Write(buffer, 0, result.Count);
                    if (!result.EndOfMessage) continue;

                    var json = msgBuffer.ToArray();
                    msgBuffer.SetLength(0);

                    evt = JsonSerializer.Deserialize<RunEvent>(json, JsonOpts)!;
                 } catch (WebSocketException) when (!ct.IsCancellationRequested)
                    {
                        break;
                    }

                    if (evt is null) continue;
                    if (evt.Seq <= lastSeq) continue;

                    lastSeq = evt.Seq;
                    yield return evt;


                    if (IsTerminal(evt.Type))
                {
                    yield break;
                }

            }

            // Socket closed unexpectedly — retry from lastSeq
            if (++attempt >= _opts.MaxReconnectAttempts)
                throw new InvalidOperationException(
                    $"Reconnect attempts exceeded ({_opts.MaxReconnectAttempts})");

            await BackoffAsync(attempt, ct);
        }
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private async Task<RunSnapshot> GetSnapshotAsync(
        string runId, long since, CancellationToken ct)
    {
        using var resp = await _http.GetAsync($"/runs/{runId}?since={since}", ct);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct);
            throw new AgentApiException(
                (int)resp.StatusCode, body,
                $"Snapshot fetch failed {(int)resp.StatusCode}: {body}");
        }
        return (await resp.Content.ReadFromJsonAsync<RunSnapshot>(ct))!;
    }

    private static async Task BackoffAsync(int attempt, CancellationToken ct)
    {
        var secs = Math.Min(Math.Pow(2, attempt), 30);
        var jitter = Random.Shared.Next(0, 1000) / 1000.0;
        await Task.Delay(TimeSpan.FromSeconds(secs + jitter), ct);
    }

    private static bool IsTerminal(string s) =>
        s is "completed" or "failed" or "cancelled";
}