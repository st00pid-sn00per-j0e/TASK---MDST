
using System.Net.Http.Json;
using AgentClient.Core.Models;

namespace AgentClient.Core.Http;

public sealed class AgentApiClient
{
    private readonly HttpClient _http;

    public AgentApiClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<CreateRunResponse> CreateRunAsync(
        CreateRunRequest request, CancellationToken ct)
    {
        var resp = await _http.PostAsJsonAsync("/runs", request, ct);
        await EnsureOk(resp, ct);
        return (await resp.Content.ReadFromJsonAsync<CreateRunResponse>(ct))!;
    }




        public async Task StartRunAsync(string runId, CancellationToken ct)
    {
        var resp = await _http.PostAsync($"/runs/{runId}/start", null, ct);
        await EnsureOk(resp, ct);
    }

    public async Task CancelRunAsync(string runId, CancellationToken ct)
    {
        var resp = await _http.PostAsync($"/runs/{runId}/cancel", null, ct);
        await EnsureOk(resp, ct);
    }

    public async Task<RunSnapshot> GetRunAsync(
        string runId, long since, CancellationToken ct)
    {
        var resp = await _http.GetAsync($"/runs/{runId}?since={since}", ct);
        await EnsureOk(resp, ct);
        return (await resp.Content.ReadFromJsonAsync<RunSnapshot>(ct))!;
    }

        private static async Task EnsureOk(HttpResponseMessage resp, CancellationToken ct)
        {
            if (resp.IsSuccessStatusCode) return;
            var body = await resp.Content.ReadAsStringAsync(ct);
            throw new AgentApiException(
                (int)resp.StatusCode, body,
                $"API returned {(int)resp.StatusCode}: {body}");
        }


        public async Task<byte[]> DownloadOutputAsync(string runId, string fileName, CancellationToken ct)
        {
            var resp = await _http.GetAsync($"/runs/{runId}/outputs/{fileName}", ct);
            await EnsureOk(resp, ct);
            return await resp.Content.ReadAsByteArrayAsync(ct);
        }



    }

public sealed class AgentApiException : Exception
{
    public int StatusCode { get; }
    public string Body { get; }

    public AgentApiException(int statusCode, string body, string message)
        : base(message)
    {
        StatusCode = statusCode;
        Body = body;
    }
}