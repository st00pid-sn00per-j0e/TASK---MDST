using System.Net.Http.Headers;
using System.Net;

namespace AgentClient.Core.Http;

public sealed class UploadService
{
    private readonly HttpClient _http;

    public UploadService(HttpClient http)
    {
        _http = http;
    }

    public async Task UploadAsync(
        string filePath,
        string uploadUrl,
        string uploadMethod,
        IProgress<long>? progress,
        CancellationToken ct)
    {
        await using var file = File.OpenRead(filePath);
        var content = new ProgressStreamContent(file, progress);

        content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        content.Headers.ContentLength = file.Length;

        using var req = new HttpRequestMessage(
            new HttpMethod(uploadMethod.ToUpperInvariant()), uploadUrl);
        req.Content = content;

        using var resp = await _http.SendAsync(
            req, HttpCompletionOption.ResponseHeadersRead, ct);

        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct);
            throw new AgentApiException(
                (int)resp.StatusCode, body,
                $"Upload failed {(int)resp.StatusCode}: {body}");
        }
    }
}



    internal sealed class ProgressStreamContent : HttpContent
{
    private readonly Stream _source;
    private readonly IProgress<long>? _progress;
    private long _total;

    public ProgressStreamContent(Stream source, IProgress<long>? progress)
    {
        _source = source;
        _progress = progress;
    }

    protected override async Task SerializeToStreamAsync(
        Stream stream, TransportContext? context)
    {
        var buffer = new byte[81920];
        int read;
        while ((read = await _source.ReadAsync(buffer)) > 0)
        {
            await stream.WriteAsync(buffer.AsMemory(0, read));
            _total += read;
            _progress?.Report(_total);
        }
    }

    protected override bool TryComputeLength(out long length)
    {
        length = _source.Length;
        return true;
    }
}