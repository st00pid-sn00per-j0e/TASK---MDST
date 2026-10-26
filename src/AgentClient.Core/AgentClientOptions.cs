namespace AgentClient.Core;

public sealed record AgentClientOptions
{
    public required string BaseUrl { get; init; }
    public int MaxReconnectAttempts { get; init; } = 8;
    public int KeepAliveSeconds { get; init; } = 20;
}
