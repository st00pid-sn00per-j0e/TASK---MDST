namespace AgentClient.Core.Models;

public record CreateRunRequest(string Instruction);
public record CreateRunResponse(string RunId, string UploadUrl, string UploadMethod);

public record RunEvent(long Seq, string Type, string Message, DateTimeOffset At);

public record RunSnapshot(
    string RunId,
    string Status,
    int Attempt,
    string? ErrorClass,
    string? ErrorMessage,
    IReadOnlyList<RunEvent> Events);

