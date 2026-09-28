namespace T28.V2.Contracts;

/// <summary>
/// All-or-none owner-local configuration shared by the server and receiver.
/// The token is deliberately never projected by this type.
/// </summary>
public sealed record FleetAgentConfiguration(Uri ServerUrl, string ReceiverId, string AgentToken)
{
    public static FleetAgentConfiguration? Parse(string? serverUrl, string? receiverId, string? agentToken)
    {
        bool configured = !string.IsNullOrWhiteSpace(serverUrl) ||
            !string.IsNullOrWhiteSpace(receiverId) ||
            !string.IsNullOrWhiteSpace(agentToken);
        if (!configured) return null;
        if (!Uri.TryCreate(serverUrl, UriKind.Absolute, out Uri? parsed) ||
            parsed.Scheme is not ("http" or "https") ||
            !ValidReceiverId(receiverId) ||
            string.IsNullOrWhiteSpace(agentToken))
            throw new InvalidDataException("Fleet agent configuration requires a server URL, receiver identity, and owner-local token.");
        return new(parsed, receiverId!, agentToken);
    }

    private static bool ValidReceiverId(string? value) => value is { Length: >= 8 and <= 64 } &&
        value.All(c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-');
}
