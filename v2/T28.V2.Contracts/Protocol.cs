using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace T28.V2.Contracts;

public static class V2Protocol
{
    public const int SampleRate = 48_000, SamplesPerPacket = 960, PcmBytes = 1_920, MaxLocalMessage = 32_768, BrowserHeaderBytes = 40;
    public const string Subprotocol = "t28.pcm.v2";
    public static byte[] Frame(LocalType type, ReadOnlySpan<byte> body)
    {
        if (body.Length + 4 > MaxLocalMessage) throw new InvalidDataException("v2 local message is too large.");
        byte[] value = new byte[4 + 4 + body.Length]; BinaryPrimitives.WriteUInt32BigEndian(value, (uint)(4 + body.Length));
        value[4] = 2; value[5] = (byte)type; value[6] = value[7] = 0; body.CopyTo(value.AsSpan(8)); return value;
    }
    public static async Task<LocalEnvelope> ReadLocalAsync(Stream stream, CancellationToken token)
    {
        byte[] prefix = new byte[4]; await ReadExactlyAsync(stream, prefix, token);
        uint bodyLength = BinaryPrimitives.ReadUInt32BigEndian(prefix);
        if (bodyLength < 4 || bodyLength > MaxLocalMessage) throw new InvalidDataException("Malformed v2 local length.");
        byte[] frame = new byte[4 + bodyLength]; prefix.CopyTo(frame, 0);
        await ReadExactlyAsync(stream, frame.AsMemory(4), token);
        return ParseLocal(frame);
    }
    public static Task WriteLocalAsync(Stream stream, LocalType type, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        byte[] frame = Frame(type, body.Span); return stream.WriteAsync(frame, token).AsTask();
    }
    private static async Task ReadExactlyAsync(Stream stream, Memory<byte> buffer, CancellationToken token)
    {
        int read = 0; while (read < buffer.Length) { int n = await stream.ReadAsync(buffer[read..], token); if (n == 0) throw new EndOfStreamException("v2 local session ended."); read += n; }
    }
    public static LocalEnvelope ParseLocal(ReadOnlySpan<byte> value)
    {
        if (value.Length < 8 || BinaryPrimitives.ReadUInt32BigEndian(value) != value.Length - 4 || value.Length - 4 > MaxLocalMessage) throw new InvalidDataException("Malformed v2 local length.");
        if (value[4] != 2 || value[6] != 0 || value[7] != 0 || !Enum.IsDefined((LocalType)value[5])) throw new InvalidDataException("Malformed v2 local envelope.");
        LocalEnvelope result = new((LocalType)value[5], value[8..].ToArray()); Validate(result); return result;
    }
    public static void Validate(LocalEnvelope message)
    {
        int length = message.Body.Length;
        if (message.Type == LocalType.Hello && (length != 8 || BinaryPrimitives.ReadUInt32BigEndian(message.Body) != SampleRate || message.Body[4] != 1 || message.Body[5] != 1 || BinaryPrimitives.ReadUInt16BigEndian(message.Body.AsSpan(6)) != SamplesPerPacket)) throw new InvalidDataException("Invalid v2 hello.");
        if (message.Type == LocalType.Audio && (length != 16 + PcmBytes || BinaryPrimitives.ReadUInt64BigEndian(message.Body) == 0)) throw new InvalidDataException("Invalid v2 audio.");
        if ((message.Type is LocalType.SetSquelch or LocalType.SquelchApplied) && (length != 10 || BinaryPrimitives.ReadInt16BigEndian(message.Body.AsSpan(8)) is < -1000 or > 0)) throw new InvalidDataException("Invalid v2 squelch.");
        if ((message.Type is LocalType.SetControl or LocalType.ControlApplied) && (length != 20 || BinaryPrimitives.ReadUInt64BigEndian(message.Body.AsSpan(8)) == 0 || BinaryPrimitives.ReadInt16BigEndian(message.Body.AsSpan(16)) is < -1000 or > 0 || !CtcssTone.IsValidTenths(BinaryPrimitives.ReadInt16BigEndian(message.Body.AsSpan(18))))) throw new InvalidDataException("Invalid v2 control.");
        if (message.Type == LocalType.ReceiverState) _ = LocalBodies.ReceiverState(message.Body);
        if (message.Type == LocalType.ChannelRegistry) _ = LocalBodies.ChannelRegistry(message.Body);
        if (message.Type == LocalType.SetChannel) _ = LocalBodies.ChannelRequest(message.Body);
        if (message.Type == LocalType.ChannelResult) _ = LocalBodies.ChannelResult(message.Body);
        if (message.Type == LocalType.Status && (length != 12 || message.Body[8] > 2 || message.Body[9] > 2 || BinaryPrimitives.ReadInt16BigEndian(message.Body.AsSpan(10)) is < -1000 or > 0)) throw new InvalidDataException("Invalid v2 status.");
    }
    public static byte[] BrowserPacket(ulong attempt, ulong sequence, ulong firstSample, ReadOnlySpan<byte> pcm)
    {
        if (attempt == 0 || pcm.Length != PcmBytes) throw new InvalidDataException("v2 PCM must be exactly 20 ms.");
        byte[] value = new byte[BrowserHeaderBytes + PcmBytes]; Encoding.ASCII.GetBytes("T28P", value); value[4] = 2; value[5] = 1; BinaryPrimitives.WriteUInt16BigEndian(value.AsSpan(6), BrowserHeaderBytes); BinaryPrimitives.WriteUInt64BigEndian(value.AsSpan(8), attempt); BinaryPrimitives.WriteUInt64BigEndian(value.AsSpan(16), sequence); BinaryPrimitives.WriteUInt64BigEndian(value.AsSpan(24), firstSample); BinaryPrimitives.WriteUInt32BigEndian(value.AsSpan(32), SampleRate); BinaryPrimitives.WriteUInt16BigEndian(value.AsSpan(36), SamplesPerPacket); value[38] = value[39] = 1; pcm.CopyTo(value.AsSpan(BrowserHeaderBytes)); return value;
    }
    public static BrowserPacket ParseBrowser(ReadOnlySpan<byte> value)
    {
        if (value.Length != BrowserHeaderBytes + PcmBytes || Encoding.ASCII.GetString(value[..4]) != "T28P" || value[4] != 2 || value[5] != 1 || BinaryPrimitives.ReadUInt16BigEndian(value[6..]) != BrowserHeaderBytes || BinaryPrimitives.ReadUInt64BigEndian(value[8..]) == 0 || BinaryPrimitives.ReadUInt32BigEndian(value[32..]) != SampleRate || BinaryPrimitives.ReadUInt16BigEndian(value[36..]) != SamplesPerPacket || value[38] != 1 || value[39] != 1) throw new InvalidDataException("Malformed t28.pcm.v2 packet.");
        return new BrowserPacket(BinaryPrimitives.ReadUInt64BigEndian(value[8..]), BinaryPrimitives.ReadUInt64BigEndian(value[16..]), BinaryPrimitives.ReadUInt64BigEndian(value[24..]), value[BrowserHeaderBytes..].ToArray());
    }
    public static ClientMessage ParseClient(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length is 0 or > 128) throw new InvalidDataException("Invalid v2 client control length.");
        using JsonDocument doc = JsonDocument.Parse(bytes.ToArray()); JsonElement root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object || root.EnumerateObject().Any(x => x.Name is not ("type" or "attempt" or "sequence" or "paused" or "packets")) || !root.TryGetProperty("type", out JsonElement type) || !root.TryGetProperty("attempt", out JsonElement attempt) || !attempt.TryGetUInt64(out ulong a) || a == 0) throw new InvalidDataException("Invalid v2 client control.");
        if (type.GetString() == "played" && root.TryGetProperty("sequence", out JsonElement sequence) && sequence.TryGetUInt64(out ulong n) && n > 0 && root.EnumerateObject().Count() == 3) return new ClientMessage(a, null, n, 0);
        if (type.GetString() == "consume" && root.TryGetProperty("sequence", out JsonElement consumeSequence) && consumeSequence.TryGetUInt64(out ulong consumed) && consumed > 0 && root.TryGetProperty("packets", out JsonElement replacement) && replacement.TryGetInt32(out int count) && count == 1 && root.EnumerateObject().Count() == 4) return new ClientMessage(a, null, consumed, 1, true);
        if (type.GetString() == "flow" && root.TryGetProperty("paused", out JsonElement paused) && (paused.ValueKind is JsonValueKind.True or JsonValueKind.False) && root.EnumerateObject().Count() == 3) return new ClientMessage(a, paused.GetBoolean(), null, 0);
        if (type.GetString() == "credit" && root.TryGetProperty("packets", out JsonElement packets) && packets.TryGetInt32(out int credit) && credit is > 0 and <= 4 && root.EnumerateObject().Count() == 3) return new ClientMessage(a, null, null, credit);
        throw new InvalidDataException("Invalid v2 client control.");
    }
}
public enum LocalType : byte { Hello = 1, Audio = 2, SetSquelch = 3, SquelchApplied = 4, Status = 5, SetControl = 6, ControlApplied = 7, ReceiverState = 8, ChannelRegistry = 9, SetChannel = 10, ChannelResult = 11 }
public sealed record LocalEnvelope(LocalType Type, byte[] Body);
public sealed record BrowserPacket(ulong Attempt, ulong Sequence, ulong FirstSample, byte[] Pcm);
public sealed record ClientMessage(ulong Attempt, bool? Paused, ulong? Played, int Credit, bool IsConsume = false);
public sealed record ApprovedChannelProjection(string ProfileId, string Number, string? DisplayName, string PlanId = "network-control", string PlanName = "Demo location 52", string PlanGroup = "Other approved categories")
{
    public string FriendlyName => string.IsNullOrWhiteSpace(DisplayName) ? Number : $"{Number} - {DisplayName}";
}
public enum ChannelSwitchCode : byte { Applied = 1, UnknownProfile = 2, StaleGeneration = 3, ReplacementFailed = 4, RequestConflict = 5 }
public sealed record ChannelSwitchRequest(Guid RequestId, ulong ExpectedGeneration, string ProfileId);
public sealed record ChannelSwitchResult(Guid RequestId, ChannelSwitchCode Code, ulong Generation, string ProfileId, short ThresholdTenths, short CtcssTenths);
