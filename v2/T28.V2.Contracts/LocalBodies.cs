using System.Buffers.Binary;
using System.Text;

namespace T28.V2.Contracts;

public static class LocalBodies
{
    public static byte[] Hello()
    {
        byte[] body = new byte[8]; BinaryPrimitives.WriteUInt32BigEndian(body, V2Protocol.SampleRate);
        body[4] = body[5] = 1; BinaryPrimitives.WriteUInt16BigEndian(body.AsSpan(6), V2Protocol.SamplesPerPacket); return body;
    }
    public static byte[] Audio(ulong generation, ulong firstSample, ReadOnlySpan<byte> pcm)
    {
        if (generation == 0 || pcm.Length != V2Protocol.PcmBytes) throw new InvalidDataException("v2 PCM must identify one generation and contain exactly 20 ms.");
        byte[] body = new byte[16 + V2Protocol.PcmBytes]; BinaryPrimitives.WriteUInt64BigEndian(body, generation); BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8), firstSample); pcm.CopyTo(body.AsSpan(16)); return body;
    }
    public static (ulong Generation, ulong FirstSample, byte[] Pcm) Audio(ReadOnlySpan<byte> body) =>
        (BinaryPrimitives.ReadUInt64BigEndian(body), BinaryPrimitives.ReadUInt64BigEndian(body[8..]), body[16..].ToArray());
    public static byte[] Squelch(ulong requestId, short threshold)
    {
        byte[] body = new byte[10]; BinaryPrimitives.WriteUInt64BigEndian(body, requestId); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(8), threshold); return body;
    }
    public static (ulong RequestId, short Threshold) Squelch(ReadOnlySpan<byte> body) =>
        (BinaryPrimitives.ReadUInt64BigEndian(body), BinaryPrimitives.ReadInt16BigEndian(body[8..]));
    public static byte[] Control(ulong requestId, ulong generation, short threshold, short ctcssTenths)
    {
        if (requestId == 0 || generation == 0) throw new InvalidDataException("v2 control identity is required.");
        byte[] body = new byte[20]; BinaryPrimitives.WriteUInt64BigEndian(body, requestId); BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8), generation); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(16), threshold); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(18), ctcssTenths); return body;
    }
    public static (ulong RequestId, ulong Generation, short Threshold, short CtcssTenths) Control(ReadOnlySpan<byte> body) =>
        (BinaryPrimitives.ReadUInt64BigEndian(body), BinaryPrimitives.ReadUInt64BigEndian(body[8..]), BinaryPrimitives.ReadInt16BigEndian(body[16..]), BinaryPrimitives.ReadInt16BigEndian(body[18..]));
    public static byte[] ReceiverState(ulong generation, short threshold, short ctcssTenths, string profileId)
    {
        byte[] id = ProfileId(profileId);
        if (generation == 0 || threshold is < -1000 or > 0 || !CtcssTone.IsValidTenths(ctcssTenths)) throw new InvalidDataException("Invalid v2 receiver state.");
        byte[] body = new byte[13 + id.Length]; BinaryPrimitives.WriteUInt64BigEndian(body, generation); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(8), threshold); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(10), ctcssTenths); body[12] = (byte)id.Length; id.CopyTo(body, 13); return body;
    }
    public static (ulong Generation, short Threshold, short CtcssTenths, string ProfileId) ReceiverState(ReadOnlySpan<byte> body)
    {
        if (body.Length < 14 || body[12] == 0 || body.Length != 13 + body[12]) throw new InvalidDataException("Invalid v2 receiver state.");
        ulong generation = BinaryPrimitives.ReadUInt64BigEndian(body); short threshold = BinaryPrimitives.ReadInt16BigEndian(body[8..]); short tone = BinaryPrimitives.ReadInt16BigEndian(body[10..]); string profileId = ParseProfileId(body[13..]);
        if (generation == 0 || threshold is < -1000 or > 0 || !CtcssTone.IsValidTenths(tone)) throw new InvalidDataException("Invalid v2 receiver state.");
        return (generation, threshold, tone, profileId);
    }
    public static byte[] ChannelRegistry(IReadOnlyList<ApprovedChannelProjection> channels)
    {
        if (channels.Count is < 1 or > 255 || channels.Select(channel => channel.ProfileId).Distinct(StringComparer.Ordinal).Count() != channels.Count) throw new InvalidDataException("Invalid approved channel registry.");
        using MemoryStream body = new(); body.WriteByte((byte)channels.Count);
        foreach (ApprovedChannelProjection channel in channels)
        {
            byte[] id = ProfileId(channel.ProfileId), number = Friendly(channel.Number, 16, required: true), name = Friendly(channel.DisplayName, 64, required: false), planId = ProfileId(channel.PlanId), planName = Friendly(channel.PlanName, 64, required: true), planGroup = Friendly(channel.PlanGroup, 64, required: true);
            body.WriteByte((byte)id.Length); body.Write(id); body.WriteByte((byte)number.Length); body.Write(number); body.WriteByte((byte)name.Length); body.Write(name); body.WriteByte((byte)planId.Length); body.Write(planId); body.WriteByte((byte)planName.Length); body.Write(planName); body.WriteByte((byte)planGroup.Length); body.Write(planGroup);
        }
        if (body.Length > V2Protocol.MaxLocalMessage - 4) throw new InvalidDataException("Approved channel registry is too large.");
        return body.ToArray();
    }
    public static IReadOnlyList<ApprovedChannelProjection> ChannelRegistry(ReadOnlySpan<byte> body)
    {
        if (body.Length < 1 || body[0] == 0) throw new InvalidDataException("Invalid approved channel registry.");
        int offset = 1; List<ApprovedChannelProjection> channels = [];
        for (int index = 0; index < body[0]; index++)
        {
            ReadOnlySpan<byte> id = Field(body, ref offset, 64, required: true), number = Field(body, ref offset, 16, required: true), name = Field(body, ref offset, 64, required: false), planId = Field(body, ref offset, 64, required: true), planName = Field(body, ref offset, 64, required: true), planGroup = Field(body, ref offset, 64, required: true);
            string profileId = ParseProfileId(id), channelNumber = ParseFriendly(number, required: true), displayName = ParseFriendly(name, required: false);
            channels.Add(new(profileId, channelNumber, displayName.Length == 0 ? null : displayName, ParseProfileId(planId), ParseFriendly(planName, 64, required: true), ParseFriendly(planGroup, 64, required: true)));
        }
        if (offset != body.Length || channels.Select(channel => channel.ProfileId).Distinct(StringComparer.Ordinal).Count() != channels.Count) throw new InvalidDataException("Invalid approved channel registry.");
        return channels;
    }
    public static byte[] ChannelRequest(ChannelSwitchRequest request)
    {
        byte[] id = ProfileId(request.ProfileId); if (request.RequestId == Guid.Empty || request.ExpectedGeneration == 0) throw new InvalidDataException("Invalid v2 channel request.");
        byte[] body = new byte[25 + id.Length]; WriteGuid(body, request.RequestId); BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(16), request.ExpectedGeneration); body[24] = (byte)id.Length; id.CopyTo(body, 25); return body;
    }
    public static ChannelSwitchRequest ChannelRequest(ReadOnlySpan<byte> body)
    {
        if (body.Length < 26 || body[24] == 0 || body.Length != 25 + body[24]) throw new InvalidDataException("Invalid v2 channel request.");
        Guid requestId = ReadGuid(body); ulong generation = BinaryPrimitives.ReadUInt64BigEndian(body[16..]); string profileId = ParseProfileId(body[25..]);
        if (requestId == Guid.Empty || generation == 0) throw new InvalidDataException("Invalid v2 channel request."); return new(requestId, generation, profileId);
    }
    public static byte[] ChannelResult(ChannelSwitchResult result)
    {
        byte[] id = ProfileId(result.ProfileId);
        if (result.RequestId == Guid.Empty || !Enum.IsDefined(result.Code) || result.Generation == 0 || result.ThresholdTenths is < -1000 or > 0 || !CtcssTone.IsValidTenths(result.CtcssTenths)) throw new InvalidDataException("Invalid v2 channel result.");
        byte[] body = new byte[30 + id.Length]; WriteGuid(body, result.RequestId); body[16] = (byte)result.Code; BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(17), result.Generation); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(25), result.ThresholdTenths); BinaryPrimitives.WriteInt16BigEndian(body.AsSpan(27), result.CtcssTenths); body[29] = (byte)id.Length; id.CopyTo(body, 30); return body;
    }
    public static ChannelSwitchResult ChannelResult(ReadOnlySpan<byte> body)
    {
        if (body.Length < 31 || body[29] == 0 || body.Length != 30 + body[29] || !Enum.IsDefined((ChannelSwitchCode)body[16])) throw new InvalidDataException("Invalid v2 channel result.");
        ChannelSwitchResult result = new(ReadGuid(body), (ChannelSwitchCode)body[16], BinaryPrimitives.ReadUInt64BigEndian(body[17..]), ParseProfileId(body[30..]), BinaryPrimitives.ReadInt16BigEndian(body[25..]), BinaryPrimitives.ReadInt16BigEndian(body[27..]));
        if (result.RequestId == Guid.Empty || result.Generation == 0 || result.ThresholdTenths is < -1000 or > 0 || !CtcssTone.IsValidTenths(result.CtcssTenths)) throw new InvalidDataException("Invalid v2 channel result."); return result;
    }
    private static byte[] ProfileId(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 64 || value.Any(character => !char.IsAsciiLetterOrDigit(character) && character is not ('-' or '_'))) throw new InvalidDataException("Invalid approved channel profile identifier.");
        return Encoding.ASCII.GetBytes(value);
    }
    private static string ParseProfileId(ReadOnlySpan<byte> value) { string result = Encoding.ASCII.GetString(value); _ = ProfileId(result); return result; }
    private static byte[] Friendly(string? value, int maximumBytes, bool required)
    {
        value ??= string.Empty; if (value != value.Trim() || (required && value.Length == 0) || value.Any(char.IsControl)) throw new InvalidDataException("Invalid approved channel label.");
        byte[] bytes = Encoding.UTF8.GetBytes(value); if (bytes.Length > maximumBytes) throw new InvalidDataException("Approved channel label is too long."); return bytes;
    }
    private static string ParseFriendly(ReadOnlySpan<byte> value, bool required) => ParseFriendly(value, required ? 16 : 64, required);
    private static string ParseFriendly(ReadOnlySpan<byte> value, int maximumBytes, bool required) { string result = new UTF8Encoding(false, true).GetString(value); _ = Friendly(result, maximumBytes, required); return result; }
    private static ReadOnlySpan<byte> Field(ReadOnlySpan<byte> body, ref int offset, int maximum, bool required)
    {
        if (offset >= body.Length) throw new InvalidDataException("Invalid approved channel registry."); int length = body[offset++];
        if (length > maximum || (required && length == 0) || offset + length > body.Length) throw new InvalidDataException("Invalid approved channel registry."); ReadOnlySpan<byte> result = body.Slice(offset, length); offset += length; return result;
    }
    private static void WriteGuid(Span<byte> destination, Guid value) { if (!value.TryWriteBytes(destination, bigEndian: true, out int written) || written != 16) throw new InvalidDataException("Invalid request identity."); }
    private static Guid ReadGuid(ReadOnlySpan<byte> source) => new(source[..16], bigEndian: true);
}

public static class CtcssTone
{
    public const short None = -1, Default = 1622;
    private static readonly short[] Standard = [670,693,719,744,770,797,825,854,885,915,948,974,1000,1035,1072,1109,1148,1188,1230,1273,1318,1365,1413,1462,1514,1567,1598,1622,1655,1679,1713,1738,1773,1799,1835,1862,1899,1928,1966,1995,2035,2065,2107,2181,2257,2291,2336,2418,2503,2541];
    public static bool IsValidTenths(short value) => value == None || Array.IndexOf(Standard, value) >= 0;
}
