using System.Text.Json;
using T28.V2.Contracts;

namespace T28.V2.Receiver;

public sealed record ReceiverChildInputs(
    string Executable,
    string AdapterPath,
    string ProfilePath,
    bool Synthetic = false,
    string? SyntheticControlMode = null,
    string? SyntheticReadinessMode = null,
    bool SyntheticDiagnostics = false,
    int? SyntheticBurstMilliseconds = null);

public sealed record ApprovedReceiverProfile(
    string ProfileId,
    string Number,
    string? DisplayName,
    ReceiverChildInputs Child,
    short ThresholdTenths = -1000,
    short CtcssTenths = CtcssTone.Default,
    string PlanId = "network-control",
    string PlanName = "Demo location 52",
    string PlanGroup = "Other approved categories")
{
    public ApprovedChannelProjection Projection => new(ProfileId, Number, DisplayName, PlanId, PlanName, PlanGroup);
}

/// <summary>
/// Immutable owner-local registry. Child inputs never cross the receiver UDS;
/// only <see cref="ApprovedChannelProjection"/> values are published.
/// </summary>
public sealed class ReceiverProfileRegistry
{
    private readonly Dictionary<string, ApprovedReceiverProfile> _profiles;

    public ReceiverProfileRegistry(IEnumerable<ApprovedReceiverProfile> profiles, string activeProfileId)
    {
        ApprovedReceiverProfile[] bounded = profiles.ToArray();
        if (bounded.Length is < 1 or > 255) throw new InvalidDataException("The receiver requires one to 255 approved profiles.");
        bounded = bounded.Select(Canonicalize).ToArray();
        _ = LocalBodies.ChannelRegistry(bounded.Select(profile => profile.Projection).ToArray());
        if (bounded.Any(profile => profile.ThresholdTenths is < -1000 or > 0 || !CtcssTone.IsValidTenths(profile.CtcssTenths))) throw new InvalidDataException("An approved profile has invalid receive controls.");
        _profiles = bounded.ToDictionary(profile => profile.ProfileId, StringComparer.Ordinal);
        if (!_profiles.ContainsKey(activeProfileId)) throw new InvalidDataException("The active receiver profile is not approved.");
        ActiveProfileId = activeProfileId;
        PublicProfiles = bounded.Select(profile => profile.Projection).ToArray();
    }

    public string ActiveProfileId { get; }
    public IReadOnlyList<ApprovedChannelProjection> PublicProfiles { get; }
    public ApprovedReceiverProfile Active => _profiles[ActiveProfileId];
    public bool TryGet(string profileId, out ApprovedReceiverProfile? profile) => _profiles.TryGetValue(profileId, out profile);

    public static ReceiverProfileRegistry LoadFromEnvironment()
    {
        string? registryPath = Environment.GetEnvironmentVariable("T28_V2_CHANNEL_REGISTRY");
        if (!string.IsNullOrWhiteSpace(registryPath)) return Load(registryPath);

        string executable = Environment.GetEnvironmentVariable("T28_V2_GNURADIO") ?? "python3";
        string profilePath = RequiredAbsoluteFile("T28_V2_PROFILE", Environment.GetEnvironmentVariable("T28_V2_PROFILE"));
        string adapterPath = RequiredAbsoluteFile("T28_V2_ADAPTER", Environment.GetEnvironmentVariable("T28_V2_ADAPTER"));
        bool synthetic = Environment.GetEnvironmentVariable("T28_V2_SYNTHETIC") == "1";
        int? burst = int.TryParse(Environment.GetEnvironmentVariable("T28_V2_SYNTHETIC_BURST_MS"), out int configuredBurst) ? configuredBurst : null;
        ReceiverChildInputs child = new(executable, adapterPath, profilePath, synthetic, SyntheticBurstMilliseconds: burst);
        ApprovedReceiverProfile fallback = new("lab-demo-hill", "371", "Demo Hill", child);
        ValidatePrivateProfile(fallback, ApprovedChannelCatalogue.All.Single(value => value.PlanId == "network-control" && value.Number == "371"));
        return new([fallback], "lab-demo-hill");
    }

    public static ReceiverProfileRegistry Load(string path)
    {
        path = RequiredAbsoluteFile("T28_V2_CHANNEL_REGISTRY", path);
        try
        {
            RegistryDocument? document = JsonSerializer.Deserialize<RegistryDocument>(File.ReadAllText(path), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            if (document is null || document.Profiles is null) throw new InvalidDataException();
            ApprovedReceiverProfile[] profiles = document.Profiles.Select(item =>
            {
                ReceiverChildInputs child = new(
                    item.Executable ?? "python3",
                    RequiredAbsoluteFile("adapterPath", item.AdapterPath),
                    RequiredAbsoluteFile("profilePath", item.ProfilePath),
                    item.Synthetic,
                    item.SyntheticControlMode,
                    item.SyntheticReadinessMode,
                    item.SyntheticDiagnostics,
                    item.SyntheticBurstMilliseconds);
                ValidateSynthetic(child);
                ApprovedReceiverProfile profile = new(item.ProfileId ?? string.Empty, item.Number ?? string.Empty, item.DisplayName, child, item.ThresholdTenths ?? -1000, item.CtcssTenths ?? CtcssTone.Default, item.PlanId ?? string.Empty, item.PlanName ?? string.Empty, item.PlanGroup ?? string.Empty);
                if (!ApprovedChannelCatalogue.TryGet(profile.PlanId, profile.Number, out ApprovedChannelDefinition? definition) || definition is null) throw new InvalidDataException("Profile is not an approved plan member.");
                if (profile.DisplayName != definition.DisplayName || profile.PlanName != definition.PlanName || profile.PlanGroup != definition.PlanGroup) throw new InvalidDataException("Profile declares a forged approved identity.");
                ValidatePrivateProfile(profile, definition);
                return profile;
            }).ToArray();
            return new(profiles, document.ActiveProfileId ?? string.Empty);
        }
        catch (Exception error) when (error is JsonException or IOException or UnauthorizedAccessException or InvalidDataException or ArgumentException)
        {
            throw new InvalidDataException("The owner-local approved channel registry is invalid.", error);
        }
    }

    private static string RequiredAbsoluteFile(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || !Path.IsPathFullyQualified(value) || !File.Exists(value)) throw new InvalidDataException($"{name} must identify an existing absolute owner-local file.");
        return value;
    }

    private static ApprovedReceiverProfile Canonicalize(ApprovedReceiverProfile profile)
    {
        if (!ApprovedChannelCatalogue.TryGet(profile.PlanId, profile.Number, out ApprovedChannelDefinition? definition) || definition is null) throw new InvalidDataException("Profile is not an approved plan member.");
        if (profile.DisplayName != definition.DisplayName || profile.PlanName != definition.PlanName || profile.PlanGroup != definition.PlanGroup) throw new InvalidDataException("Profile declares a forged approved identity.");
        return profile with { Number = definition.Number, DisplayName = definition.DisplayName, PlanName = definition.PlanName, PlanGroup = definition.PlanGroup };
    }

    private static void ValidatePrivateProfile(ApprovedReceiverProfile profile, ApprovedChannelDefinition definition)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(File.ReadAllText(profile.Child.ProfilePath)); JsonElement root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("profile_id", out JsonElement id) || id.GetString() != profile.ProfileId || !root.TryGetProperty("frequency_hz", out JsonElement frequency) || !frequency.TryGetInt64(out long actualFrequency) || actualFrequency != definition.FrequencyHz || !root.TryGetProperty("channel_bandwidth_hz", out JsonElement bandwidth) || !bandwidth.TryGetInt64(out long actualBandwidth) || actualBandwidth != 12_500) throw new InvalidDataException("Private profile does not match its approved channel.");
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException or InvalidDataException)
        {
            throw new InvalidDataException("Private profile does not match its approved channel.", error);
        }
    }

    private static void ValidateSynthetic(ReceiverChildInputs child)
    {
        string[] controls = ["valid", "malformed", "mismatch", "timeout", "eof", "duplicate", "out-of-order"];
        if (child.SyntheticControlMode is not null && !controls.Contains(child.SyntheticControlMode, StringComparer.Ordinal)) throw new InvalidDataException("Invalid synthetic control fixture.");
        if (child.SyntheticReadinessMode is not null && child.SyntheticReadinessMode is not ("ready" or "delayed" or "malformed" or "control" or "duplicate" or "timeout" or "eof" or "late")) throw new InvalidDataException("Invalid synthetic readiness fixture.");
        if (child.SyntheticBurstMilliseconds is { } burst && burst is < 500 or > 550) throw new InvalidDataException("Invalid synthetic burst fixture.");
        if (!child.Synthetic && (child.SyntheticControlMode is not null || child.SyntheticReadinessMode is not null || child.SyntheticDiagnostics || child.SyntheticBurstMilliseconds is not null)) throw new InvalidDataException("Synthetic child settings require a synthetic profile.");
    }

    private sealed record RegistryDocument(string? ActiveProfileId, RegistryProfile[]? Profiles);
    private sealed record RegistryProfile(
        string? ProfileId,
        string? Number,
        string? DisplayName,
        string? Executable,
        string? AdapterPath,
        string? ProfilePath,
        bool Synthetic,
        string? SyntheticControlMode,
        string? SyntheticReadinessMode,
        bool SyntheticDiagnostics,
        int? SyntheticBurstMilliseconds,
        short? ThresholdTenths,
        short? CtcssTenths,
        string? PlanId,
        string? PlanName,
        string? PlanGroup);
}

public interface IReceiverChildFactory
{
    Task<IGnuRadioV2> CreateAsync(ApprovedReceiverProfile profile, CancellationToken token);
}

public sealed class GnuRadioChildFactory : IReceiverChildFactory
{
    public Task<IGnuRadioV2> CreateAsync(ApprovedReceiverProfile profile, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        ReceiverChildInputs child = profile.Child;
        IGnuRadioV2 result = new GnuRadioProcessAdapter(child.Executable, child.AdapterPath, child.ProfilePath, child.Synthetic, child.SyntheticControlMode, child.SyntheticReadinessMode, child.SyntheticDiagnostics, child.SyntheticBurstMilliseconds);
        return Task.FromResult(result);
    }
}
