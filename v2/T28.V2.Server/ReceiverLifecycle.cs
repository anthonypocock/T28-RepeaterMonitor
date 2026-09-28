using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace T28.V2.Server;

/// <summary>Durable, secret-free receiver enrolment state.  Runtime sessions and
/// media remain in <see cref="FleetRegistry"/> and are deliberately not stored here.</summary>
public enum ReceiverLifecycleState { PendingEnrolment, Active, DisablePendingStop, Disabled, DecommissionPendingStop, RecoveryRequired, Decommissioned }
public enum EnrolmentOfferStatus { Issued, Consumed, Cancelled, Expired }
public enum AgentCredentialStatus { PendingProof, Active, Revoked, Expired }
public enum ReceiverLifecycleGoal { Disabled, Decommissioned, CredentialExpired }
public sealed record ManagedReceiver(string ReceiverId, string DisplayName, ReceiverLifecycleState LifecycleState, long ManagementRevision, long CredentialEpoch, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt, DateTimeOffset? RestoreUntil = null, DateTimeOffset? TombstoneExpiresAt = null, ReceiverLifecycleGoal? PendingGoal = null);
public sealed record EnrolmentOfferRecord(string EnrolmentId, string ReceiverId, string Purpose, string? Verifier, int PepperVersion, EnrolmentOfferStatus Status, DateTimeOffset IssuedAt, DateTimeOffset ExpiresAt, DateTimeOffset? ConsumedAt = null, DateTimeOffset? CancelledAt = null, string? PendingCredentialId = null);
public sealed record AgentCredentialRecord(string CredentialId, string ReceiverId, string? Verifier, int PepperVersion, AgentCredentialStatus Status, long CredentialEpoch, DateTimeOffset IssuedAt, DateTimeOffset NotAfter, DateTimeOffset ProofDeadline, DateTimeOffset? FirstAuthenticatedAt = null, DateTimeOffset? LastAuthenticatedAt = null, DateTimeOffset? RevokedAt = null, string? RevokeReason = null);
public sealed record AgentAuthentication(string ReceiverId, string CredentialId, AgentCredentialStatus CredentialStatus, ReceiverLifecycleState LifecycleState, long CredentialEpoch)
{
    public bool RecoveryOnly => LifecycleState is ReceiverLifecycleState.DisablePendingStop or ReceiverLifecycleState.DecommissionPendingStop or ReceiverLifecycleState.RecoveryRequired;
}
public sealed record ReceiverLifecycleAudit(string AuditId, DateTimeOffset At, string ReceiverId, string EventType, long RevisionBefore, long RevisionAfter, string Outcome);
public sealed record ReceiverLifecycleDocument(ManagedReceiver[] Receivers, EnrolmentOfferRecord[] Offers, AgentCredentialRecord[] Credentials, ReceiverLifecycleAudit[] Audit);
public interface IReceiverLifecycleStore { ReceiverLifecycleDocument Load(); void Save(ReceiverLifecycleDocument document); }
public sealed class JsonReceiverLifecycleStore(string path) : IReceiverLifecycleStore
{
    public ReceiverLifecycleDocument Load()
    {
        if (!File.Exists(path)) return new([], [], [], []);
        using FileStream input = File.OpenRead(path);
        return JsonSerializer.Deserialize<ReceiverLifecycleDocument>(input) ?? throw new InvalidDataException("Receiver lifecycle store is malformed.");
    }
    public void Save(ReceiverLifecycleDocument document)
    {
        string directory = Path.GetDirectoryName(path) ?? throw new InvalidOperationException("Receiver lifecycle store requires a directory.");
        Directory.CreateDirectory(directory); string temporary = path + ".tmp-" + Guid.NewGuid().ToString("N");
        try { using FileStream output = new(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None); JsonSerializer.Serialize(output, document); output.Flush(flushToDisk: true); File.Move(temporary, path, true); }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}

/// <summary>Small transactional authority for the ADR-005 durable records.
/// Secrets are returned only by Issue/Exchange and never appear in any record or audit.</summary>
public sealed class ReceiverLifecycleRegistry
{
    private const int AuditLimit = 256;
    private readonly object _gate = new(); private readonly IReceiverLifecycleStore _store; private readonly byte[] _pepper; private readonly int _pepperVersion; private readonly Func<DateTimeOffset> _clock;
    private readonly Dictionary<string, ManagedReceiver> _receivers; private readonly Dictionary<string, EnrolmentOfferRecord> _offers; private readonly Dictionary<string, AgentCredentialRecord> _credentials; private readonly List<ReceiverLifecycleAudit> _audit;
    public ReceiverLifecycleRegistry(IReceiverLifecycleStore store, byte[] pepper, int pepperVersion = 1, Func<DateTimeOffset>? clock = null)
    {
        if (pepper.Length < 32 || pepperVersion < 1) throw new ArgumentOutOfRangeException(nameof(pepper));
        _store = store; _pepper = pepper.ToArray(); _pepperVersion = pepperVersion; _clock = clock ?? (() => DateTimeOffset.UtcNow);
        ReceiverLifecycleDocument state = store.Load(); _receivers = state.Receivers.ToDictionary(value => value.ReceiverId, StringComparer.Ordinal); _offers = state.Offers.ToDictionary(value => value.EnrolmentId, StringComparer.Ordinal); _credentials = state.Credentials.ToDictionary(value => value.CredentialId, StringComparer.Ordinal); _audit = state.Audit.ToList();
    }
    public IReadOnlyList<ManagedReceiver> List() { lock (_gate) return _receivers.Values.Where(value => value.LifecycleState != ReceiverLifecycleState.Decommissioned).OrderBy(value => value.DisplayName, StringComparer.Ordinal).ThenBy(value => value.ReceiverId, StringComparer.Ordinal).ToArray(); }
    public ManagedReceiver Get(string receiverId) { lock (_gate) return Require(receiverId); }
    public (ManagedReceiver Receiver, string EnrolmentCredential) Add(string displayName)
    {
        lock (_gate)
        {
            DateTimeOffset now = _clock(); string id; do { id = "rxr_" + Base32(RandomNumberGenerator.GetBytes(16)); } while (_receivers.ContainsKey(id));
            ManagedReceiver receiver = new(id, ValidateName(displayName), ReceiverLifecycleState.PendingEnrolment, 1, 1, now, now); _receivers.Add(id, receiver); Audit(receiver, "receiver-added", 0, 1, "pending-enrolment");
            (EnrolmentOfferRecord _, string credential) = IssueLocked(receiver, "FirstEnrolment"); Persist(); return (receiver, credential);
        }
    }
    public string IssueReplacement(string receiverId)
    {
        lock (_gate) { ManagedReceiver receiver = Require(receiverId); if (receiver.LifecycleState is not (ReceiverLifecycleState.Active or ReceiverLifecycleState.Disabled)) throw new InvalidOperationException("Receiver cannot receive a replacement credential."); (EnrolmentOfferRecord _, string credential) = IssueLocked(receiver, "ReplaceCredential"); Persist(); return credential; }
    }
    public (string ReceiverId, string AgentCredential) Exchange(string enrolmentCredential)
    {
        lock (_gate)
        {
            DateTimeOffset now = _clock(); (string handle, byte[] actual) = Parse(enrolmentCredential, "t28e1"); if (!_offers.TryGetValue(handle, out EnrolmentOfferRecord? offer) || offer.Status != EnrolmentOfferStatus.Issued || offer.ExpiresAt <= now || offer.Verifier is null || !CryptographicOperations.FixedTimeEquals(Convert.FromHexString(offer.Verifier), Verify(enrolmentCredential))) { if (offer is not null && offer.Status == EnrolmentOfferStatus.Issued && offer.ExpiresAt <= now) _offers[handle] = offer with { Status = EnrolmentOfferStatus.Expired, Verifier = null }; Persist(); throw new UnauthorizedAccessException("Invalid enrolment credential."); }
            ManagedReceiver receiver = Require(offer.ReceiverId); if (receiver.LifecycleState is not (ReceiverLifecycleState.PendingEnrolment or ReceiverLifecycleState.Active or ReceiverLifecycleState.Disabled)) throw new UnauthorizedAccessException("Invalid enrolment credential.");
            string credential = NewCredential("t28a1", out string credentialId); AgentCredentialRecord pending = new(credentialId, receiver.ReceiverId, Convert.ToHexString(Verify(credential)), _pepperVersion, AgentCredentialStatus.PendingProof, checked(receiver.CredentialEpoch + 1), now, now.AddDays(180), now.AddMinutes(15)); _credentials.Add(credentialId, pending); _offers[handle] = offer with { Status = EnrolmentOfferStatus.Consumed, Verifier = null, ConsumedAt = now, PendingCredentialId = credentialId }; Audit(receiver, "enrolment-exchanged", receiver.ManagementRevision, receiver.ManagementRevision, "pending-proof"); Persist(); return (receiver.ReceiverId, credential);
        }
    }
    /// <summary>Validates a credential without changing its lifecycle.  A
    /// PendingProof credential may be used only for Hello and its first full
    /// snapshot; activation is deliberately a separate transaction.</summary>
    public AgentAuthentication Authenticate(string agentCredential, bool allowPendingProof = false)
    {
        lock (_gate)
        {
            DateTimeOffset now = _clock(); (string handle, byte[] _) = Parse(agentCredential, "t28a1");
            bool known = _credentials.TryGetValue(handle, out AgentCredentialRecord? credential);
            bool verifierMatches = known && credential!.Verifier is not null && CryptographicOperations.FixedTimeEquals(Convert.FromHexString(credential.Verifier), Verify(agentCredential));
            if (!verifierMatches) throw new UnauthorizedAccessException("Invalid agent credential.");
            AgentCredentialRecord matched = credential!; bool pendingExpired = matched.Status == AgentCredentialStatus.PendingProof && matched.ProofDeadline <= now;
            if (matched.Status is AgentCredentialStatus.Revoked or AgentCredentialStatus.Expired || matched.NotAfter <= now || pendingExpired)
            {
                if (pendingExpired) { _credentials[handle] = matched with { Status = AgentCredentialStatus.Expired, Verifier = null }; Persist(); }
                throw new UnauthorizedAccessException("Invalid agent credential.");
            }
            ManagedReceiver receiver = Require(matched.ReceiverId);
            if (receiver.LifecycleState is ReceiverLifecycleState.Decommissioned or ReceiverLifecycleState.Disabled || matched.Status == AgentCredentialStatus.PendingProof && !allowPendingProof) throw new UnauthorizedAccessException("Invalid agent credential.");
            _credentials[handle] = matched with { LastAuthenticatedAt = now };
            Persist(); return new(receiver.ReceiverId, matched.CredentialId, matched.Status, receiver.LifecycleState, matched.CredentialEpoch);
        }
    }
    /// <summary>One-time compatibility boundary for the owner-configured default
    /// receiver. The existing credential is verified in constant time and only
    /// its peppered verifier is persisted. Once an ADR-005 credential activates,
    /// the compatibility credential can no longer authenticate.</summary>
    public AgentAuthentication? AuthenticateConfiguredDefault(string receiverId, string displayName, string agentCredential, string? configuredReceiverId, string? configuredAgentCredential, bool allowActivation = false)
    {
        lock (_gate)
        {
            if (string.IsNullOrEmpty(configuredReceiverId) || string.IsNullOrEmpty(configuredAgentCredential) ||
                !string.Equals(receiverId, configuredReceiverId, StringComparison.Ordinal) ||
                !SecretEquals(agentCredential, configuredAgentCredential)) return null;

            DateTimeOffset now = _clock();
            if (!_receivers.TryGetValue(configuredReceiverId, out ManagedReceiver? receiver))
            {
                if (!allowActivation || _receivers.Count != 0 || _offers.Count != 0 || _credentials.Count != 0 || _audit.Count != 0 || !FleetIdentity.IsReceiverId(configuredReceiverId)) return null;
                string credentialId = "legacy-" + Base64(RandomNumberGenerator.GetBytes(16));
                receiver = new(configuredReceiverId, ValidateName(displayName), ReceiverLifecycleState.Active, 1, 1, now, now);
                AgentCredentialRecord credential = new(credentialId, configuredReceiverId, Convert.ToHexString(Verify(agentCredential)), _pepperVersion, AgentCredentialStatus.Active, 1, now, DateTimeOffset.MaxValue, now, now, now);
                ReceiverLifecycleAudit audit = new(Guid.NewGuid().ToString("N"), now, receiver.ReceiverId, "configured-default-activated", 0, receiver.ManagementRevision, "legacy-verifier-preserved");
                _store.Save(new(_receivers.Values.Append(receiver).OrderBy(value => value.ReceiverId, StringComparer.Ordinal).ToArray(), _offers.Values.OrderBy(value => value.EnrolmentId, StringComparer.Ordinal).ToArray(), _credentials.Values.Append(credential).OrderBy(value => value.CredentialId, StringComparer.Ordinal).ToArray(), _audit.Append(audit).ToArray()));
                _receivers.Add(receiver.ReceiverId, receiver); _credentials.Add(credentialId, credential); _audit.Add(audit);
                return new(receiver.ReceiverId, credential.CredentialId, credential.Status, receiver.LifecycleState, credential.CredentialEpoch);
            }

            byte[] verifier = Verify(agentCredential);
            AgentCredentialRecord? existing = _credentials.Values.SingleOrDefault(value => value.ReceiverId == receiver.ReceiverId && value.Status == AgentCredentialStatus.Active && value.Verifier is not null && CryptographicOperations.FixedTimeEquals(Convert.FromHexString(value.Verifier), verifier));
            if (existing is null || existing.NotAfter <= now ||
                receiver.LifecycleState is ReceiverLifecycleState.PendingEnrolment or ReceiverLifecycleState.Disabled or ReceiverLifecycleState.Decommissioned) return null;
            existing = existing with { LastAuthenticatedAt = now }; _credentials[existing.CredentialId] = existing; Persist();
            return new(receiver.ReceiverId, existing.CredentialId, existing.Status, receiver.LifecycleState, existing.CredentialEpoch);
        }
    }
    public ManagedReceiver ActivatePendingProof(string credentialId, string receiverId)
    {
        lock (_gate)
        {
            DateTimeOffset now = _clock(); AgentCredentialRecord credential = _credentials.GetValueOrDefault(credentialId) ?? throw new UnauthorizedAccessException("Invalid agent credential."); ManagedReceiver receiver = Require(receiverId);
            if (credential.ReceiverId != receiverId || credential.Status != AgentCredentialStatus.PendingProof || credential.ProofDeadline <= now || receiver.LifecycleState is not (ReceiverLifecycleState.PendingEnrolment or ReceiverLifecycleState.Active or ReceiverLifecycleState.Disabled)) throw new UnauthorizedAccessException("Invalid agent credential.");
            foreach (AgentCredentialRecord old in _credentials.Values.Where(value => value.ReceiverId == receiverId && value.CredentialId != credentialId && value.Status == AgentCredentialStatus.Active).ToArray()) _credentials[old.CredentialId] = old with { Status = AgentCredentialStatus.Revoked, Verifier = null, RevokedAt = now, RevokeReason = "replaced" };
            credential = credential with { Status = AgentCredentialStatus.Active, CredentialEpoch = checked(receiver.CredentialEpoch + 1), FirstAuthenticatedAt = now, LastAuthenticatedAt = now }; _credentials[credentialId] = credential;
            receiver = receiver with { LifecycleState = ReceiverLifecycleState.Active, CredentialEpoch = credential.CredentialEpoch, ManagementRevision = checked(receiver.ManagementRevision + 1), UpdatedAt = now }; _receivers[receiverId] = receiver; Audit(receiver, "agent-credential-activated", receiver.ManagementRevision - 1, receiver.ManagementRevision, "hello-and-full-snapshot"); Persist(); return receiver;
        }
    }
    public ManagedReceiver Rename(string receiverId, string displayName, long expectedRevision)
    {
        lock (_gate) { ManagedReceiver receiver = Require(receiverId); if (receiver.ManagementRevision != expectedRevision || receiver.LifecycleState == ReceiverLifecycleState.Decommissioned) throw new InvalidOperationException("Stale management revision."); DateTimeOffset now = _clock(); ManagedReceiver updated = receiver with { DisplayName = ValidateName(displayName), ManagementRevision = checked(receiver.ManagementRevision + 1), UpdatedAt = now }; _receivers[receiverId] = updated; Audit(updated, "receiver-renamed", receiver.ManagementRevision, updated.ManagementRevision, "renamed"); Persist(); return updated; }
    }
    public ManagedReceiver Disable(string receiverId, long expectedRevision)
        => Transition(receiverId, expectedRevision, ReceiverLifecycleState.DisablePendingStop, "receiver-disable-requested", "pending-stop", ReceiverLifecycleGoal.Disabled);
    public ManagedReceiver Enable(string receiverId, long expectedRevision)
        => Transition(receiverId, expectedRevision, ReceiverLifecycleState.Active, "receiver-enabled", "enabled");
    /// <summary>Marks a receiver for removal.  The caller must first retire its
    /// live session/audio, then call <see cref="CompleteDecommission"/>.</summary>
    public ManagedReceiver BeginDecommission(string receiverId, long expectedRevision)
        => Transition(receiverId, expectedRevision, ReceiverLifecycleState.DecommissionPendingStop, "decommission-requested", "pending-stop", ReceiverLifecycleGoal.Decommissioned);
    public ManagedReceiver RequireRecovery(string receiverId)
    {
        lock (_gate) { ManagedReceiver receiver = Require(receiverId); if (receiver.LifecycleState is not (ReceiverLifecycleState.DisablePendingStop or ReceiverLifecycleState.DecommissionPendingStop or ReceiverLifecycleState.RecoveryRequired)) throw new InvalidOperationException("Receiver does not require recovery."); DateTimeOffset now = _clock(); ManagedReceiver updated = receiver with { LifecycleState = ReceiverLifecycleState.RecoveryRequired, ManagementRevision = checked(receiver.ManagementRevision + 1), UpdatedAt = now }; _receivers[receiverId] = updated; Audit(updated, "receiver-recovery-required", receiver.ManagementRevision, updated.ManagementRevision, "awaiting-current-stop-proof"); Persist(); return updated; }
    }
    /// <summary>Called only after the current authenticated session supplied a
    /// full Stopped/child-absent snapshot.  Silence and disconnection are never proof.</summary>
    public ManagedReceiver ProveStopped(string receiverId)
    {
        lock (_gate) { ManagedReceiver receiver = Require(receiverId); if (receiver.LifecycleState is not (ReceiverLifecycleState.DisablePendingStop or ReceiverLifecycleState.DecommissionPendingStop or ReceiverLifecycleState.RecoveryRequired) || receiver.PendingGoal is null) throw new InvalidOperationException("Receiver is not awaiting stop proof."); DateTimeOffset now = _clock(); ReceiverLifecycleState state = receiver.PendingGoal == ReceiverLifecycleGoal.Decommissioned ? ReceiverLifecycleState.Decommissioned : ReceiverLifecycleState.Disabled; ManagedReceiver updated = receiver with { LifecycleState = state, ManagementRevision = checked(receiver.ManagementRevision + 1), CredentialEpoch = checked(receiver.CredentialEpoch + 1), UpdatedAt = now, PendingGoal = null, RestoreUntil = state == ReceiverLifecycleState.Decommissioned ? now.AddDays(30) : null, TombstoneExpiresAt = state == ReceiverLifecycleState.Decommissioned ? now.AddDays(365) : null }; if (state == ReceiverLifecycleState.Decommissioned) foreach (AgentCredentialRecord credential in _credentials.Values.Where(value => value.ReceiverId == receiverId && value.Status is AgentCredentialStatus.Active or AgentCredentialStatus.PendingProof).ToArray()) _credentials[credential.CredentialId] = credential with { Status = AgentCredentialStatus.Revoked, Verifier = null, RevokedAt = now, RevokeReason = "decommissioned-after-stop-proof" }; _receivers[receiverId] = updated; Audit(updated, state == ReceiverLifecycleState.Decommissioned ? "receiver-decommissioned" : "receiver-disabled", receiver.ManagementRevision, updated.ManagementRevision, "current-stop-proof"); Persist(); return updated; }
    }
    private ManagedReceiver Transition(string receiverId, long expectedRevision, ReceiverLifecycleState state, string audit, string outcome, ReceiverLifecycleGoal? goal = null)
    {
        lock (_gate)
        {
            ManagedReceiver receiver = Require(receiverId);
            if (receiver.ManagementRevision != expectedRevision || receiver.LifecycleState == ReceiverLifecycleState.Decommissioned) throw new InvalidOperationException("Stale management revision.");
            DateTimeOffset now = _clock();
            if (state == ReceiverLifecycleState.Active && receiver.LifecycleState != ReceiverLifecycleState.Disabled) throw new InvalidOperationException("Receiver cannot be enabled.");
            ManagedReceiver updated = receiver with { LifecycleState = state, ManagementRevision = checked(receiver.ManagementRevision + 1), UpdatedAt = now, PendingGoal = goal };
            _receivers[receiverId] = updated; Audit(updated, audit, receiver.ManagementRevision, updated.ManagementRevision, outcome); Persist(); return updated;
        }
    }
    private (EnrolmentOfferRecord Offer, string Credential) IssueLocked(ManagedReceiver receiver, string purpose)
    {
        DateTimeOffset now = _clock(); foreach (EnrolmentOfferRecord active in _offers.Values.Where(value => value.ReceiverId == receiver.ReceiverId && value.Purpose == purpose && value.Status == EnrolmentOfferStatus.Issued).ToArray()) _offers[active.EnrolmentId] = active with { Status = EnrolmentOfferStatus.Cancelled, Verifier = null, CancelledAt = now };
        string credential = NewCredential("t28e1", out string id); EnrolmentOfferRecord offer = new(id, receiver.ReceiverId, purpose, Convert.ToHexString(Verify(credential)), _pepperVersion, EnrolmentOfferStatus.Issued, now, now.AddMinutes(10)); _offers.Add(id, offer); Audit(receiver, "enrolment-issued", receiver.ManagementRevision, receiver.ManagementRevision, purpose); return (offer, credential);
    }
    private ManagedReceiver Require(string id) => _receivers.TryGetValue(id, out ManagedReceiver? value) ? value : throw new KeyNotFoundException("Unknown receiver.");
    private void Audit(ManagedReceiver receiver, string type, long before, long after, string outcome) { _audit.Add(new(Guid.NewGuid().ToString("N"), _clock(), receiver.ReceiverId, type, before, after, outcome)); while (_audit.Count(value => value.ReceiverId == receiver.ReceiverId) > AuditLimit) _audit.Remove(_audit.First(value => value.ReceiverId == receiver.ReceiverId)); }
    private void Persist() => _store.Save(new(_receivers.Values.OrderBy(value => value.ReceiverId, StringComparer.Ordinal).ToArray(), _offers.Values.OrderBy(value => value.EnrolmentId, StringComparer.Ordinal).ToArray(), _credentials.Values.OrderBy(value => value.CredentialId, StringComparer.Ordinal).ToArray(), _audit.ToArray()));
    private byte[] Verify(string credential) => HMACSHA256.HashData(_pepper, Encoding.ASCII.GetBytes(credential));
    private static bool SecretEquals(string actual, string expected) => CryptographicOperations.FixedTimeEquals(SHA256.HashData(Encoding.UTF8.GetBytes(actual)), SHA256.HashData(Encoding.UTF8.GetBytes(expected)));
    private static string ValidateName(string value) { string normalized = value.Trim().Normalize(NormalizationForm.FormC); if (normalized.Length is < 1 or > 64 || normalized.Any(char.IsControl) || normalized.Any(value => value is '\u202A' or '\u202B' or '\u202D' or '\u202E' or '\u2066' or '\u2067' or '\u2068' or '\u2069')) throw new InvalidDataException("Invalid receiver display name."); return normalized; }
    private static string NewCredential(string prefix, out string handle) { handle = Base64(RandomNumberGenerator.GetBytes(16)); return prefix + "." + handle + "." + Base64(RandomNumberGenerator.GetBytes(32)); }
    private static (string Handle, byte[] Secret) Parse(string value, string prefix) { string[] parts = value.Split('.'); if (parts.Length != 3 || parts[0] != prefix || parts[1] != Canonical(parts[1], 16) || parts[2] != Canonical(parts[2], 32)) throw new UnauthorizedAccessException("Invalid credential."); return (parts[1], Convert.FromBase64String(parts[2].Replace('-', '+').Replace('_', '/') + ((parts[2].Length % 4) switch { 2 => "==", 3 => "=", _ => "" }))); }
    private static string Canonical(string value, int expectedBytes) { try { byte[] bytes = Convert.FromBase64String(value.Replace('-', '+').Replace('_', '/') + ((value.Length % 4) switch { 2 => "==", 3 => "=", _ => "" })); return bytes.Length == expectedBytes ? Base64(bytes) : ""; } catch (FormatException) { return ""; } }
    private static string Base64(byte[] value) => Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    private static string Base32(byte[] value) { const string alphabet = "abcdefghijklmnopqrstuvwxyz234567"; StringBuilder result = new(); int buffer = 0, bits = 0; foreach (byte item in value) { buffer = (buffer << 8) | item; bits += 8; while (bits >= 5) { result.Append(alphabet[(buffer >> (bits -= 5)) & 31]); } } if (bits > 0) result.Append(alphabet[(buffer << (5 - bits)) & 31]); return result.ToString(); }
}
