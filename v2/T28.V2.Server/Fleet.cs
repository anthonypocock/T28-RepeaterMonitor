using System.Security.Cryptography;
using System.Text.Json;
using T28.V2.Contracts;

namespace T28.V2.Server;

public static class FleetIdentity
{
    public static bool IsReceiverId(string? value) => value is { Length: >= 8 and <= 64 } && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-');
    public static bool IsBootId(string? value) => value is { Length: >= 8 and <= 128 } && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-');
    public static string NewAudioGeneration() => Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant();
}
public enum FleetConnectivity { Online, Stale, Offline }
public enum FleetSessionStatus { Current, Unknown, Replaced }
public enum ReceiverOperatingState { Stopped, Starting, Receiving, Stopping, Faulted }
public enum FleetRole { Listener, Admin }
public enum FleetCommandAction { StartReceiver, StopReceiver, ApplyConfiguration, SetChannel, SetSquelch }
public enum FleetCommandOutcome { Accepted, Succeeded, RejectedUnauthorized, RejectedOffline, RejectedStaleSession, RejectedStaleState, RequestConflict, OutcomeUnknown }
public sealed record FleetSafeProjection(ReceiverOperatingState State, long StateVersion, string? AudioGeneration, string AppliedProfileId, long AppliedRevision, string Hardware, string? FaultCode, short ThresholdTenths = -1000, short CtcssTenths = 1622, IReadOnlyList<ApprovedChannelProjection>? Channels = null, bool OwnedChildAbsent = false)
{ public static FleetSafeProjection Stopped(string profileId, long revision = 0) => new(ReceiverOperatingState.Stopped, 1, null, profileId, revision, "unknown", null, OwnedChildAbsent: true); }
public sealed record FleetReceiverRecord(string ReceiverId, string DisplayName, bool Enabled, long SessionEpoch, string DesiredProfileId, long DesiredRevision, FleetSafeProjection LastAcknowledged, DateTimeOffset? LastAuthenticatedContact, string[] Audit, bool? DesiredRunning = null)
{
    /// <summary>Null means legacy fleet rows that never recorded an explicit Stop.</summary>
    public bool WantsRunning => DesiredRunning ?? true;
}
public sealed record FleetSession(string ReceiverId, string BootId, string ConnectionId, long Epoch);
public sealed record FleetSnapshot(string ReceiverId, string DisplayName, FleetConnectivity Connectivity, DateTimeOffset? LastAuthenticatedContact, FleetSafeProjection Projection, string DesiredProfileId, long DesiredRevision, bool ConfigurationPending, string? CurrentBootId, long SessionEpoch, bool ProjectionCurrent = false, bool DesiredRunning = true);
public sealed record FleetCommand(Guid RequestId, string ReceiverId, long SessionEpoch, string BootId, long ExpectedStateVersion, FleetCommandAction Action, string? ProfileId = null, long? DesiredRevision = null, short? ThresholdTenths = null, short? CtcssTenths = null);
public sealed record FleetCommandResult(Guid RequestId, FleetCommandOutcome Outcome, FleetSafeProjection Projection, string? Reason = null);
public interface IFleetStore { IReadOnlyList<FleetReceiverRecord> Load(); void Save(IReadOnlyList<FleetReceiverRecord> records); }
public sealed class JsonFleetStore(string path) : IFleetStore
{
    private readonly string _path = path;
    public IReadOnlyList<FleetReceiverRecord> Load() { if (!File.Exists(_path)) return []; using FileStream input = File.OpenRead(_path); return JsonSerializer.Deserialize<FleetReceiverRecord[]>(input) ?? throw new InvalidDataException("Fleet store is malformed."); }
    public void Save(IReadOnlyList<FleetReceiverRecord> records)
    {
        string directory = Path.GetDirectoryName(_path) ?? throw new InvalidOperationException("Fleet store requires a directory."); Directory.CreateDirectory(directory); string temporary = _path + ".tmp-" + Guid.NewGuid().ToString("N");
        try { using (FileStream output = new(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None)) { JsonSerializer.Serialize(output, records); output.Flush(flushToDisk: true); } File.Move(temporary, _path, overwrite: true); } finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}
public sealed class FleetRegistry
{
    private static readonly TimeSpan StaleAfter = TimeSpan.FromSeconds(15), OfflineAfter = TimeSpan.FromSeconds(45), AutoStartInitialBackoff = TimeSpan.FromSeconds(2), AutoStartMaxBackoff = TimeSpan.FromSeconds(30); private const int AuditLimit = 32, RequestHistoryLimit = 128;
    private readonly IFleetStore _store; private readonly Func<DateTimeOffset> _clock; private readonly object _gate = new(); private readonly Dictionary<string, FleetReceiverRecord> _records; private readonly Dictionary<string, FleetSession> _sessions = []; private readonly Dictionary<string, long> _sessionVersions = []; private readonly HashSet<string> _currentProjections = []; private readonly Dictionary<string, Dictionary<Guid, FleetCommand>> _requests = []; private readonly Dictionary<string, Dictionary<Guid, FleetCommandResult>> _results = []; private readonly Dictionary<(string ReceiverId, Guid RequestId, long Epoch, string BootId, long Version), TaskCompletionSource<FleetCommandResult>> _waiters = []; private readonly Dictionary<string, DateTimeOffset> _autoStartSuppressedUntil = []; private readonly Dictionary<string, TimeSpan> _autoStartBackoff = [];
    public FleetRegistry(IFleetStore store, Func<DateTimeOffset>? clock = null)
    { _store = store; _clock = clock ?? (() => DateTimeOffset.UtcNow); _records = store.Load().ToDictionary(record => record.ReceiverId, StringComparer.Ordinal); if (_records.Count == 0) { FleetReceiverRecord legacy = new("lab-demo-hill", "Demo Hill lab receiver", true, 0, "lab-demo-hill", 0, FleetSafeProjection.Stopped("lab-demo-hill"), null, []); _records.Add(legacy.ReceiverId, legacy); PersistLocked(); } }
    public IReadOnlyList<FleetSnapshot> List() { lock (_gate) return _records.Keys.Order(StringComparer.Ordinal).Select(SnapshotLocked).ToArray(); }
    public FleetSnapshot Snapshot(string receiverId) { lock (_gate) return SnapshotLocked(RequireLocked(receiverId)); }
    public FleetSnapshot Register(string receiverId, string displayName)
    {
        if (!FleetIdentity.IsReceiverId(receiverId)) throw new InvalidDataException("Invalid receiver identity.");
        lock (_gate) { if (_records.ContainsKey(receiverId)) return SnapshotLocked(receiverId); FleetReceiverRecord record = new(receiverId, displayName, true, 0, "lab-demo-hill", 0, FleetSafeProjection.Stopped("lab-demo-hill"), null, []); _records.Add(receiverId, record); PersistLocked(); return SnapshotLocked(record); }
    }
    public FleetSnapshot Rename(string receiverId, string displayName)
    {
        lock (_gate) { FleetReceiverRecord record = RequireLocked(receiverId); ReplaceLocked(record with { DisplayName = displayName }, "display-name-updated"); return SnapshotLocked(receiverId); }
    }
    public void Retire(string receiverId)
    {
        lock (_gate) { FleetReceiverRecord record = RequireLocked(receiverId); _sessions.Remove(receiverId); _sessionVersions.Remove(receiverId); _currentProjections.Remove(receiverId); _requests.Remove(receiverId); _results.Remove(receiverId); ReplaceLocked(record with { Enabled = false }, "retired"); }
    }
    public void EnableManagedReceiver(string receiverId)
    {
        lock (_gate) { FleetReceiverRecord record = RequireLocked(receiverId); ReplaceLocked(record with { Enabled = true }, "lifecycle-enabled"); }
    }
    /// <summary>Lifecycle recovery retains only an already-safe Stop command.
    /// Other requests must not execute after listener retirement.</summary>
    public void CancelNonStop(string receiverId)
    {
        lock (_gate)
        {
            if (!_requests.TryGetValue(receiverId, out Dictionary<Guid, FleetCommand>? requests)) return;
            foreach (FleetCommand command in requests.Values.Where(value => value.Action != FleetCommandAction.StopReceiver).ToArray())
            {
                requests.Remove(command.RequestId);
                if (_results.TryGetValue(receiverId, out Dictionary<Guid, FleetCommandResult>? results)) results[command.RequestId] = new(command.RequestId, FleetCommandOutcome.OutcomeUnknown, RequireLocked(receiverId).LastAcknowledged, "lifecycle-retired");
            }
        }
    }
    /// <summary>Removes the portal projection only after the lifecycle authority
    /// has persisted its bounded decommission tombstone.  Runtime sessions and
    /// audio are already retired by <see cref="Retire"/>.</summary>
    public void RemoveManagedReceiver(string receiverId)
    {
        lock (_gate)
        {
            RequireLocked(receiverId); _sessions.Remove(receiverId); _sessionVersions.Remove(receiverId); _currentProjections.Remove(receiverId); _requests.Remove(receiverId); _results.Remove(receiverId);
            foreach (var waiter in _waiters.Where(pair => pair.Key.ReceiverId == receiverId).ToArray()) { _waiters.Remove(waiter.Key); waiter.Value.TrySetCanceled(); }
            _records.Remove(receiverId); PersistLocked();
        }
    }
    public FleetSession AcceptHello(string receiverId, string bootId, string connectionId)
    {
        if (!FleetIdentity.IsReceiverId(receiverId) || !FleetIdentity.IsBootId(bootId) || !FleetIdentity.IsBootId(connectionId)) throw new InvalidDataException("Invalid receiver session identity."); lock (_gate) { FleetReceiverRecord record = RequireLocked(receiverId); if (!record.Enabled) throw new InvalidOperationException("Receiver is disabled."); foreach (var waiter in _waiters.Where(pair => pair.Key.ReceiverId == receiverId).ToArray()) { _waiters.Remove(waiter.Key); waiter.Value.TrySetCanceled(); } FleetSession next = new(receiverId, bootId, connectionId, checked(record.SessionEpoch + 1)); _sessions[receiverId] = next; _sessionVersions[receiverId] = 0; _currentProjections.Remove(receiverId); _requests.Remove(receiverId); _results.Remove(receiverId); ReplaceLocked(record with { SessionEpoch = next.Epoch, LastAuthenticatedContact = _clock() }, "session-accepted"); return next; }
    }
    public bool ReceiveHeartbeat(FleetSession session) { lock (_gate) return CurrentLocked(session, true); }
    public FleetSessionStatus SessionStatus(FleetSession session) { lock (_gate) return SessionStatusLocked(session); }
    public bool ReceiveSnapshot(FleetSession session, FleetSafeProjection projection) { ValidateProjection(projection); lock (_gate) { if (!CurrentLocked(session, true) || projection.StateVersion < _sessionVersions.GetValueOrDefault(session.ReceiverId)) return false; FleetReceiverRecord record = RequireLocked(session.ReceiverId); _sessionVersions[session.ReceiverId] = projection.StateVersion; _currentProjections.Add(session.ReceiverId); ReplaceLocked(record with { LastAcknowledged = projection }, "snapshot"); return true; } }
    /// <summary>Audio is accepted only from the current authenticated session
    /// and only for its currently acknowledged receive generation.</summary>
    public bool AcceptAudio(FleetSession session, string generation)
    {
        lock (_gate)
        {
            // Audio proves neither liveness nor a heartbeat. It is accepted only
            // after this exact session supplied a current safe projection.
            if (!CurrentLocked(session, false) || !_currentProjections.Contains(session.ReceiverId)) return false;
            FleetReceiverRecord record = RequireLocked(session.ReceiverId);
            return record.LastAcknowledged.State == ReceiverOperatingState.Receiving &&
                record.LastAcknowledged.AudioGeneration == generation;
        }
    }
    public FleetSnapshot SaveDesired(FleetRole role, string receiverId, string profileId) { if (role != FleetRole.Admin || !FleetIdentity.IsReceiverId(receiverId) || string.IsNullOrWhiteSpace(profileId) || profileId.Length > 64) throw new UnauthorizedAccessException("Admin role and approved profile identity are required."); lock (_gate) { FleetReceiverRecord record = RequireLocked(receiverId); if (record.DesiredProfileId == profileId) return SnapshotLocked(record); ReplaceLocked(record with { DesiredProfileId = profileId, DesiredRevision = checked(record.DesiredRevision + 1) }, "desired-config-saved"); return SnapshotLocked(receiverId); } }
    public FleetCommandResult Admit(FleetRole role, FleetCommand command) { lock (_gate) return AdmitLocked(role, command); }
    private FleetCommandResult AdmitLocked(FleetRole role, FleetCommand command)
    {
        FleetReceiverRecord record = RequireLocked(command.ReceiverId); if (role != FleetRole.Admin) return new(command.RequestId, FleetCommandOutcome.RejectedUnauthorized, record.LastAcknowledged, "listener-role"); if (command.RequestId == Guid.Empty) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "invalid-request"); if (command.Action == FleetCommandAction.SetChannel && (command.ProfileId is null || record.LastAcknowledged.Channels?.Any(channel => channel.ProfileId == command.ProfileId) != true)) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "unsupported-profile"); if (command.Action == FleetCommandAction.SetSquelch && (command.ThresholdTenths is < -1000 or > 0 || command.CtcssTenths is null || !CtcssTone.IsValidTenths(command.CtcssTenths.Value))) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "unsupported-control"); if (command.Action == FleetCommandAction.StartReceiver && (record.DesiredRevision != record.LastAcknowledged.AppliedRevision || record.DesiredProfileId != record.LastAcknowledged.AppliedProfileId)) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "configuration-pending"); if (command.Action == FleetCommandAction.ApplyConfiguration && record.LastAcknowledged.State != ReceiverOperatingState.Stopped) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "receiver-not-stopped"); Dictionary<Guid, FleetCommand> requests = _requests.GetValueOrDefault(command.ReceiverId) ?? (_requests[command.ReceiverId] = []); Dictionary<Guid, FleetCommandResult> results = _results.GetValueOrDefault(command.ReceiverId) ?? (_results[command.ReceiverId] = []); if (requests.TryGetValue(command.RequestId, out FleetCommand? prior)) return prior == command ? results.GetValueOrDefault(command.RequestId) ?? new(command.RequestId, FleetCommandOutcome.Accepted, record.LastAcknowledged) : new(command.RequestId, FleetCommandOutcome.RequestConflict, record.LastAcknowledged, "request-conflict"); if (results.TryGetValue(command.RequestId, out FleetCommandResult? expired)) return expired; if (SnapshotLocked(record).Connectivity != FleetConnectivity.Online || !_sessions.TryGetValue(command.ReceiverId, out FleetSession? current) || !_currentProjections.Contains(command.ReceiverId)) return new(command.RequestId, FleetCommandOutcome.RejectedOffline, record.LastAcknowledged, "session-not-synchronized"); if (current.Epoch != command.SessionEpoch || current.BootId != command.BootId) return new(command.RequestId, FleetCommandOutcome.RejectedStaleSession, record.LastAcknowledged, "stale-session"); if (record.LastAcknowledged.StateVersion != command.ExpectedStateVersion) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "stale-state"); if (command.Action == FleetCommandAction.ApplyConfiguration && (command.DesiredRevision != record.DesiredRevision || command.ProfileId != record.DesiredProfileId)) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "stale-desired-revision"); if (command.Action == FleetCommandAction.StartReceiver && (command.DesiredRevision != record.DesiredRevision || command.ProfileId != record.DesiredProfileId || record.LastAcknowledged.State is not (ReceiverOperatingState.Stopped or ReceiverOperatingState.Faulted) || !record.LastAcknowledged.OwnedChildAbsent)) return new(command.RequestId, FleetCommandOutcome.RejectedStaleState, record.LastAcknowledged, "stale-start-request");         if (requests.Count >= RequestHistoryLimit) return new(command.RequestId, FleetCommandOutcome.RequestConflict, record.LastAcknowledged, "request-history-full"); requests.Add(command.RequestId, command); FleetCommandResult accepted = new(command.RequestId, FleetCommandOutcome.Accepted, record.LastAcknowledged); results.Add(command.RequestId, accepted); return accepted;
    }
    public bool Complete(FleetSession session, Guid requestId, FleetCommandOutcome outcome, FleetSafeProjection projection)
    {
        ValidateProjection(projection);
        lock (_gate)
        {
            if (!CurrentLocked(session, true) || !_currentProjections.Contains(session.ReceiverId) || !_requests.TryGetValue(session.ReceiverId, out Dictionary<Guid, FleetCommand>? requests) || !requests.TryGetValue(requestId, out FleetCommand? command) || (outcome == FleetCommandOutcome.Succeeded && (!ExactCompletion(command, projection) || projection.StateVersion <= command.ExpectedStateVersion)) || projection.StateVersion < _sessionVersions.GetValueOrDefault(session.ReceiverId)) return false;
            FleetReceiverRecord record = RequireLocked(session.ReceiverId); _sessionVersions[session.ReceiverId] = projection.StateVersion;
            bool succeeded = outcome == FleetCommandOutcome.Succeeded;
            bool? desiredRunning = command.Action switch { FleetCommandAction.StartReceiver when succeeded => true, FleetCommandAction.StopReceiver when succeeded => false, _ => null };
            record = record with { LastAcknowledged = projection, DesiredRunning = desiredRunning ?? record.DesiredRunning };
            ReplaceLocked(record, succeeded ? "command-succeeded" : "command-failed");
            if (command.Action == FleetCommandAction.StartReceiver) { if (succeeded) { _autoStartSuppressedUntil.Remove(session.ReceiverId); _autoStartBackoff.Remove(session.ReceiverId); } else if (record.WantsRunning) NoteAutoStartBackoffLocked(session.ReceiverId); }
            FleetCommandResult result = new(requestId, outcome, projection); _results[session.ReceiverId][requestId] = result;
            if (_waiters.Remove((session.ReceiverId, requestId, command.SessionEpoch, command.BootId, command.ExpectedStateVersion), out TaskCompletionSource<FleetCommandResult>? waiter)) waiter.TrySetResult(result);
            ReconcileRunningIntentLocked(session); return true;
        }
    }
    /// <summary>When the durable intent is Receiving, enqueue Apply/Start for the
    /// current session without an admin click.  Stop clears the intent until Start.</summary>
    public void ReconcileRunningIntent(FleetSession session) { lock (_gate) ReconcileRunningIntentLocked(session); }
    public async Task<FleetCommandResult?> WaitForCompletionAsync(FleetCommand command, TimeSpan timeout, CancellationToken token)
    {
        TaskCompletionSource<FleetCommandResult> waiter = new(TaskCreationOptions.RunContinuationsAsynchronously); var key = (command.ReceiverId, command.RequestId, command.SessionEpoch, command.BootId, command.ExpectedStateVersion);
        lock (_gate) { if (_results.TryGetValue(command.ReceiverId, out Dictionary<Guid, FleetCommandResult>? results) && results.TryGetValue(command.RequestId, out FleetCommandResult? known) && known.Outcome != FleetCommandOutcome.Accepted) return known; if (!_requests.TryGetValue(command.ReceiverId, out Dictionary<Guid, FleetCommand>? requests) || !requests.TryGetValue(command.RequestId, out FleetCommand? admitted) || admitted != command || !_waiters.TryAdd(key, waiter)) return null; }
        try { using CancellationTokenSource expiry = CancellationTokenSource.CreateLinkedTokenSource(token); expiry.CancelAfter(timeout); return await waiter.Task.WaitAsync(expiry.Token); }
        catch (OperationCanceledException)
        {
            // A completion wait is the command's delivery lease.  Once it
            // expires (or its HTTP caller goes away), remove the admitted
            // command as well as its waiter so it cannot execute after a
            // delayed poll, acknowledgement, or reconnect.
            lock (_gate)
            {
                _waiters.Remove(key);
                if (_requests.TryGetValue(command.ReceiverId, out Dictionary<Guid, FleetCommand>? requests) && requests.TryGetValue(command.RequestId, out FleetCommand? admitted) && admitted == command && _results.TryGetValue(command.ReceiverId, out Dictionary<Guid, FleetCommandResult>? results))
                {
                    requests.Remove(command.RequestId);
                    results[command.RequestId] = new(command.RequestId, FleetCommandOutcome.OutcomeUnknown, RequireLocked(command.ReceiverId).LastAcknowledged, "completion-timeout");
                }
            }
            return null;
        }
        finally { lock (_gate) _waiters.Remove(key); }
    }
    /// <summary>Returns only commands admitted for this exact live session.  The
    /// collection is deliberately memory-only, so a reconnect cannot replay a
    /// Start or Stop that was accepted for an older fence.</summary>
    public IReadOnlyList<FleetCommand> Pending(FleetSession session) { lock (_gate) { if (!CurrentLocked(session, true) || !_requests.TryGetValue(session.ReceiverId, out Dictionary<Guid, FleetCommand>? requests)) return []; return requests.Values.Where(command => !_results[session.ReceiverId].TryGetValue(command.RequestId, out FleetCommandResult? result) || result.Outcome == FleetCommandOutcome.Accepted).ToArray(); } }
    public void DefinitiveClose(FleetSession session) { lock (_gate) { if (CurrentLocked(session, false)) { _sessions.Remove(session.ReceiverId); _sessionVersions.Remove(session.ReceiverId); _currentProjections.Remove(session.ReceiverId); _requests.Remove(session.ReceiverId); _results.Remove(session.ReceiverId); foreach (var waiter in _waiters.Where(pair => pair.Key.ReceiverId == session.ReceiverId).ToArray()) { _waiters.Remove(waiter.Key); waiter.Value.TrySetCanceled(); } } } }
    public void DisconnectForTesting(string receiverId)
    {
        lock (_gate)
        {
            _sessions.Remove(receiverId); _sessionVersions.Remove(receiverId); _currentProjections.Remove(receiverId); _requests.Remove(receiverId); _results.Remove(receiverId);
            foreach (var waiter in _waiters.Where(pair => pair.Key.ReceiverId == receiverId).ToArray()) { _waiters.Remove(waiter.Key); waiter.Value.TrySetCanceled(); }
        }
    }
    private bool CurrentLocked(FleetSession session, bool touch) { if (!_sessions.TryGetValue(session.ReceiverId, out FleetSession? current) || current != session) return false; if (touch) { FleetReceiverRecord record = RequireLocked(session.ReceiverId); ReplaceLocked(record with { LastAuthenticatedContact = _clock() }, null); } return true; }
    private FleetSessionStatus SessionStatusLocked(FleetSession session) => !_sessions.TryGetValue(session.ReceiverId, out FleetSession? current) ? FleetSessionStatus.Unknown : current == session ? FleetSessionStatus.Current : FleetSessionStatus.Replaced;
    private FleetSnapshot SnapshotLocked(string receiverId) => SnapshotLocked(RequireLocked(receiverId));
    private FleetSnapshot SnapshotLocked(FleetReceiverRecord record) { DateTimeOffset? contact = record.LastAuthenticatedContact; FleetConnectivity connectivity = !_sessions.ContainsKey(record.ReceiverId) || contact is null ? FleetConnectivity.Offline : _clock() - contact <= StaleAfter ? FleetConnectivity.Online : _clock() - contact < OfflineAfter ? FleetConnectivity.Stale : FleetConnectivity.Offline; FleetSession? session = _sessions.GetValueOrDefault(record.ReceiverId); return new(record.ReceiverId, record.DisplayName, connectivity, contact, record.LastAcknowledged, record.DesiredProfileId, record.DesiredRevision, record.DesiredRevision != record.LastAcknowledged.AppliedRevision || record.DesiredProfileId != record.LastAcknowledged.AppliedProfileId, session?.BootId, session?.Epoch ?? record.SessionEpoch, session is not null && _currentProjections.Contains(record.ReceiverId), record.WantsRunning); }
    private FleetReceiverRecord RequireLocked(string id) => _records.TryGetValue(id, out FleetReceiverRecord? record) ? record : throw new KeyNotFoundException("Unknown receiver.");
    private void ReplaceLocked(FleetReceiverRecord record, string? audit) { if (audit is not null) record = record with { Audit = record.Audit.Append(audit).TakeLast(AuditLimit).ToArray() }; _records[record.ReceiverId] = record; PersistLocked(); }
    private void PersistLocked() => _store.Save(_records.Values.OrderBy(record => record.ReceiverId, StringComparer.Ordinal).ToArray());
    private void ReconcileRunningIntentLocked(FleetSession session)
    {
        if (!CurrentLocked(session, false) || !_currentProjections.Contains(session.ReceiverId)) return;
        FleetReceiverRecord record = RequireLocked(session.ReceiverId);
        if (!record.Enabled || !record.WantsRunning) return;
        if (_autoStartSuppressedUntil.TryGetValue(session.ReceiverId, out DateTimeOffset suppressed) && _clock() < suppressed) return;
        if (record.LastAcknowledged.State is ReceiverOperatingState.Starting or ReceiverOperatingState.Receiving or ReceiverOperatingState.Stopping) return;
        if (HasPendingActionLocked(session.ReceiverId, FleetCommandAction.StartReceiver) || HasPendingActionLocked(session.ReceiverId, FleetCommandAction.ApplyConfiguration)) return;
        FleetSession current = _sessions[session.ReceiverId];
        if (record.DesiredRevision != record.LastAcknowledged.AppliedRevision || record.DesiredProfileId != record.LastAcknowledged.AppliedProfileId)
        {
            FleetCommand apply = new(Guid.NewGuid(), session.ReceiverId, current.Epoch, current.BootId, record.LastAcknowledged.StateVersion, FleetCommandAction.ApplyConfiguration, record.DesiredProfileId, record.DesiredRevision);
            if (AdmitLocked(FleetRole.Admin, apply).Outcome == FleetCommandOutcome.Accepted) ReplaceLocked(RequireLocked(session.ReceiverId), "auto-apply-enqueued");
            return;
        }
        if (record.LastAcknowledged.State is ReceiverOperatingState.Stopped or ReceiverOperatingState.Faulted)
        {
            FleetCommand start = new(Guid.NewGuid(), session.ReceiverId, current.Epoch, current.BootId, record.LastAcknowledged.StateVersion, FleetCommandAction.StartReceiver, record.DesiredProfileId, record.DesiredRevision);
            if (AdmitLocked(FleetRole.Admin, start).Outcome == FleetCommandOutcome.Accepted) ReplaceLocked(RequireLocked(session.ReceiverId), "auto-start-enqueued");
        }
    }
    private void NoteAutoStartBackoffLocked(string receiverId)
    {
        TimeSpan next = _autoStartBackoff.TryGetValue(receiverId, out TimeSpan prior) ? TimeSpan.FromMilliseconds(Math.Min(AutoStartMaxBackoff.TotalMilliseconds, prior.TotalMilliseconds * 2)) : AutoStartInitialBackoff;
        _autoStartBackoff[receiverId] = next; _autoStartSuppressedUntil[receiverId] = _clock() + next;
    }
    private bool HasPendingActionLocked(string receiverId, FleetCommandAction action) => _requests.TryGetValue(receiverId, out Dictionary<Guid, FleetCommand>? requests) && requests.Values.Any(command => command.Action == action && (!_results[receiverId].TryGetValue(command.RequestId, out FleetCommandResult? result) || result.Outcome == FleetCommandOutcome.Accepted));
    private static bool ExactCompletion(FleetCommand command, FleetSafeProjection projection) => command.Action switch { FleetCommandAction.ApplyConfiguration => projection.State == ReceiverOperatingState.Stopped && projection.AudioGeneration is null && projection.OwnedChildAbsent && projection.AppliedProfileId == command.ProfileId && projection.AppliedRevision == command.DesiredRevision, FleetCommandAction.StartReceiver => projection.State == ReceiverOperatingState.Receiving && projection.AudioGeneration is not null && projection.AppliedProfileId == command.ProfileId && projection.AppliedRevision == command.DesiredRevision, FleetCommandAction.StopReceiver => projection.State == ReceiverOperatingState.Stopped && projection.AudioGeneration is null && projection.OwnedChildAbsent, FleetCommandAction.SetChannel => projection.AppliedProfileId == command.ProfileId, FleetCommandAction.SetSquelch => projection.ThresholdTenths == command.ThresholdTenths && projection.CtcssTenths == command.CtcssTenths, _ => false };
    private static void ValidateProjection(FleetSafeProjection projection) { if (projection.StateVersion < 1 || string.IsNullOrWhiteSpace(projection.AppliedProfileId) || projection.ThresholdTenths is < -1000 or > 0 || !CtcssTone.IsValidTenths(projection.CtcssTenths) || projection.Channels is { Count: > 0 } channels && channels.Any(channel => channel.ProfileId == projection.AppliedProfileId) == false || (projection.State == ReceiverOperatingState.Receiving) != (projection.AudioGeneration is not null) || (projection.AudioGeneration is not null && projection.AudioGeneration.Length != 32)) throw new InvalidDataException("Invalid safe receiver projection."); }
}
public sealed class FleetAudioHub
{
    private readonly object _gate = new(); private readonly Dictionary<string, (long Epoch, string? Generation, List<FleetAudioListener> Listeners)> _receivers = [];
    public FleetAudioListener Bind(string receiverId, long epoch, string generation) { lock (_gate) { if (!_receivers.TryGetValue(receiverId, out var current) || current.Epoch != epoch || current.Generation != generation) throw new InvalidOperationException("Receiver generation is not current."); FleetAudioListener listener = new(receiverId, generation); current.Listeners.Add(listener); return listener; } }
    public void Promote(string receiverId, long epoch, string? generation) { lock (_gate) { if (_receivers.TryGetValue(receiverId, out var prior) && prior.Epoch == epoch && prior.Generation == generation) return; if (_receivers.TryGetValue(receiverId, out prior)) foreach (FleetAudioListener listener in prior.Listeners) listener.Retire(); _receivers[receiverId] = (epoch, generation, []); } }
    public void Retire(string receiverId) { lock (_gate) { if (_receivers.Remove(receiverId, out var prior)) foreach (FleetAudioListener listener in prior.Listeners) listener.Retire(); } }
    public int ListenerCount(string receiverId) { lock (_gate) return _receivers.TryGetValue(receiverId, out var current) ? current.Listeners.Count(listener => !listener.Retired) : 0; }
    public void Publish(string receiverId, long epoch, string generation, ulong firstSample, byte[] pcm) { lock (_gate) if (_receivers.TryGetValue(receiverId, out var current) && current.Epoch == epoch && current.Generation == generation) foreach (FleetAudioListener listener in current.Listeners) listener.Publish(firstSample, pcm); }
}
public sealed class FleetAudioListener
{
    private readonly LiveQueue<(ulong FirstSample, byte[] Pcm)> _queue = new(); private int _retired; internal FleetAudioListener(string receiverId, string generation) { ReceiverId = receiverId; AudioGeneration = generation; } public string ReceiverId { get; } public string AudioGeneration { get; } public bool Retired => Volatile.Read(ref _retired) != 0; internal void Retire() { Interlocked.Exchange(ref _retired, 1); _queue.Clear(); } internal void Publish(ulong firstSample, byte[] pcm) { if (!Retired) _queue.Push((firstSample, pcm)); } public bool TryTake(out (ulong FirstSample, byte[] Pcm) packet) { if (!Retired && _queue.TryTake(out packet)) return true; packet = default; return false; }
}
