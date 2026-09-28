using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using T28.V2.Contracts;

namespace T28.V2.Receiver;

/// <summary>Always-on, outbound fleet client.  It contains no raw receive
/// inputs: those remain in the owner-local approved profile registry.</summary>
public sealed class FleetReceiverAgent : IAsyncDisposable
{
    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(10), StopTimeout = TimeSpan.FromSeconds(3);
    private static readonly JsonSerializerOptions FleetJson = new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };
    private readonly ReceiverProfileRegistry _profiles; private readonly IReceiverChildFactory _factory; private readonly HttpClient _http;
    private readonly Dictionary<Guid, FleetAgentAcknowledgement> _outcomes = []; private readonly string _receiverId, _bootId = NewId(), _token;
    private readonly TimeSpan _heartbeatInterval, _heartbeatStaleTimeout;
    private readonly Action<int> _requestProcessExit;
    private long _lastSuccessfulHeartbeatUtcTicks;
    private IGnuRadioV2? _child; private ApprovedReceiverProfile? _active; private long _epoch, _version = 1, _appliedRevision; private string _appliedProfile; private short _threshold, _tone; private FleetAgentState _state = FleetAgentState.Stopped; private string? _audioGeneration, _fault;
    private bool _desiredReceiving; private int _childRespawnAttempt; private Task? _childRespawn; private CancellationTokenSource? _childRespawnStop;
    private static readonly TimeSpan ChildRespawnInitialBackoff = TimeSpan.FromSeconds(2), ChildRespawnMaxBackoff = TimeSpan.FromSeconds(30);
    private CancellationTokenSource? _audioStop; private Task? _audioDrain; private string? _connectionId; private TaskCompletionSource<Exception>? _transportFailure;
    public FleetReceiverAgent(ReceiverProfileRegistry profiles, IReceiverChildFactory factory, HttpClient http, string receiverId, string token, TimeSpan? heartbeatInterval = null, TimeSpan? heartbeatStaleTimeout = null, Action<int>? requestProcessExit = null)
    { if (!ValidId(receiverId) || string.IsNullOrWhiteSpace(token)) throw new InvalidDataException("Fleet receiver identity is required."); _profiles = profiles; _factory = factory; _http = http; _receiverId = receiverId; _token = token; _heartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(5); _heartbeatStaleTimeout = heartbeatStaleTimeout ?? ResolveHeartbeatStaleTimeout(); _requestProcessExit = requestProcessExit ?? Environment.Exit; _lastSuccessfulHeartbeatUtcTicks = DateTimeOffset.UtcNow.UtcTicks; if (_heartbeatInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(heartbeatInterval)); if (_heartbeatStaleTimeout < TimeSpan.FromMilliseconds(50)) throw new ArgumentOutOfRangeException(nameof(heartbeatStaleTimeout)); _appliedProfile = profiles.Active.ProfileId; _threshold = profiles.Active.ThresholdTenths; _tone = profiles.Active.CtcssTenths; }
    public FleetAgentState State => _state; public string? AudioGeneration => _audioGeneration; public TimeSpan HeartbeatStaleTimeout => _heartbeatStaleTimeout;
    public static TimeSpan ResolveHeartbeatStaleTimeout(string? configuredMinutes = null)
    {
        configuredMinutes ??= Environment.GetEnvironmentVariable("T28_V2_FLEET_HEARTBEAT_STALE_MINUTES");
        return int.TryParse(configuredMinutes, out int minutes) && minutes >= 10 ? TimeSpan.FromMinutes(minutes) : TimeSpan.FromMinutes(15);
    }
    public async Task RunAsync(CancellationToken token)
    {
        using CancellationTokenSource watchdogStop = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task watchdog = WatchHeartbeatStaleAsync(watchdogStop.Token);
        try
        {
            while (!token.IsCancellationRequested)
            {
                string connection = NewId();
                try
                {
                    FleetHelloAccepted accepted = await PostAsync<FleetHello, FleetHelloAccepted>("/v2/agent/hello", new(_receiverId, _bootId, connection), token);
                    if (accepted.ReceiverId != _receiverId || accepted.BootId != _bootId || accepted.SessionEpoch < 1 || accepted.HeartbeatIntervalSeconds != 5) throw new InvalidDataException("Invalid fleet hello acceptance.");
                    _epoch = accepted.SessionEpoch; _connectionId = connection; _transportFailure = new(TaskCreationOptions.RunContinuationsAsynchronously); _outcomes.Clear();
                    await SnapshotAsync(connection, token);
                    if (_state == FleetAgentState.Receiving && _child is not null && _audioGeneration is not null) StartAudioDrain(_child, _audioGeneration);
                    await ExecutePendingAsync(connection, token);
                    using PeriodicTimer commandPoll = new(TimeSpan.FromMilliseconds(100)); DateTimeOffset nextHeartbeat = DateTimeOffset.UtcNow + _heartbeatInterval;
                    while (true)
                    {
                        Task<bool> tick = commandPoll.WaitForNextTickAsync(token).AsTask();
                        Task completed = await Task.WhenAny(tick, _transportFailure.Task);
                        if (completed == _transportFailure.Task) throw await _transportFailure.Task;
                        if (!await tick) break;
                        if (DateTimeOffset.UtcNow >= nextHeartbeat) { await PostNoContentAsync("/v2/agent/heartbeat", new FleetEnvelope(_receiverId, _bootId, connection, _epoch), token); NoteSuccessfulHeartbeat(); await SnapshotAsync(connection, token); nextHeartbeat = DateTimeOffset.UtcNow + _heartbeatInterval; }
                        await ExecutePendingAsync(connection, token);
                    }
                }
                catch (FleetSessionReplacedException) when (!token.IsCancellationRequested) { await FailClosedAsync("session-replaced"); return; }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                catch (Exception) when (!token.IsCancellationRequested) { await Task.Delay(TimeSpan.FromSeconds(1), token); }
                finally { if (_connectionId == connection) _connectionId = null; await SuspendAudioDrainAsync(); _transportFailure = null; }
            }
        }
        finally
        {
            watchdogStop.Cancel();
            try { await watchdog; } catch (OperationCanceledException) { }
        }
    }
    private async Task ExecutePendingAsync(string connection, CancellationToken token)
    {
        FleetAgentCommand[]? commands = await GetAsync<FleetAgentCommand[]>("/v2/agent/commands?receiverId=" + Uri.EscapeDataString(_receiverId) + "&bootId=" + Uri.EscapeDataString(_bootId) + "&connectionId=" + Uri.EscapeDataString(connection) + "&sessionEpoch=" + _epoch, token);
        foreach (FleetAgentCommand command in commands ?? [])
        {
            if (command.ReceiverId != _receiverId || command.BootId != _bootId || command.SessionEpoch != _epoch || command.ExpectedStateVersion != _version) continue;
            FleetAgentAcknowledgement result = _outcomes.GetValueOrDefault(command.RequestId) ?? await ExecuteAsync(command, connection, token);
            _outcomes[command.RequestId] = result; await PostNoContentAsync("/v2/agent/acknowledgement", result, token); EnsureAudioDrain();
        }
    }
    private async Task<FleetAgentAcknowledgement> ExecuteAsync(FleetAgentCommand command, string connection, CancellationToken token)
    {
        bool success = false;
        try
        {
            switch (command.Action)
            {
                case FleetAgentAction.ApplyConfiguration: success = await ApplyAsync(command.ProfileId, command.DesiredRevision, token); break;
                case FleetAgentAction.StartReceiver: success = await StartAsync(command.ProfileId, command.DesiredRevision, token); break;
                case FleetAgentAction.StopReceiver: success = await StopAsync(token); break;
                case FleetAgentAction.SetChannel: success = await SetChannelAsync(command.ProfileId, token); break;
                case FleetAgentAction.SetSquelch: success = await SetSquelchAsync(command.ThresholdTenths, command.CtcssTenths, token); break;
            }
        }
        catch (Exception) when (!token.IsCancellationRequested) { await FailClosedAsync("child-operation-failed"); }
        return new(_receiverId, _bootId, connection, _epoch, command.RequestId, success ? FleetAgentOutcome.Succeeded : FleetAgentOutcome.OutcomeUnknown, Projection());
    }
    private async Task<bool> ApplyAsync(string? profileId, long? revision, CancellationToken token)
    {
        if (profileId is null || revision is null || !_profiles.TryGet(profileId, out ApprovedReceiverProfile? profile) || profile is null) { Fault("unsupported-profile"); return false; }
        if (_state == FleetAgentState.Stopped) { _appliedProfile = profileId; _appliedRevision = revision.Value; _threshold = profile.ThresholdTenths; _tone = profile.CtcssTenths; Advance(); return true; }
        if (_state != FleetAgentState.Receiving) return false;
        return await ReplaceReceivingAsync(profile, profile.ThresholdTenths, profile.CtcssTenths, () => { _appliedProfile = profileId; _appliedRevision = revision.Value; _threshold = profile.ThresholdTenths; _tone = profile.CtcssTenths; }, token);
    }
    private async Task<bool> StartAsync(string? profileId, long? revision, CancellationToken token)
    {
        CancelChildRespawn();
        if (_state == FleetAgentState.Receiving) { _desiredReceiving = true; return profileId == _appliedProfile && revision == _appliedRevision; }
        if (_state is FleetAgentState.Starting or FleetAgentState.Stopping || profileId != _appliedProfile || revision != _appliedRevision || !_profiles.TryGet(_appliedProfile, out ApprovedReceiverProfile? profile) || profile is null) return false;
        _state = FleetAgentState.Starting; Advance();
        try { _child = await PrepareAsync(profile, _threshold, _tone, token); _active = profile; _audioGeneration = NewId(); _state = FleetAgentState.Receiving; _desiredReceiving = true; _childRespawnAttempt = 0; Advance(); return true; } catch { _desiredReceiving = false; Fault("readiness-failed"); return false; }
    }
    private async Task<bool> StopAsync(CancellationToken token)
    {
        CancelChildRespawn(); _desiredReceiving = false;
        if (_state == FleetAgentState.Stopped) return true;
        _state = FleetAgentState.Stopping; await RetireActiveAudioAsync(); Advance(); IGnuRadioV2? child = _child; _child = null; _active = null;
        try { if (child is not null) await StopChildAsync(child); _state = FleetAgentState.Stopped; _fault = null; Advance(); return true; } catch { Fault("orphan-suspected"); return false; }
    }
    private async Task<bool> SetChannelAsync(string? profileId, CancellationToken token)
    { if (profileId is null || !_profiles.TryGet(profileId, out ApprovedReceiverProfile? profile) || profile is null || _state is not (FleetAgentState.Stopped or FleetAgentState.Receiving)) return false; if (_state == FleetAgentState.Stopped) { _appliedProfile = profileId; Advance(); return true; } return await ReplaceReceivingAsync(profile, _threshold, _tone, () => _appliedProfile = profileId, token); }
    private async Task<bool> SetSquelchAsync(short? threshold, short? tone, CancellationToken token)
    { if (threshold is not short requestedThreshold || requestedThreshold is < -1000 or > 0 || tone is not short requestedTone || !CtcssTone.IsValidTenths(requestedTone) || _state is FleetAgentState.Starting or FleetAgentState.Stopping) return false; if (_state == FleetAgentState.Receiving && _child is not null) { (short actualThreshold, short actualTone) = await _child.SetControlAsync((ulong)_version, requestedThreshold, requestedTone, token); if (actualThreshold != requestedThreshold || actualTone != requestedTone) return false; } _threshold = requestedThreshold; _tone = requestedTone; Advance(); return true; }
    private async Task<IGnuRadioV2> PrepareAsync(ApprovedReceiverProfile profile, short threshold, short tone, CancellationToken token)
    { using CancellationTokenSource ready = CancellationTokenSource.CreateLinkedTokenSource(token); ready.CancelAfter(ReadyTimeout); IGnuRadioV2 child = await _factory.CreateAsync(profile, ready.Token); try { await child.ReadyAsync(ready.Token); (short actualThreshold, short actualTone) = await child.SetControlAsync((ulong)_version, threshold, tone, ready.Token); if (actualThreshold != threshold || actualTone != tone) throw new InvalidDataException("Prepared child did not acknowledge controls."); return child; } catch { await StopChildAsync(child); throw; } }
    private async Task<bool> ReplaceReceivingAsync(ApprovedReceiverProfile profile, short threshold, short tone, Action apply, CancellationToken token)
    {
        IGnuRadioV2? old = _child; if (_state != FleetAgentState.Receiving || old is null) return false;
        _state = FleetAgentState.Stopping; Advance(); await RetireActiveAudioAsync(); _child = null; _active = null;
        try { await StopChildAsync(old); }
        catch { _child = old; Fault("orphan-suspected"); return false; }
        _state = FleetAgentState.Starting; Advance();
        try { IGnuRadioV2 replacement = await PrepareAsync(profile, threshold, tone, token); _child = replacement; _active = profile; apply(); _audioGeneration = NewId(); _state = FleetAgentState.Receiving; Advance(); return true; }
        catch { _child = null; _active = null; Fault("readiness-failed"); return false; }
    }
    private static async Task StopChildAsync(IGnuRadioV2 child) { await child.StopAsync(CancellationToken.None).WaitAsync(StopTimeout); }
    private void Fault(string code) { _audioStop?.Cancel(); RetireGeneration(); _state = FleetAgentState.Faulted; _fault = code; Advance(); }
    private void RetireGeneration() { _audioGeneration = null; }
    private void StartAudioDrain(IGnuRadioV2 child, string generation)
    {
        TaskCompletionSource<Exception> failure = _transportFailure ?? throw new InvalidOperationException("Fleet audio requires a current transport.");
        _audioStop?.Cancel(); _audioStop?.Dispose(); _audioStop = new(); _audioDrain = DrainAudioAsync(child, generation, failure, _audioStop.Token);
    }
    private void EnsureAudioDrain() { if (_audioDrain is null && _state == FleetAgentState.Receiving && _child is not null && _audioGeneration is not null) StartAudioDrain(_child, _audioGeneration); }
    private async Task RetireActiveAudioAsync()
    {
        RetireGeneration(); await SuspendAudioDrainAsync();
    }
    private async Task SuspendAudioDrainAsync()
    {
        CancellationTokenSource? stop = _audioStop; Task? drain = _audioDrain; _audioStop = null; _audioDrain = null; stop?.Cancel();
        if (drain is not null) try { await drain.WaitAsync(StopTimeout); } catch (OperationCanceledException) { } catch { }
        stop?.Dispose();
    }
    private async Task DrainAudioAsync(IGnuRadioV2 child, string generation, TaskCompletionSource<Exception> transportFailure, CancellationToken stop)
    {
        try
        {
            long mediaOrigin = Stopwatch.GetTimestamp(); TimeSpan? nextPacket = null;
            await foreach ((ulong firstSample, byte[] pcm) in child.ReadPcmAsync(stop))
            {
                if (pcm.Length != T28.V2.Contracts.V2Protocol.PcmBytes || stop.IsCancellationRequested || !ReferenceEquals(_child, child) || _state != FleetAgentState.Receiving || _audioGeneration != generation) return;
                TimeSpan now = Stopwatch.GetElapsedTime(mediaOrigin), due = nextPacket ?? now; if (now > due + TimeSpan.FromMilliseconds(20)) due = now; if (due > now) await Task.Delay(due - now, stop);
                string? connection = _connectionId; if (connection is null) return;
                try { await PostNoContentAsync("/v2/agent/audio", new FleetAudioEnvelope(_receiverId, _bootId, connection, _epoch, generation, firstSample, pcm), stop); }
                catch (Exception error) when (!stop.IsCancellationRequested) { transportFailure.TrySetResult(error); return; }
                nextPacket = due + TimeSpan.FromMilliseconds(20);
            }
            if (!stop.IsCancellationRequested && ReferenceEquals(_child, child)) await HandleChildExitAsync(child, generation, stop);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch when (!stop.IsCancellationRequested && ReferenceEquals(_child, child)) { await HandleChildExitAsync(child, generation, stop); }
    }
    private async Task HandleChildExitAsync(IGnuRadioV2 child, string generation, CancellationToken token)
    {
        if (!_desiredReceiving || !ReferenceEquals(_child, child) || _state != FleetAgentState.Receiving || _audioGeneration != generation) { Fault("child-exited"); return; }
        await RetireActiveAudioAsync(); _child = null; _active = null;
        try { await StopChildAsync(child); } catch { Fault("orphan-suspected"); return; }
        _state = FleetAgentState.Stopped; _fault = null; Advance();
        ScheduleChildRespawn(token);
    }
    private void ScheduleChildRespawn(CancellationToken token)
    {
        if (!_desiredReceiving || token.IsCancellationRequested) return;
        CancelChildRespawn();
        _childRespawnStop = CancellationTokenSource.CreateLinkedTokenSource(token);
        _childRespawn = RespawnChildAfterBackoffAsync(_childRespawnStop.Token);
    }
    private void CancelChildRespawn()
    {
        CancellationTokenSource? stop = _childRespawnStop; Task? respawn = _childRespawn; _childRespawnStop = null; _childRespawn = null; stop?.Cancel(); stop?.Dispose();
        if (respawn is not null) _ = respawn.ContinueWith(_ => { }, TaskScheduler.Default);
    }
    private async Task RespawnChildAfterBackoffAsync(CancellationToken token)
    {
        int attempt = Interlocked.Increment(ref _childRespawnAttempt);
        double seconds = Math.Min(ChildRespawnMaxBackoff.TotalSeconds, ChildRespawnInitialBackoff.TotalSeconds * Math.Pow(2, attempt - 1));
        try { await Task.Delay(TimeSpan.FromSeconds(seconds), token); } catch (OperationCanceledException) { return; }
        if (!_desiredReceiving || token.IsCancellationRequested) return;
        if (!_profiles.TryGet(_appliedProfile, out ApprovedReceiverProfile? profile) || profile is null) { Fault("unsupported-profile"); return; }
        _state = FleetAgentState.Starting; Advance();
        try
        {
            IGnuRadioV2 replacement = await PrepareAsync(profile, _threshold, _tone, token);
            if (!_desiredReceiving || token.IsCancellationRequested) { await StopChildAsync(replacement); return; }
            _child = replacement; _active = profile; _audioGeneration = NewId(); _state = FleetAgentState.Receiving; _childRespawnAttempt = 0; Advance(); EnsureAudioDrain();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch { if (_desiredReceiving && !token.IsCancellationRequested) { _state = FleetAgentState.Faulted; _fault = "readiness-failed"; Advance(); ScheduleChildRespawn(token); } }
    }
    private async Task FailClosedAsync(string code)
    {
        CancelChildRespawn(); await RetireActiveAudioAsync(); IGnuRadioV2? child = _child; _child = null; _active = null;
        if (child is not null) try { await StopChildAsync(child); } catch { _child = child; code = "orphan-suspected"; }
        Fault(code);
    }
    private void Advance() { _version = checked(_version + 1); }
    private FleetProjection Projection() => new(_state, _version, _audioGeneration, _appliedProfile, _appliedRevision, "ready", _fault, _threshold, _tone, _profiles.PublicProfiles, _child is null);
    private Task SnapshotAsync(string connection, CancellationToken token) => PostNoContentAsync("/v2/agent/snapshot", new FleetSnapshotEnvelope(_receiverId, _bootId, connection, _epoch, Projection()), token);
    private void NoteSuccessfulHeartbeat() => Interlocked.Exchange(ref _lastSuccessfulHeartbeatUtcTicks, DateTimeOffset.UtcNow.UtcTicks);
    private async Task WatchHeartbeatStaleAsync(CancellationToken token)
    {
        TimeSpan poll = _heartbeatStaleTimeout < TimeSpan.FromSeconds(2) ? TimeSpan.FromMilliseconds(20) : TimeSpan.FromSeconds(1);
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(poll, token);
            if (DateTimeOffset.UtcNow.UtcTicks - Interlocked.Read(ref _lastSuccessfulHeartbeatUtcTicks) < _heartbeatStaleTimeout.Ticks) continue;
            Console.Error.WriteLine("t28 v2 receiver agent-heartbeat-stale: no successful POST /v2/agent/heartbeat within " + _heartbeatStaleTimeout + "; exiting so systemd Restart=always can recover");
            _requestProcessExit(1);
            return;
        }
    }
    private async Task<TResponse> PostAsync<TRequest, TResponse>(string route, TRequest request, CancellationToken token) { using HttpRequestMessage message = Request(HttpMethod.Post, route); message.Content = JsonContent.Create(request, options: FleetJson); using HttpResponseMessage response = await SendControlAsync(message, route, token); EnsureFleetResponse(response); return await response.Content.ReadFromJsonAsync<TResponse>(FleetJson, token) ?? throw new InvalidDataException("Missing fleet response."); }
    private async Task PostNoContentAsync<T>(string route, T request, CancellationToken token) { using HttpRequestMessage message = Request(HttpMethod.Post, route); message.Content = JsonContent.Create(request); using HttpResponseMessage response = await SendControlAsync(message, route, token); EnsureFleetResponse(response); }
    private async Task<T?> GetAsync<T>(string route, CancellationToken token) { using HttpRequestMessage message = Request(HttpMethod.Get, route); using HttpResponseMessage response = await SendControlAsync(message, route, token); EnsureFleetResponse(response); return await response.Content.ReadFromJsonAsync<T>(FleetJson, token); }
    private async Task<HttpResponseMessage> SendControlAsync(HttpRequestMessage message, string route, CancellationToken token)
    {
        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(ControlPlaneRequestTimeout(route));
        try { return await _http.SendAsync(message, deadline.Token); }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { throw new TimeoutException("Fleet control-plane request exceeded its timeout."); }
    }
    public static TimeSpan ControlPlaneRequestTimeout(string route)
    {
        const int slackMilliseconds = 5_000, defaultMilliseconds = 30_000;
        int marker = route.IndexOf("waitMilliseconds=", StringComparison.Ordinal);
        if (marker >= 0)
        {
            ReadOnlySpan<char> rest = route.AsSpan(marker + "waitMilliseconds=".Length);
            int end = rest.IndexOf('&');
            if (end >= 0) rest = rest[..end];
            if (int.TryParse(rest, out int wait) && wait >= 0) return TimeSpan.FromMilliseconds(wait + slackMilliseconds);
        }
        return TimeSpan.FromMilliseconds(defaultMilliseconds);
    }
    private static void EnsureFleetResponse(HttpResponseMessage response) { if (response.StatusCode == System.Net.HttpStatusCode.Conflict && response.Headers.TryGetValues("X-T28-V2-Session-Fence", out IEnumerable<string>? values) && values.Contains("replaced", StringComparer.Ordinal)) throw new FleetSessionReplacedException(); response.EnsureSuccessStatusCode(); }
    private HttpRequestMessage Request(HttpMethod method, string route) { HttpRequestMessage message = new(method, route); message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("T28-Agent", _token); return message; }
    private static bool ValidId(string value) => value.Length is >= 8 and <= 64 && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-'); private static string NewId() => Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant();
    public async ValueTask DisposeAsync() { await StopAsync(CancellationToken.None); }
}

internal sealed class FleetSessionReplacedException : HttpRequestException { }

public enum FleetAgentState { Stopped, Starting, Receiving, Stopping, Faulted }
public enum FleetAgentAction { StartReceiver, StopReceiver, ApplyConfiguration, SetChannel, SetSquelch }
public enum FleetAgentOutcome { Accepted, Succeeded, RejectedUnauthorized, RejectedOffline, RejectedStaleSession, RejectedStaleState, RequestConflict, OutcomeUnknown }
public sealed record FleetProjection(FleetAgentState State, long StateVersion, string? AudioGeneration, string AppliedProfileId, long AppliedRevision, string Hardware, string? FaultCode, short ThresholdTenths = -1000, short CtcssTenths = 1622, IReadOnlyList<T28.V2.Contracts.ApprovedChannelProjection>? Channels = null, bool OwnedChildAbsent = false);
public sealed record FleetAgentCommand(Guid RequestId, string ReceiverId, long SessionEpoch, string BootId, long ExpectedStateVersion, FleetAgentAction Action, string? ProfileId, long? DesiredRevision, short? ThresholdTenths = null, short? CtcssTenths = null);
public sealed record FleetEnvelope(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch);
public sealed record FleetSnapshotEnvelope(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, FleetProjection Projection);
public sealed record FleetAgentAcknowledgement(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, Guid RequestId, FleetAgentOutcome Outcome, FleetProjection Projection);
public sealed record FleetHello(string ReceiverId, string BootId, string ConnectionId);
public sealed record FleetHelloAccepted(string ReceiverId, string BootId, long SessionEpoch, int HeartbeatIntervalSeconds);
public sealed record FleetAudioEnvelope(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, string AudioGeneration, ulong FirstSample, byte[] Pcm);
